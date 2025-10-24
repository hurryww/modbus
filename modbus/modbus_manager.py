import threading
import uuid
import time
import logging
from typing import Optional, List, Any, Dict

try:
    from pymodbus.client.sync import ModbusTcpClient
    from pymodbus.exceptions import ModbusIOException
except Exception:
    # If pymodbus isn't installed, raise a helpful error when attempting network ops.
    ModbusTcpClient = None
    ModbusIOException = Exception

logger = logging.getLogger(__name__)

# Default values for timeouts and retries
DEFAULT_CONNECT_TIMEOUT = 3.0
DEFAULT_OPERATION_TIMEOUT = 3.0
DEFAULT_RETRIES = 0
DEFAULT_RETRY_BACKOFF = 0.5


class ModbusConnection:
    def __init__(
        self,
        host: str,
        port: int = 502,
        unit: int = 1,
        name: Optional[str] = None,
        connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
        operation_timeout: float = DEFAULT_OPERATION_TIMEOUT,
        retries: int = DEFAULT_RETRIES,
        retry_backoff: float = DEFAULT_RETRY_BACKOFF,
    ):
        """
        connect_timeout: (reserved) logical parameter for connect attempts, currently operation_timeout
                         is passed to pymodbus client as socket timeout; keep both for clarity.
        operation_timeout: socket/IO timeout used by ModbusTcpClient
        retries: number of additional attempts for connect (0 = no retry)
        retry_backoff: base backoff multiplier (seconds)
        """
        self.id = str(uuid.uuid4())
        self.host = host
        self.port = int(port)
        self.unit = int(unit)
        self.name = name or f"{host}:{port}"
        self._lock = threading.Lock()
        self.client: Optional["ModbusTcpClient"] = None
        self.connected: bool = False
        self.last_read: Optional[List[Any]] = None
        # small timestamp for debugging
        self._last_connect_time: Optional[float] = None

        # configuration
        self.connect_timeout = float(connect_timeout)
        self.operation_timeout = float(operation_timeout)
        self.retries = int(retries)
        self.retry_backoff = float(retry_backoff)

    def _create_client(self, timeout: float):
        if ModbusTcpClient is None:
            raise RuntimeError("pymodbus is not installed (pip install pymodbus)")
        # ModbusTcpClient takes timeout which is the socket timeout for operations
        return ModbusTcpClient(self.host, port=self.port, timeout=timeout)

    def connect(self, timeout: Optional[float] = None) -> bool:
        """
        Create a new ModbusTcpClient and try to connect.
        Returns True on success, False otherwise.

        Implementation notes:
        - We avoid holding the main lock during blocking network I/O where possible.
        - Supports configurable retries with exponential-ish backoff (linear backoff multiplied by attempt).
        """
        if ModbusTcpClient is None:
            raise RuntimeError("pymodbus is not installed (pip install pymodbus)")

        # Determine effective timeout to use for client socket operations
        effective_timeout = float(timeout) if timeout is not None else self.operation_timeout

        # Close previous client reference in a short critical section, but do actual close outside lock
        old_client = None
        with self._lock:
            if self.client:
                old_client = self.client
                self.client = None
                self.connected = False

        if old_client:
            try:
                old_client.close()
            except Exception:
                logger.debug("error closing previous client", exc_info=True)

        # Try to connect with retries
        attempt = 0
        max_attempts = 1 + max(0, self.retries)
        last_exc = None
        while attempt < max_attempts:
            attempt += 1
            try:
                new_client = self._create_client(effective_timeout)
                ok = new_client.connect()
                if not ok:
                    # some implementations return False on failure rather than raising
                    raise ConnectionError("client.connect() returned False")
                # Put the new client into state with a short critical section
                with self._lock:
                    self.client = new_client
                    self.connected = True
                    self._last_connect_time = time.time()
                logger.info("connected to %s:%s (attempt %s)", self.host, self.port, attempt)
                return True
            except Exception as e:
                last_exc = e
                logger.warning(
                    "connect attempt %s/%s failed for %s:%s: %s",
                    attempt,
                    max_attempts,
                    self.host,
                    self.port,
                    e,
                    exc_info=True,
                )
                # backoff before next attempt
                if attempt < max_attempts:
                    time.sleep(self.retry_backoff * attempt)

        # all attempts failed, ensure consistent disconnected state
        with self._lock:
            self.client = None
            self.connected = False
        logger.error("all connect attempts failed for %s:%s: %s", self.host, self.port, last_exc)
        return False

    def close(self) -> None:
        """
        Fully close the underlying client and mark as disconnected.
        This ensures subsequent read attempts won't reuse a stale socket.
        """
        with self._lock:
            client = self.client
            self.client = None
            self.connected = False

        if client:
            try:
                client.close()
            except Exception:
                logger.debug("error closing client during close()", exc_info=True)

    def _single_read(self, type_: str, address: int, count: int) -> List[Any]:
        """
        Perform a single read operation using the underlying pymodbus client.
        Returns a Python list of values (booleans for coils/discrete, ints for registers).
        Raises exceptions on error.
        """
        # short critical section to get the client reference
        with self._lock:
            client = self.client

        if client is None:
            raise ConnectionError("No underlying client available")

        # Ensure using ints
        address = int(address)
        count = int(count)

        try:
            if type_ == "coils":
                rr = client.read_coils(address, count, unit=self.unit)
                if rr is None:
                    raise ModbusIOException("No response")
                # pymodbus response often has .bits
                if hasattr(rr, "bits"):
                    return rr.bits[:count]
                raise ModbusIOException("Unexpected response for coils")

            if type_ == "discrete":
                rr = client.read_discrete_inputs(address, count, unit=self.unit)
                if rr is None:
                    raise ModbusIOException("No response")
                if hasattr(rr, "bits"):
                    return rr.bits[:count]
                raise ModbusIOException("Unexpected response for discrete inputs")

            if type_ == "holding":
                rr = client.read_holding_registers(address, count, unit=self.unit)
                if rr is None:
                    raise ModbusIOException("No response")
                if hasattr(rr, "registers"):
                    return rr.registers[:count]
                raise ModbusIOException("Unexpected response for holding registers")

            if type_ == "input":
                rr = client.read_input_registers(address, count, unit=self.unit)
                if rr is None:
                    raise ModbusIOException("No response")
                if hasattr(rr, "registers"):
                    return rr.registers[:count]
                raise ModbusIOException("Unexpected response for input registers")

            raise ValueError(f"Unknown read type: {type_}")
        except Exception:
            # propagate exception to caller (read() will mark connection false if needed)
            raise

    def read(self, type: str, address: int, count: int, allow_reconnect: bool = False):
        """
        Public read API:
        - If connection is closed and allow_reconnect is False, raise ConnectionError.
        - If allow_reconnect is True, attempt to connect once.
        - On Modbus or socket errors, set connected = False and re-raise.
        """
        with self._lock:
            client = self.client
            connected = self.connected

        if not connected or client is None:
            if not allow_reconnect:
                raise ConnectionError(f"Connection to {self.host}:{self.port} is closed")
            ok = self.connect(timeout=self.operation_timeout)
            if not ok:
                raise ConnectionError(f"Auto-reconnect to {self.host}:{self.port} failed")

        # re-fetch client reference
        with self._lock:
            client = self.client

        if client is None:
            raise ConnectionError(f"No client available for {self.host}:{self.port}")

        try:
            result = self._single_read(type, address, count)
            with self._lock:
                self.last_read = result
            return result
        except ModbusIOException:
            # Mark disconnected on IO problems
            with self._lock:
                self.connected = False
            raise
        except Exception:
            # Any other exception - mark disconnected to be safe
            with self._lock:
                self.connected = False
            raise


class ConnectionManager:
    """
    Simple manager maintaining ModbusConnection instances.
    Provides minimal API used by the Streamlit UI:
    - create_connection(host, port, unit, name, connect_timeout, operation_timeout, retries) -> ModbusConnection
    - list_connections() -> list[dict]
    - get(conn_id) -> ModbusConnection or None
    - remove(conn_id) -> None
    """

    def __init__(self):
        self._conns: Dict[str, ModbusConnection] = {}
        self._lock = threading.Lock()

    def create_connection(
        self,
        host: str,
        port: int = 502,
        unit: int = 1,
        name: Optional[str] = None,
        connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
        operation_timeout: float = DEFAULT_OPERATION_TIMEOUT,
        retries: int = DEFAULT_RETRIES,
    ) -> ModbusConnection:
        with self._lock:
            conn = ModbusConnection(
                host=host,
                port=port,
                unit=unit,
                name=name,
                connect_timeout=connect_timeout,
                operation_timeout=operation_timeout,
                retries=retries,
            )
            self._conns[conn.id] = conn
            return conn

    def list_connections(self) -> List[Dict[str, Any]]:
        with self._lock:
            out = []
            for c in self._conns.values():
                out.append({
                    "id": c.id,
                    "name": c.name,
                    "host": c.host,
                    "port": c.port,
                    "unit": c.unit,
                    "connected": c.connected,
                    "connect_timeout": c.connect_timeout,
                    "operation_timeout": c.operation_timeout,
                    "retries": c.retries,
                })
            return out

    def get(self, conn_id: str) -> Optional[ModbusConnection]:
        return self._conns.get(conn_id)

    def remove(self, conn_id: str) -> None:
        with self._lock:
            c = self._conns.pop(conn_id, None)
        if c:
            try:
                c.close()
            except Exception:
                pass


# Export a single manager instance used by the UI
manager = ConnectionManager()

