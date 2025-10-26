import threading
import uuid
import time
import logging
from typing import Optional, List, Any, Dict, Union

try:
    from pymodbus.client.sync import ModbusTcpClient
    from pymodbus.exceptions import ModbusIOException
except Exception:
    ModbusTcpClient = None
    ModbusIOException = Exception

logger = logging.getLogger(__name__)

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
        self.id = str(uuid.uuid4())
        self.host = host
        self.port = int(port)
        self.unit = int(unit)
        self.name = name or f"{host}:{port}"
        self._lock = threading.Lock()
        self.client: Optional["ModbusTcpClient"] = None
        self.connected: bool = False
        self.last_read: Optional[List[Any]] = None
        self._last_connect_time: Optional[float] = None

        self.connect_timeout = float(connect_timeout)
        self.operation_timeout = float(operation_timeout)
        self.retries = int(retries)
        self.retry_backoff = float(retry_backoff)

    def _create_client(self, timeout: float):
        if ModbusTcpClient is None:
            raise RuntimeError("pymodbus is not installed (pip install pymodbus)")
        return ModbusTcpClient(self.host, port=self.port, timeout=timeout)

    def connect(self, timeout: Optional[float] = None) -> bool:
        if ModbusTcpClient is None:
            raise RuntimeError("pymodbus is not installed (pip install pymodbus)")

        effective_timeout = float(timeout) if timeout is not None else self.operation_timeout

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

        attempt = 0
        max_attempts = 1 + max(0, self.retries)
        last_exc = None
        while attempt < max_attempts:
            attempt += 1
            try:
                new_client = self._create_client(effective_timeout)
                ok = new_client.connect()
                if not ok:
                    raise ConnectionError("client.connect() returned False")
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
                if attempt < max_attempts:
                    time.sleep(self.retry_backoff * attempt)

        with self._lock:
            self.client = None
            self.connected = False
        logger.error("all connect attempts failed for %s:%s: %s", self.host, self.port, last_exc)
        return False

    def close(self) -> None:
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
        with self._lock:
            client = self.client

        if client is None:
            raise ConnectionError("No underlying client available")

        address = int(address)
        count = int(count)

        try:
            if type_ == "coils":
                rr = client.read_coils(address, count, unit=self.unit)
                if rr is None:
                    raise ModbusIOException("No response")
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
            raise

    def read(self, type: str, address: int, count: int, allow_reconnect: bool = False):
        with self._lock:
            client = self.client
            connected = self.connected

        if not connected or client is None:
            if not allow_reconnect:
                raise ConnectionError(f"Connection to {self.host}:{self.port} is closed")
            ok = self.connect(timeout=self.operation_timeout)
            if not ok:
                raise ConnectionError(f"Auto-reconnect to {self.host}:{self.port} failed")

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
            with self._lock:
                self.connected = False
            raise
        except Exception:
            with self._lock:
                self.connected = False
            raise

    def write(self, type: str, address: int, value: Union[int, List[int], List[bool]], allow_reconnect: bool = False):
        with self._lock:
            client = self.client
            connected = self.connected

        if not connected or client is None:
            if not allow_reconnect:
                raise ConnectionError(f"Connection to {self.host}:{self.port} is closed")
            ok = self.connect(timeout=self.operation_timeout)
            if not ok:
                raise ConnectionError(f"Auto-reconnect to {self.host}:{self.port} failed")

        with self._lock:
            client = self.client

        if client is None:
            raise ConnectionError(f"No client available for {self.host}:{self.port}")

        address = int(address)

        try:
            if type == "coils":
                if isinstance(value, (list, tuple)):
                    coils = [True if v not in (0, "0", False, "false", "False") else False for v in value]
                    rr = client.write_coils(address, coils, unit=self.unit)
                    if rr is None:
                        raise ModbusIOException("No response writing coils")
                    return rr
                else:
                    val_bool = bool(value)
                    rr = client.write_coil(address, val_bool, unit=self.unit)
                    if rr is None:
                        raise ModbusIOException("No response writing coil")
                    return rr

            if type == "holding":
                if isinstance(value, (list, tuple)):
                    regs = [int(v) for v in value]
                    if hasattr(client, "write_registers"):
                        rr = client.write_registers(address, regs, unit=self.unit)
                        if rr is None:
                            raise ModbusIOException("No response writing registers")
                        return rr
                    else:
                        last_rr = None
                        for idx, rv in enumerate(regs):
                            last_rr = client.write_register(address + idx, int(rv), unit=self.unit)
                            if last_rr is None:
                                raise ModbusIOException("No response writing register")
                        return last_rr
                else:
                    rr = client.write_register(address, int(value), unit=self.unit)
                    if rr is None:
                        raise ModbusIOException("No response writing register")
                    return rr

            raise ValueError(f"Write not supported for type: {type}")
        except Exception:
            with self._lock:
                self.connected = False
            raise


class ConnectionManager:
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


manager = ConnectionManager()
