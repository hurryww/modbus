# modbus/modbus_manager.py
# Modbus connection and manager implementation used by the Streamlit UI.
# - ModbusConnection wraps pymodbus ModbusTcpClient with safe connect/close/read semantics.
# - Manager keeps a registry of connections and helpers used by the UI (create/list/get/remove).
# NOTE: install pymodbus (pip install pymodbus) before running.

import threading
import uuid
import time
from typing import Optional, List, Any, Dict

try:
    from pymodbus.client.sync import ModbusTcpClient
    from pymodbus.exceptions import ModbusIOException
except Exception:
    # If pymodbus isn't installed, raise a helpful error when attempting network ops.
    ModbusTcpClient = None
    ModbusIOException = Exception


class ModbusConnection:
    def __init__(self, host: str, port: int = 502, unit: int = 1, name: Optional[str] = None):
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

    def connect(self, timeout: float = 3.0) -> bool:
        """
        Create a new ModbusTcpClient and try to connect.
        Returns True on success, False otherwise.
        """
        if ModbusTcpClient is None:
            raise RuntimeError("pymodbus is not installed (pip install pymodbus)")

        with self._lock:
            try:
                # Close previous client if exists
                if self.client:
                    try:
                        self.client.close()
                    except Exception:
                        pass
                    self.client = None
                    self.connected = False

                self.client = ModbusTcpClient(self.host, port=self.port, timeout=timeout)
                ok = self.client.connect()
                self.connected = bool(ok)
                if self.connected:
                    self._last_connect_time = time.time()
                return self.connected
            except Exception:
                # ensure consistent state on failure
                self.client = None
                self.connected = False
                return False

    def close(self) -> None:
        """
        Fully close the underlying client and mark as disconnected.
        This ensures subsequent read attempts won't reuse a stale socket.
        """
        with self._lock:
            try:
                if self.client:
                    try:
                        self.client.close()
                    except Exception:
                        pass
            finally:
                # Always clear the client reference and state
                self.client = None
                self.connected = False

    def _single_read(self, type_: str, address: int, count: int) -> List[Any]:
        """
        Perform a single read operation using the underlying pymodbus client.
        Returns a Python list of values (booleans for coils/discrete, ints for registers).
        Raises exceptions on error.
        """
        if self.client is None:
            raise ConnectionError("No underlying client available")

        # Ensure using ints
        address = int(address)
        count = int(count)

        try:
            if type_ == "coils":
                rr = self.client.read_coils(address, count, unit=self.unit)
                if rr is None:
                    raise ModbusIOException("No response")
                # pymodbus response often has .bits
                if hasattr(rr, "bits"):
                    return rr.bits[:count]
                raise ModbusIOException("Unexpected response for coils")

            if type_ == "discrete":
                rr = self.client.read_discrete_inputs(address, count, unit=self.unit)
                if rr is None:
                    raise ModbusIOException("No response")
                if hasattr(rr, "bits"):
                    return rr.bits[:count]
                raise ModbusIOException("Unexpected response for discrete inputs")

            if type_ == "holding":
                rr = self.client.read_holding_registers(address, count, unit=self.unit)
                if rr is None:
                    raise ModbusIOException("No response")
                if hasattr(rr, "registers"):
                    return rr.registers[:count]
                raise ModbusIOException("Unexpected response for holding registers")

            if type_ == "input":
                rr = self.client.read_input_registers(address, count, unit=self.unit)
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
            if not self.connected or self.client is None:
                if not allow_reconnect:
                    raise ConnectionError(f"Connection to {self.host}:{self.port} is closed")
                ok = self.connect()
                if not ok:
                    raise ConnectionError(f"Auto-reconnect to {self.host}:{self.port} failed")

            if self.client is None:
                raise ConnectionError(f"No client available for {self.host}:{self.port}")

            try:
                result = self._single_read(type, address, count)
                self.last_read = result
                return result
            except ModbusIOException:
                # Mark disconnected on IO problems
                self.connected = False
                raise
            except Exception:
                # Any other exception - mark disconnected to be safe
                self.connected = False
                raise


class ConnectionManager:
    """
    Simple manager maintaining ModbusConnection instances.
    Provides minimal API used by the Streamlit UI:
    - create_connection(host, port, unit, name) -> ModbusConnection
    - list_connections() -> list[dict]
    - get(conn_id) -> ModbusConnection or None
    - remove(conn_id) -> None
    """

    def __init__(self):
        self._conns: Dict[str, ModbusConnection] = {}
        self._lock = threading.Lock()

    def create_connection(self, host: str, port: int = 502, unit: int = 1, name: Optional[str] = None) -> ModbusConnection:
        with self._lock:
            conn = ModbusConnection(host=host, port=port, unit=unit, name=name)
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
                    "connected": c.connected
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
