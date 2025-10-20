# modbus_manager.py
import threading
import time
import uuid
from typing import Any, Dict, List, Optional
from pymodbus.client.sync import ModbusTcpClient
from pymodbus.exceptions import ModbusIOException

class ModbusConnection:
    def __init__(self, host: str, port: int = 502, unit: int = 1, name: Optional[str] = None, id: Optional[str] = None):
        self.id = id or str(uuid.uuid4())
        self.name = name or f"{host}:{port}"
        self.host = host
        self.port = port
        self.unit = unit
        self.client = ModbusTcpClient(host, port=port)
        self.connected = False
        # poll thread control
        self._poll_thread: Optional[threading.Thread] = None
        self._poll_stop = threading.Event()
        self._poll_params: Optional[Dict[str, Any]] = None
        self._lock = threading.Lock()
        # last read & history
        self.last_read: Optional[Dict[str, Any]] = None
        self.poll_history: List[Dict[str, Any]] = []

    def connect(self, timeout: float = 5.0) -> bool:
        # attempt to connect; set connected flag
        try:
            self.client.close()
            self.client = ModbusTcpClient(self.host, port=self.port, timeout=timeout)
            ok = self.client.connect()
            self.connected = bool(ok)
            return self.connected
        except Exception:
            self.connected = False
            return False

    def close(self):
        try:
            self.stop_poll()
            if self.client:
                self.client.close()
        finally:
            self.connected = False

    def _ensure_connected(self) -> bool:
        if self.connected and self.client and self.client.connect():
            return True
        # try to connect
        return self.connect()

    def read(self, type: str = "holding", address: int = 0, count: int = 1) -> List[int]:
        """
        type: 'holding'|'input'|'coils'|'discrete'
        returns list of values (ints or 0/1)
        """
        with self._lock:
            if not self._ensure_connected():
                raise ConnectionError(f"Unable to connect to {self.host}:{self.port}")
            try:
                if type == "holding":
                    rr = self.client.read_holding_registers(address, count, unit=self.unit)
                    if rr.isError():
                        raise ModbusIOException(str(rr))
                    values = list(rr.registers)
                elif type == "input":
                    rr = self.client.read_input_registers(address, count, unit=self.unit)
                    if rr.isError():
                        raise ModbusIOException(str(rr))
                    values = list(rr.registers)
                elif type == "coils":
                    rr = self.client.read_coils(address, count, unit=self.unit)
                    if rr.isError():
                        raise ModbusIOException(str(rr))
                    values = [1 if b else 0 for b in rr.bits[:count]]
                elif type == "discrete":
                    rr = self.client.read_discrete_inputs(address, count, unit=self.unit)
                    if rr.isError():
                        raise ModbusIOException(str(rr))
                    values = [1 if b else 0 for b in rr.bits[:count]]
                else:
                    raise ValueError("unsupported type")
                # update last_read
                self.last_read = {
                    "timestamp": time.time(),
                    "type": type,
                    "address": address,
                    "count": count,
                    "values": values
                }
                return values
            except Exception as e:
                # mark disconnected on errors to allow reconnection next time
                self.connected = False
                raise

    def write(self, type: str = "holding", address: int = 0, values: List[int] = []):
        """
        type: 'holding'|'coils'
        values: list of ints (for coils: 0/1; for holding: 16-bit integers)
        """
        with self._lock:
            if not self._ensure_connected():
                raise ConnectionError(f"Unable to connect to {self.host}:{self.port}")
            try:
                if type == "holding":
                    if len(values) == 1:
                        rr = self.client.write_register(address, int(values[0]), unit=self.unit)
                    else:
                        rr = self.client.write_registers(address, [int(v) for v in values], unit=self.unit)
                elif type == "coils":
                    # write_coil or write_coils
                    bitvals = [bool(int(v)) for v in values]
                    if len(bitvals) == 1:
                        rr = self.client.write_coil(address, bitvals[0], unit=self.unit)
                    else:
                        rr = self.client.write_coils(address, bitvals, unit=self.unit)
                else:
                    raise ValueError("unsupported write type")
                if rr.isError():
                    raise ModbusIOException(str(rr))
                return True
            except Exception:
                self.connected = False
                raise

    def start_poll(self, type: str, address: int, count: int, interval: float, max_history: int = 100):
        """
        Start a background thread to poll and append to poll_history.
        """
        self.stop_poll()
        self._poll_stop.clear()
        self._poll_params = {"type": type, "address": address, "count": count, "interval": interval, "max_history": max_history}

        def _worker():
            # poll loop
            while not self._poll_stop.wait(0):
                start = time.time()
                try:
                    values = self.read(type=type, address=address, count=count)
                    entry = {
                        "timestamp": time.time(),
                        "type": type,
                        "address": address,
                        "count": count,
                        "values": values,
                        "error": None
                    }
                except Exception as e:
                    entry = {
                        "timestamp": time.time(),
                        "type": type,
                        "address": address,
                        "count": count,
                        "values": None,
                        "error": str(e)
                    }
                # append history, enforce max_history
                self.poll_history.append(entry)
                if len(self.poll_history) > max_history:
                    self.poll_history.pop(0)
                # wait interval (subtract elapsed)
                elapsed = time.time() - start
                to_wait = max(0, interval - elapsed)
                if self._poll_stop.wait(to_wait):
                    break

        t = threading.Thread(target=_worker, daemon=True)
        self._poll_thread = t
        t.start()

    def stop_poll(self):
        if self._poll_thread and self._poll_thread.is_alive():
            self._poll_stop.set()
            self._poll_thread.join(timeout=2)
        self._poll_thread = None
        self._poll_stop.clear()
        self._poll_params = None

class ModbusManager:
    def __init__(self):
        self._conns: Dict[str, ModbusConnection] = {}
        self._lock = threading.Lock()

    def list_connections(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [
                {"id": c.id, "name": c.name, "host": c.host, "port": c.port, "unit": c.unit, "connected": c.connected}
                for c in self._conns.values()
            ]

    def create_connection(self, host: str, port: int = 502, unit: int = 1, name: Optional[str] = None) -> ModbusConnection:
        conn = ModbusConnection(host=host, port=port, unit=unit, name=name)
        with self._lock:
            self._conns[conn.id] = conn
        # attempt to connect immediately (best effort)
        conn.connect()
        return conn

    def get(self, id: str) -> Optional[ModbusConnection]:
        with self._lock:
            return self._conns.get(id)

    def remove(self, id: str):
        with self._lock:
            c = self._conns.pop(id, None)
        if c:
            c.close()

    def close_all(self):
        with self._lock:
            ids = list(self._conns.keys())
        for i in ids:
            self.remove(i)

# expose a singleton manager
manager = ModbusManager()