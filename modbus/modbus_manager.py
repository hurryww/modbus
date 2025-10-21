# modbus_manager.py (增加：遇到 IllegalAddress 时的可选自动探测/调整地址)
import threading
import time
import uuid
import logging
from typing import Any, Dict, List, Optional
from pymodbus.client.sync import ModbusTcpClient
from pymodbus.exceptions import ModbusIOException

logger = logging.getLogger(__name__)

def plc_to_modbus(plc_addr: int, base: int) -> int:
    try:
        plc_addr = int(plc_addr)
    except Exception:
        raise ValueError("PLC 地址必须是整数")
    if plc_addr >= base:
        return plc_addr - base
    return plc_addr

class ModbusConnection:
    DEFAULT_MAX_READ = {
        "holding": 100,
        "input": 100,
        "coils": 100,
        "discrete": 100,
    }

    def __init__(
        self,
        host: str,
        port: int = 502,
        unit: int = 1,
        name: Optional[str] = None,
        id: Optional[str] = None,
        max_read_override: Optional[Dict[str, int]] = None,
        inter_request_delay: float = 0.02,
    ):
        self.id = id or str(uuid.uuid4())
        self.name = name or f"{host}:{port}"
        self.host = host
        self.port = port
        self.unit = unit
        self.client = ModbusTcpClient(host, port=port)
        self.connected = False
        self._poll_thread: Optional[threading.Thread] = None
        self._poll_stop = threading.Event()
        self._poll_params: Optional[Dict[str, Any]] = None
        self._lock = threading.Lock()
        self.last_read: Optional[Dict[str, Any]] = None
        self.poll_history: List[Dict[str, Any]] = []

        self._max_read = dict(ModbusConnection.DEFAULT_MAX_READ)
        if max_read_override:
            self._max_read.update(max_read_override)

        self._inter_request_delay = float(inter_request_delay)

    def connect(self, timeout: float = 5.0) -> bool:
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
        return self.connect()

    def _single_read(self, type: str, address: int, count: int):
        if type == "holding":
            rr = self.client.read_holding_registers(address, count, unit=self.unit)
            if rr is None:
                raise ModbusIOException(f"No response for read_holding_registers(address={address}, count={count}, unit={self.unit})")
            if rr.isError():
                raise ModbusIOException(f"Modbus Error reading holding registers at {address} (count={count}): {rr}")
            return list(rr.registers)
        if type == "input":
            rr = self.client.read_input_registers(address, count, unit=self.unit)
            if rr is None:
                raise ModbusIOException(f"No response for read_input_registers(address={address}, count={count}, unit={self.unit})")
            if rr.isError():
                raise ModbusIOException(f"Modbus Error reading input registers at {address} (count={count}): {rr}")
            return list(rr.registers)
        if type == "coils":
            rr = self.client.read_coils(address, count, unit=self.unit)
            if rr is None:
                raise ModbusIOException(f"No response for read_coils(address={address}, count={count}, unit={self.unit})")
            if rr.isError():
                raise ModbusIOException(f"Modbus Error reading coils at {address} (count={count}): {rr}")
            return [1 if b else 0 for b in rr.bits[:count]]
        if type == "discrete":
            rr = self.client.read_discrete_inputs(address, count, unit=self.unit)
            if rr is None:
                raise ModbusIOException(f"No response for read_discrete_inputs(address={address}, count={count}, unit={self.unit})")
            if rr.isError():
                raise ModbusIOException(f"Modbus Error reading discrete inputs at {address} (count={count}): {rr}")
            return [1 if b else 0 for b in rr.bits[:count]]
        raise ValueError("unsupported type")

    def _probe_address(self, type: str, addr: int, timeout_probe: float = 1.0) -> bool:
        """
        Probe a single address by reading 1 item. Return True if successful (no Modbus exception).
        """
        try:
            # Small probe; don't change connection state on failure other than logging
            part = self._single_read(type, addr, 1)
            return True if part is not None else False
        except ModbusIOException as me:
            # Known Modbus exception (IllegalAddress etc.)
            logger.debug("Probe failed at addr %s: %s", addr, me)
            return False
        except Exception as e:
            logger.debug("Probe unexpected error at addr %s: %s", addr, e)
            return False

    def read(
        self,
        type: str = "holding",
        address: int = 0,
        count: int = 1,
        max_per_request: Optional[int] = None,
        try_alternatives: bool = False,
        alt_probe_range: int = 5,
    ) -> List[int]:
        """
        支持自动分片的读取接口，并可选在遇到 IllegalAddress 时探测邻近地址。

        - try_alternatives: 如果为 True，遇到 IllegalAddress 时会在 address +/- alt_probe_range 内探测第一个可读地址并以此继续读取（风险：可能调整了偏移）。
        """
        with self._lock:
            if address is None or address < 0:
                raise ValueError("address must be a non-negative integer")
            count = int(count)
            if count <= 0:
                raise ValueError("count must be >= 1")
            if not self._ensure_connected():
                raise ConnectionError(f"Unable to connect to {self.host}:{self.port}")

            if max_per_request is None:
                max_allowed = self._max_read.get(type, ModbusConnection.DEFAULT_MAX_READ.get(type, 100))
            else:
                max_allowed = int(max_per_request)
            if max_allowed <= 0:
                max_allowed = ModbusConnection.DEFAULT_MAX_READ.get(type, 100)

            try:
                # Fast path: if count <= max_allowed, do one request (with normal error propagation)
                if count <= max_allowed:
                    values = self._single_read(type, address, count)
                    self.last_read = {"timestamp": time.time(), "type": type, "address": address, "count": count, "values": values}
                    return values

                # 分片读取
                results: List[int] = []
                remaining = count
                offset = 0
                while remaining > 0:
                    chunk = remaining if remaining <= max_allowed else max_allowed
                    chunk_addr = address + offset
                    part = self._single_read(type, chunk_addr, chunk)
                    results.extend(part)
                    remaining -= len(part)
                    offset += len(part)
                    if len(part) < chunk:
                        break
                    if remaining > 0:
                        time.sleep(self._inter_request_delay)
                results = results[:count]
                self.last_read = {"timestamp": time.time(), "type": type, "address": address, "count": count, "values": results}
                return results

            except ModbusIOException as me:
                msg = str(me)
                logger.debug("ModbusIOException during read: %s", msg)
                # 如果是 IllegalAddress，并且允许探测替代地址，则探测邻近地址
                if try_alternatives and ("IllegalAddress" in msg or "Illegal Data Address" in msg or "Illegal" in msg):
                    logger.info("IllegalAddress received at addr %s; trying alternatives in +/-%s range", address, alt_probe_range)
                    # 探测从小到大的偏移（先 +1, -1, +2, -2 ...）
                    for delta in range(1, alt_probe_range + 1):
                        for sign in (1, -1):
                            probe_addr = address + sign * delta
                            if probe_addr < 0:
                                continue
                            if self._probe_address(type, probe_addr):
                                # 找到可读地址，尝试从该地址按原始 count 读取（使用分片逻辑）
                                logger.info("Found readable address at %s (adjusted from %s)", probe_addr, address)
                                # 继续使用正常分片读取逻辑从 probe_addr 读取 count 个
                                values = self.read(type=type, address=probe_addr, count=count, max_per_request=max_per_request, try_alternatives=False)
                                # 标注 last_read 为已调整
                                if isinstance(self.last_read, dict):
                                    self.last_read["adjusted_from"] = address
                                    self.last_read["adjusted_to"] = probe_addr
                                return values
                    # 若探测失败，抛出原始异常（带更多上下文）
                    raise ModbusIOException(f"{msg} (and probe in +/-{alt_probe_range} failed)")
                # 否则直接把异常向上抛
                self.connected = False
                raise

            except Exception:
                self.connected = False
                logger.exception("Exception during Modbus read")
                raise

    def write(self, type: str = "holding", address: int = 0, values: List[int] = []):
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

    # start_poll / stop_poll / manager code 保持不变（略）
    def start_poll(self, type: str, address: int, count: int, interval: float, max_history: int = 100):
        self.stop_poll()
        self._poll_stop.clear()
        self._poll_params = {"type": type, "address": address, "count": count, "interval": interval, "max_history": max_history}
        def _worker():
            while not self._poll_stop.wait(0):
                start = time.time()
                try:
                    values = self.read(type=type, address=address, count=count)
                    entry = {"timestamp": time.time(), "type": type, "address": address, "count": count, "values": values, "error": None}
                except Exception as e:
                    entry = {"timestamp": time.time(), "type": type, "address": address, "count": count, "values": None, "error": str(e)}
                self.poll_history.append(entry)
                if len(self.poll_history) > max_history:
                    self.poll_history.pop(0)
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

    def create_connection(self, host: str, port: int = 502, unit: int = 1, name: Optional[str] = None, max_read_override: Optional[Dict[str,int]] = None) -> ModbusConnection:
        conn = ModbusConnection(host=host, port=port, unit=unit, name=name, max_read_override=max_read_override)
        with self._lock:
            self._conns[conn.id] = conn
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

manager = ModbusManager()