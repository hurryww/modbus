import streamlit as st
from modbus_manager import manager
import time

# Optional helper: pip install streamlit-autorefresh
try:
    from streamlit_autorefresh import st_autorefresh
except Exception:
    st_autorefresh = None

st.set_page_config(page_title="Modbus TCP Manager", layout="wide")
st.title("Modbus TCP Manager")


def rerun():
    try:
        if hasattr(st, "experimental_rerun"):
            st.experimental_rerun()
            return
    except Exception:
        # best-effort rerun fallback
        try:
            params = dict(st.experimental_get_query_params())
            params["_rerun"] = [str(time.time())]
            st.experimental_set_query_params(**params)
        except Exception:
            st.session_state["_rerun_flag"] = time.time()


def ensure_session_default(key: str, default):
    if key not in st.session_state:
        st.session_state[key] = default


# ---------- 辅助函数 ----------
def find_existing_connection(host: str, port: int, unit: int):
    """
    在 manager.list_connections() 中查找是否存在相同 host/port/unit 的连接。
    返回匹配的连接字典（第一个匹配项），如果没有则返回 None。
    """
    try:
        conns = manager.list_connections()
    except Exception:
        return None

    for c in conns:
        try:
            if (
                str(c.get("host")) == str(host)
                and int(c.get("port")) == int(port)
                and int(c.get("unit")) == int(unit)
            ):
                return c
        except Exception:
            continue
    return None


def plc_to_modbus(plc_addr: int, base: int) -> int:
    plc_addr = int(plc_addr)
    if plc_addr >= base:
        return plc_addr - base
    return plc_addr


# ---------- 初始化 session state ----------
ensure_session_default("read_values", {})
ensure_session_default("last_modbus_address", {})
ensure_session_default("last_plc_address", {})
ensure_session_default("clone_map", {})  # mapping parent_id -> list of child_ids
ensure_session_default("_write_flags", {})  # mapping safe_id -> bool for inline write visibility
ensure_session_default("editing_cell", None)

# Sidebar: create connection form
with st.sidebar.expander("新增 Modbus TCP 连接", expanded=True):
    with st.form("create_conn"):
        host = st.text_input("Host (IP or hostname)", value="127.0.0.1")
        port = st.number_input("Port", value=502, min_value=1, max_value=65535, step=1)
        unit = st.number_input("Unit ID", value=1, min_value=0, max_value=255, step=1)
        name = st.text_input("名称（可选）")
        submitted = st.form_submit_button("创建连接")
        if submitted:
            try:
                existing = find_existing_connection(host=host, port=int(port), unit=int(unit))
                if existing is not None:
                    existing_name = existing.get("name") or f'{existing.get("host")}:{existing.get("port")}'
                    st.warning(
                        f"已存在相同的连接：ID={existing.get('id')}，名称={existing_name}。已阻止重复创建。"
                    )
                else:
                    conn = manager.create_connection(host=host, port=int(port), unit=int(unit), name=name or None)
                    # 自动连接，最多重试 5 次
                    max_attempts = 5
                    attempt = 0
                    connected = False
                    backoff_base = 0.5
                    st.info(f"正在尝试自动连接 ({max_attempts} 次重试)...")
                    while attempt < max_attempts:
                        attempt += 1
                        try:
                            ok = conn.connect()
                        except Exception:
                            ok = False
                        if ok:
                            connected = True
                            break
                        time.sleep(backoff_base * attempt)
                    if connected:
                        st.session_state.pop(f"conn_failed_{conn.id}", None)
                        st.success(f"创建并已连接: {conn.id} ({conn.name})")
                        rerun()
                    else:
                        st.session_state[f"conn_failed_{conn.id}"] = True
                        st.error(f"创建连接 {conn.id} 但自动连接失败（尝试 {max_attempts} 次）")
                        rerun()
            except Exception as e:
                st.error(f"创建失败: {e}")

# Get connections
conns = manager.list_connections()
conn_map = {c["id"]: c for c in conns}
selected_ids = [c["id"] for c in conns] if conns else []

if not selected_ids:
    st.info("当前没有连接，请先在左边栏创建一个或多个连接。")
    st.stop()

st.markdown("## 已连接（自动显示所有连接，克隆项与原连接放在同一方框） ")

# Function options: (display string, internal type, base address for PLC example)
FUNCTION_OPTIONS = [
    ("01 Coil Status (0x) - Coil", "coils", 1),
    ("02 Input Status (1x) - Discrete Input", "discrete", 10001),
    ("03 Holding Register (4x) - Holding", "holding", 40001),
    ("04 Input Registers (3x) - Input Reg", "input", 30001),
]
func_display_list = [opt[0] for opt in FUNCTION_OPTIONS]

# Clean clone_map
clean_clone_map = {}
for parent_id, children in st.session_state["clone_map"].items():
    valid_children = [cid for cid in children if cid in conn_map]
    if valid_children and parent_id in conn_map:
        clean_clone_map[parent_id] = valid_children
st.session_state["clone_map"] = clean_clone_map

# Build child -> parent
child_to_parent = {}
for p, childs in st.session_state["clone_map"].items():
    for ch in childs:
        child_to_parent[ch] = p

top_level_ids = [cid for cid in selected_ids if cid not in child_to_parent]


def render_connection_panel(conn_id: str, show_clone_button: bool):
    conn_meta = manager.get(conn_id)
    if conn_meta is None:
        st.warning(f"连接 {conn_id} 不存在")
        return

    cid = conn_meta.id
    safe = cid.replace("-", "_")
    func_key = f"func_opt_{safe}"
    plc_key = f"plc_addr_{safe}"
    count_key = f"count_{safe}"
    write_flag_key = f"write_flag_{safe}"

    ensure_session_default(func_key, func_display_list[2])
    if st.session_state.get(func_key) not in func_display_list:
        st.session_state[func_key] = func_display_list[2]
    ensure_session_default(plc_key, FUNCTION_OPTIONS[2][2])
    ensure_session_default(count_key, 4)
    if write_flag_key not in st.session_state.get("_write_flags", {}):
        st.session_state["_write_flags"][write_flag_key] = False

    cur_func = st.session_state.get(func_key, func_display_list[2])
    try:
        cur_idx = func_display_list.index(cur_func)
        cur_base = FUNCTION_OPTIONS[cur_idx][2]
    except Exception:
        cur_idx = 2
        cur_base = FUNCTION_OPTIONS[2][2]

    display_name = conn_meta.name or f"{conn_meta.host}:{conn_meta.port}"
    st.markdown(
        f"### {display_name}  ({conn_meta.host}:{conn_meta.port})  ID: {conn_meta.id}  Unit: {conn_meta.unit}  状态: {'已连接' if conn_meta.connected else '未连接'}"
    )

    if st.session_state.get(f"conn_failed_{cid}", False):
        st.error("自动连接失败：已尝试 5 次，仍未连接。")

    cols = st.columns([3, 2, 1])
    with cols[0]:
        sel = st.selectbox(f"功能（{display_name}）", options=func_display_list, key=func_key)
        sel_idx = func_display_list.index(sel)
        func_type = FUNCTION_OPTIONS[sel_idx][1]
        func_base = FUNCTION_OPTIONS[sel_idx][2]
    with cols[1]:
        plc_val = st.number_input(f"PLC 地址（示例 {func_base}）", min_value=0, step=1, key=plc_key)
    with cols[2]:
        cnt_val = st.number_input("数量", min_value=1, step=1, key=count_key)

    # second row: Read | Write (批量) | Clone | Delete
    btn_cols = st.columns([1, 1, 1, 1])
    with btn_cols[0]:
        if st.button("读取", key=f"read_btn_{safe}"):
            try:
                cur_plc = int(st.session_state.get(plc_key))
                cur_cnt = int(st.session_state.get(count_key))
                cur_func_display = st.session_state.get(func_key)
                cur_idx = func_display_list.index(cur_func_display) if cur_func_display in func_display_list else 2
                cur_func_type = FUNCTION_OPTIONS[cur_idx][1]
                cur_func_base = FUNCTION_OPTIONS[cur_idx][2]
                modbus_address = plc_to_modbus(cur_plc, cur_func_base)
                if not conn_meta.connected:
                    ok = conn_meta.connect()
                    if not ok:
                        raise ConnectionError("connect failed")
                values = conn_meta.read(type=cur_func_type, address=int(modbus_address), count=int(cur_cnt), allow_reconnect=True)
                st.session_state["read_values"][cid] = values
                st.session_state["last_modbus_address"][cid] = int(modbus_address)
                st.session_state["last_plc_address"][cid] = int(cur_plc)
                if conn_meta.connected:
                    st.session_state.pop(f"conn_failed_{cid}", None)
                st.success(f"{display_name} 读取成功")
            except ConnectionError as ce:
                st.error(f"{display_name} 未连接：{ce}")
            except Exception as e:
                st.session_state.get("read_values", {}).pop(cid, None)
                st.session_state.get("last_modbus_address", {}).pop(cid, None)
                st.session_state.get("last_plc_address", {}).pop(cid, None)
                st.error(f"读取失败: {e}")
            rerun()

    with btn_cols[1]:
        # Batch write
        write_flag = st.session_state["_write_flags"].get(write_flag_key, False)
        if not write_flag:
            if st.button("写入", key=f"write_toggle_{safe}"):
                st.session_state["_write_flags"][write_flag_key] = True
                rerun()
        else:
            cur_func_display = st.session_state.get(func_key)
            cur_idx = func_display_list.index(cur_func_display) if cur_func_display in func_display_list else 2
            cur_func_type = FUNCTION_OPTIONS[cur_idx][1]
            cur_func_base = FUNCTION_OPTIONS[cur_idx][2]

            with st.form(f"batch_write_form_{safe}"):
                start_plc = st.number_input(
                    "PLC 地址起始",
                    min_value=0,
                    step=1,
                    value=int(st.session_state.get(plc_key, cur_func_base)),
                    key=f"batch_plc_{safe}",
                )
                cnt = st.number_input(
                    "数量",
                    min_value=1,
                    step=1,
                    value=int(st.session_state.get(count_key, 4)),
                    key=f"batch_cnt_{safe}",
                )

                # default batch values
                try:
                    default_vals = st.session_state.get("read_values", {}).get(cid)
                    if default_vals is None:
                        default_batch = ",".join("0" for _ in range(int(cnt)))
                    else:
                        slice_vals = default_vals[:int(cnt)]
                        default_batch = ",".join(str(v) for v in slice_vals)
                        if len(slice_vals) < int(cnt):
                            default_batch += (
                                ","
                                + ",".join("0" for _ in range(int(cnt) - len(slice_vals)))
                                if int(cnt) - len(slice_vals) > 0
                                else ""
                            )
                except Exception:
                    default_batch = ",".join("0" for _ in range(int(cnt)))

                batch_widget_key = f"batch_values_{safe}"
                batch_saved_key = f"batch_saved_{safe}"
                initial_batch_value = st.session_state.get(batch_saved_key, default_batch)

                batch_text = st.text_area(
                    "批量值，逗号或空白分隔（数量应与 上方 数量 相同）",
                    value=initial_batch_value,
                    key=batch_widget_key,
                    height=100,
                )
                submit_write = st.form_submit_button("写入确认")
                cancel_write = st.form_submit_button("取消")
                if submit_write:
                    raw = batch_text.strip()
                    if not raw:
                        st.error("批量值为空，请输入值。")
                    else:
                        parts = [p for p in [x.strip() for x in raw.replace("\n", " ").replace("\t", " ").split(",")] if p != ""]
                        if len(parts) == 1 and (" " in parts[0]):
                            parts = [p for p in parts[0].split() if p != ""]
                        final_vals = []
                        for token in parts:
                            for sub in token.split():
                                if sub != "":
                                    final_vals.append(sub)
                        try:
                            if cur_func_type == "coils":
                                parsed = [1 if token not in ("0", "False", "false", "off", "OFF") else 0 for token in final_vals]
                            else:
                                parsed = [int(token) for token in final_vals]
                        except Exception as e:
                            st.error(f"解析批量值失败，请确保为整数（或布尔）: {e}")
                            parsed = None

                        if parsed is not None:
                            desired = int(cnt)
                            if len(parsed) < desired:
                                parsed = parsed + [0] * (desired - len(parsed))
                            elif len(parsed) > desired:
                                parsed = parsed[:desired]

                            modbus_address = plc_to_modbus(start_plc, cur_func_base)

                            try:
                                if not conn_meta.connected:
                                    ok = conn_meta.connect()
                                    if not ok:
                                        st.error("与设备连接失败，无法写入")
                                        raise ConnectionError("connect failed")
                                if hasattr(conn_meta, "write"):
                                    try:
                                        conn_meta.write(type=cur_func_type, address=int(modbus_address), value=parsed, allow_reconnect=True)
                                    except TypeError:
                                        for idx, v in enumerate(parsed):
                                            conn_meta.write(type=cur_func_type, address=int(modbus_address) + idx, value=v, allow_reconnect=True)
                                else:
                                    raise RuntimeError("连接对象不支持写操作")
                                try:
                                    rv = st.session_state.get("read_values", {}).get(cid)
                                    lm = st.session_state.get("last_modbus_address", {}).get(cid)
                                    if rv is not None and lm is not None:
                                        for idx in range(desired):
                                            target_addr = int(modbus_address) + idx
                                            if int(lm) <= target_addr < int(lm) + len(rv):
                                                rv_idx = int(target_addr) - int(lm)
                                                rv[rv_idx] = parsed[idx]
                                except Exception:
                                    pass

                                st.success("批量写入成功")
                            except Exception as e:
                                st.error(f"写入失败: {e}")

                        st.session_state[batch_saved_key] = batch_text
                        st.session_state["_write_flags"][write_flag_key] = False
                        rerun()
                if cancel_write:
                    st.session_state["_write_flags"][write_flag_key] = False
                    rerun()

    with btn_cols[2]:
        # Clone (自动连接新 clone)
        if show_clone_button:
            if st.button("新建", key=f"clone_{safe}"):
                try:
                    clone_name = f"{conn_meta.name}_copy" if conn_meta.name else None
                    new_conn = manager.create_connection(host=conn_meta.host, port=conn_meta.port, unit=conn_meta.unit, name=clone_name)
                    max_attempts = 5
                    attempt = 0
                    connected = False
                    backoff_base = 0.5
                    while attempt < max_attempts:
                        attempt += 1
                        try:
                            ok = new_conn.connect()
                        except Exception:
                            ok = False
                        if ok:
                            connected = True
                            break
                        time.sleep(backoff_base * attempt)
                    if connected:
                        st.success(f"已创建并连接新连接: {new_conn.id} ({new_conn.name})")
                        st.session_state.pop(f"conn_failed_{new_conn.id}", None)
                    else:
                        st.session_state[f"conn_failed_{new_conn.id}"] = True
                        st.error(f"已创建新连接 {new_conn.id}，但自动连接失败（尝试 {max_attempts} 次）")
                    lst = st.session_state["clone_map"].setdefault(conn_meta.id, [])
                    if new_conn.id not in lst:
                        lst.append(new_conn.id)
                    rerun()
                except Exception as e:
                    st.error(f"新建连接失败: {e}")

    with btn_cols[3]:
        # 删除连接
        if st.button("删除", key=f"delete_{safe}"):
            try:
                manager.remove(cid)
                st.session_state.get("read_values", {}).pop(cid, None)
                st.session_state.get("last_modbus_address", {}).pop(cid, None)
                st.session_state.get("last_plc_address", {}).pop(cid, None)
                st.session_state["clone_map"].pop(cid, None)
                for p in list(st.session_state["clone_map"].keys()):
                    lst = st.session_state["clone_map"].get(p, [])
                    if cid in lst:
                        try:
                            lst.remove(cid)
                        except ValueError:
                            pass
                        if not lst:
                            st.session_state["clone_map"].pop(p, None)
                        else:
                            st.session_state["clone_map"][p] = lst
                st.success(f"{display_name} 已删除")
            except Exception as e:
                st.error(f"删除失败: {e}")
            rerun()


# --- 自动轮询 / 自动刷新（用于检测 PLC 外部修改并在页面刷新显示） ---
# 如果安装了 streamlit-autorefresh，则启用自动刷新（毫秒）
REFRESH_INTERVAL_MS = 3000  # 3s，按需调整
if st_autorefresh is not None:
    st_autorefresh(interval=REFRESH_INTERVAL_MS, key="autorefresh")

# 轮询：尽量使用面板上当前配置（plc addr / count / function）来读取最新值
for cid in list(selected_ids):
    conn_meta = manager.get(cid)
    if conn_meta is None:
        continue
    try:
        safe = cid.replace("-", "_")
        cur_plc = int(st.session_state.get(f"plc_addr_{safe}", FUNCTION_OPTIONS[2][2]))
        cur_cnt = int(st.session_state.get(f"count_{safe}", 4))
        func_opt = st.session_state.get(f"func_opt_{safe}", func_display_list[2])
        cur_idx = func_display_list.index(func_opt) if func_opt in func_display_list else 2
        cur_func_type = FUNCTION_OPTIONS[cur_idx][1]
        cur_func_base = FUNCTION_OPTIONS[cur_idx][2]
        modbus_address = plc_to_modbus(cur_plc, cur_func_base)
    except Exception:
        continue

    # 如果连接被标记为自动连接失败则跳过轮询（避免重复重试刷屏），否则短连接尝试读取
    if st.session_state.get(f"conn_failed_{cid}", False):
        continue

    try:
        values = conn_meta.read(type=cur_func_type, address=int(modbus_address), count=int(cur_cnt), allow_reconnect=True)
        st.session_state["read_values"][cid] = values
        st.session_state["last_modbus_address"][cid] = int(modbus_address)
        st.session_state["last_plc_address"][cid] = int(cur_plc)
        # clear failure mark if any
        st.session_state.pop(f"conn_failed_{cid}", None)
    except Exception:
        # 忽略单次读取失败，保留上一次显示的值
        pass

# Render groups
for parent_id in top_level_ids:
    render_connection_panel(parent_id, show_clone_button=True)

    children = st.session_state["clone_map"].get(parent_id, [])
    for child_id in children:
        cols = st.columns([0.5, 9.5])
        with cols[0]:
            st.write("")
        with cols[1]:
            render_connection_panel(child_id, show_clone_button=False)

    st.markdown("---")

# Read results area (uses st.session_state["read_values"] prepared above)
st.markdown("## 读取结果（按连接分组）")
for cid in selected_ids:
    conn_meta = manager.get(cid)
    if conn_meta is None:
        st.warning(f"连接 {cid} 已不存在")
        continue
    st.markdown(f"**{conn_meta.name}  ({conn_meta.host}:{conn_meta.port})**  ID: {conn_meta.id}  Unit: {conn_meta.unit}")
    read_values = st.session_state["read_values"].get(cid)
    last_modbus_address = st.session_state["last_modbus_address"].get(cid)
    last_plc_address = st.session_state["last_plc_address"].get(cid)
    if read_values is None:
        st.info("无读取结果或读取失败（见上方错误信息）")
        continue

    cols = st.columns([2, 2, 3])
    cols[0].markdown("**PLC 地址**")
    cols[1].markdown("**Modbus 地址**")
    cols[2].markdown("**值（点击可直接修改）**")

    for i, cur in enumerate(read_values):
        addr_modbus = (last_modbus_address + i) if last_modbus_address is not None else i
        addr_plc = (last_plc_address + i) if last_plc_address is not None else i
        c0, c1, c2 = st.columns([2, 2, 3])
        c0.write(addr_plc)
        c1.write(addr_modbus)

        safe = cid.replace("-", "_")
        func_opt = st.session_state.get(f"func_opt_{safe}", None)
        func_idx = func_display_list.index(func_opt) if func_opt in func_display_list else 2
        func_type_for_edit = FUNCTION_OPTIONS[func_idx][1]

        editing = st.session_state.get("editing_cell")
        is_editing_this = editing and editing.get("conn_id") == cid and editing.get("address") == addr_modbus

        if is_editing_this:
            if func_type_for_edit == "coils":
                widget_key = f"edit_input_{cid}_{addr_modbus}"
                new_val_bool = c2.checkbox("值编辑", value=bool(st.session_state["read_values"][cid][i]), key=widget_key, label_visibility="collapsed")
                btn_left, btn_right = c2.columns([1, 1])
                if btn_left.button("确认", key=f"confirm_{cid}_{addr_modbus}"):
                    try:
                        new_val = 1 if bool(st.session_state.get(widget_key)) else 0
                        if not conn_meta.connected:
                            ok = conn_meta.connect()
                            if not ok:
                                st.error("与设备连接失败，无法写入")
                                raise ConnectionError("connect failed")
                        if hasattr(conn_meta, "write"):
                            conn_meta.write(type=func_type_for_edit, address=int(addr_modbus), value=new_val, allow_reconnect=True)
                        st.session_state["read_values"][cid][i] = new_val
                        st.success("写入成功")
                    except Exception as e:
                        st.error(f"写入失败: {e}")
                    st.session_state["editing_cell"] = None
                    rerun()
                if btn_right.button("取消", key=f"cancel_{cid}_{addr_modbus}"):
                    st.session_state["editing_cell"] = None
                    rerun()
            else:
                widget_key = f"edit_input_{cid}_{addr_modbus}"
                try:
                    default_val = int(st.session_state["read_values"][cid][i])
                except Exception:
                    default_val = 0
                new_val = c2.number_input("值编辑", value=default_val, step=1, key=widget_key, label_visibility="collapsed")
                btn_left, btn_right = c2.columns([1, 1])
                if btn_left.button("确认", key=f"confirm_{cid}_{addr_modbus}"):
                    try:
                        new_int = int(st.session_state.get(widget_key))
                        if not conn_meta.connected:
                            ok = conn_meta.connect()
                            if not ok:
                                st.error("与设备连接失败，无法写入")
                                raise ConnectionError("connect failed")
                        if hasattr(conn_meta, "write"):
                            conn_meta.write(type=func_type_for_edit, address=int(addr_modbus), value=new_int, allow_reconnect=True)
                        st.session_state["read_values"][cid][i] = int(new_int)
                        st.success("写入成功")
                    except Exception as e:
                        st.error(f"写入失败: {e}")
                    st.session_state["editing_cell"] = None
                    rerun()
                if btn_right.button("取消", key=f"cancel_{cid}_{addr_modbus}"):
                    st.session_state["editing_cell"] = None
                    rerun()
        else:
            value_click_key = f"value_click_{cid}_{addr_modbus}"
            if c2.button(str(cur), key=value_click_key):
                st.session_state["editing_cell"] = {
                    "conn_id": cid,
                    "address": addr_modbus,
                    "index": i,
                    "type": func_type_for_edit,
                }
                rerun()
