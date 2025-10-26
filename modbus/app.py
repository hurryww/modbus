import streamlit as st
from modbus_manager import manager
import time

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

# initialize persistent UI session keys
ensure_session_default("read_values", {})
ensure_session_default("last_modbus_address", {})
ensure_session_default("last_plc_address", {})
ensure_session_default("clone_map", {})  # mapping parent_id -> list of child_ids
ensure_session_default("_write_flags", {})  # mapping safe_id -> bool for inline write visibility

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
                conn = manager.create_connection(host=host, port=int(port), unit=int(unit), name=name or None)
                st.success(f"创建连接: {conn.id} ({conn.name})")
                rerun()
            except Exception as e:
                st.error(f"创建失败: {e}")

# Get connections; automatically show connections in main area
conns = manager.list_connections()
conn_map = {c["id"]: c for c in conns}
selected_ids = [c["id"] for c in conns] if conns else []

if not selected_ids:
    st.info("当前没有连接，请先在侧边栏创建一个或多个连接。")
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

def plc_to_modbus(plc_addr: int, base: int) -> int:
    plc_addr = int(plc_addr)
    if plc_addr >= base:
        return plc_addr - base
    return plc_addr

# Clean clone_map: remove child ids that no longer exist; remove parents without children
clean_clone_map = {}
for parent_id, children in st.session_state["clone_map"].items():
    valid_children = [cid for cid in children if cid in conn_map]
    if valid_children and parent_id in conn_map:
        clean_clone_map[parent_id] = valid_children
st.session_state["clone_map"] = clean_clone_map

# Build child -> parent index for quick lookup and top-level parent list
child_to_parent = {}
for p, childs in st.session_state["clone_map"].items():
    for ch in childs:
        child_to_parent[ch] = p

top_level_ids = [cid for cid in selected_ids if cid not in child_to_parent]

# Helper to render a single connection panel.
# show_clone_button: True for parent panels so user can create clones; False for cloned panels.
def render_connection_panel(conn_id: str, show_clone_button: bool):
    conn_meta = manager.get(conn_id)
    if conn_meta is None:
        st.warning(f"连接 {conn_id} 不存在")
        return

    cid = conn_meta.id
    safe = cid.replace("-", "_")
    func_key = f"func_opt_{safe}"    # selectbox key (stores option string)
    plc_key = f"plc_addr_{safe}"
    count_key = f"count_{safe}"
    prev_key = f"prev_func_{safe}"   # previous function selection
    write_flag_key = f"write_flag_{safe}"

    ensure_session_default(func_key, func_display_list[2])  # default to Holding
    if st.session_state.get(func_key) not in func_display_list:
        st.session_state[func_key] = func_display_list[2]
    ensure_session_default(prev_key, st.session_state.get(func_key, func_display_list[2]))
    # store per-connection write toggle in a map to avoid too many top-level keys
    if write_flag_key not in st.session_state.get("_write_flags", {}):
        st.session_state["_write_flags"][write_flag_key] = False

    # compute current and previous bases
    cur_func = st.session_state.get(func_key, func_display_list[2])
    try:
        cur_idx = func_display_list.index(cur_func)
        cur_base = FUNCTION_OPTIONS[cur_idx][2]
    except Exception:
        cur_idx = 2
        cur_base = FUNCTION_OPTIONS[2][2]

    prev_func = st.session_state.get(prev_key, None)
    prev_base = None
    if prev_func in func_display_list:
        prev_base = FUNCTION_OPTIONS[func_display_list.index(prev_func)][2]

    ensure_session_default(plc_key, cur_base)
    try:
        plc_val_current = int(st.session_state.get(plc_key, cur_base))
    except Exception:
        plc_val_current = cur_base
    ensure_session_default(count_key, 4)
    try:
        st.session_state[count_key] = int(st.session_state.get(count_key, 4))
    except Exception:
        st.session_state[count_key] = 4

    # if function changed (prev -> cur) update plc to cur_base unconditionally
    if prev_func is not None and prev_func != cur_func:
        st.session_state[plc_key] = cur_base
        st.session_state[prev_key] = cur_func

    # Panel header (display name: if name is None fall back to host:port)
    display_name = conn_meta.name or f"{conn_meta.host}:{conn_meta.port}"
    st.markdown(f"### {display_name}  ({conn_meta.host}:{conn_meta.port})  ID: {conn_meta.id}  Unit: {conn_meta.unit}  状态: {'已连接' if conn_meta.connected else '未连接'}")

    # top row: function | plc address | count
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

    # second row: Connect | Disconnect | Read | Write (and optionally Clone)
    # For cloned connections we hide the Clone button by passing show_clone_button=False
    btn_cols = st.columns([1, 1, 1, 1, 1])
    with btn_cols[0]:
        if st.button("连接", key=f"connect_{safe}"):
            try:
                ok = conn_meta.connect()
                if ok:
                    st.success(f"{display_name} 已连接")
                else:
                    st.error(f"{display_name} 连接失败")
            except Exception as e:
                st.error(f"{display_name} 连接异常: {e}")
            rerun()
    with btn_cols[1]:
        if st.button("断开", key=f"close_{safe}"):
            try:
                conn_meta.close()
                st.session_state.get("read_values", {}).pop(cid, None)
                st.session_state.get("last_modbus_address", {}).pop(cid, None)
                st.session_state.get("last_plc_address", {}).pop(cid, None)
                st.info(f"{display_name} 已断开")
            except Exception as e:
                st.error(f"{display_name} 断开异常: {e}")
            rerun()
    with btn_cols[2]:
        if st.button("读取", key=f"read_btn_{safe}"):
            try:
                cur_plc = int(st.session_state.get(plc_key))
                cur_cnt = int(st.session_state.get(count_key))

                cur_func_display = st.session_state.get(func_key)
                if cur_func_display in func_display_list:
                    cur_idx = func_display_list.index(cur_func_display)
                else:
                    cur_idx = cur_idx
                cur_func_type = FUNCTION_OPTIONS[cur_idx][1]
                cur_func_base = FUNCTION_OPTIONS[cur_idx][2]
                modbus_address = plc_to_modbus(cur_plc, cur_func_base)

                # ensure connected (try to connect if not)
                if not conn_meta.connected:
                    ok = conn_meta.connect()
                    if not ok:
                        raise ConnectionError("connect failed")

                values = conn_meta.read(type=cur_func_type, address=int(modbus_address), count=int(cur_cnt))
                st.session_state["read_values"][cid] = values
                st.session_state["last_modbus_address"][cid] = int(modbus_address)
                st.session_state["last_plc_address"][cid] = int(cur_plc)
                st.success(f"{display_name} 读取成功")
            except ConnectionError as ce:
                st.error(f"{display_name} 未连接：{ce}")
            except Exception as e:
                st.session_state.get("read_values", {}).pop(cid, None)
                st.session_state.get("last_modbus_address", {}).pop(cid, None)
                st.session_state.get("last_plc_address", {}).pop(cid, None)
                st.error(f"读取失败: {e}")
            rerun()
    with btn_cols[3]:
        write_flag = st.session_state["_write_flags"].get(write_flag_key, False)
        if not write_flag:
            if st.button("写入", key=f"write_toggle_{safe}"):
                st.session_state["_write_flags"][write_flag_key] = True
                rerun()
        else:
            # show inline write form
            cur_func_display = st.session_state.get(func_key)
            if cur_func_display in func_display_list:
                cur_idx = func_display_list.index(cur_func_display)
            else:
                cur_idx = cur_idx
            cur_func_type = FUNCTION_OPTIONS[cur_idx][1]
            cur_func_base = FUNCTION_OPTIONS[cur_idx][2]

            with st.form(f"write_form_{safe}"):
                default_addr = int(st.session_state.get(plc_key, cur_func_base))
                write_plc = st.number_input("PLC 地址", min_value=0, step=1, value=default_addr, key=f"write_plc_{safe}")
                if cur_func_type == "coils":
                    write_val = st.checkbox("新值（勾选=1）", value=False, key=f"write_coil_{safe}")
                else:
                    write_val = st.number_input("新值（整数）", value=0, step=1, key=f"write_hold_{safe}")
                submit_write = st.form_submit_button("写入确认")
                cancel_write = st.form_submit_button("取消")
                if submit_write:
                    try:
                        modbus_address = plc_to_modbus(write_plc, cur_func_base)
                        if not conn_meta.connected:
                            ok = conn_meta.connect()
                            if not ok:
                                raise ConnectionError("connect failed")
                        val_to_write = 1 if (cur_func_type == "coils" and bool(write_val)) else int(write_val)
                        if hasattr(conn_meta, "write"):
                            conn_meta.write(type=cur_func_type, address=int(modbus_address), value=val_to_write)
                        st.success("写入成功")
                        st.session_state["_write_flags"][write_flag_key] = False
                        rerun()
                    except Exception as e:
                        st.error(f"写入失败: {e}")
                if cancel_write:
                    st.session_state["_write_flags"][write_flag_key] = False
                    rerun()
    with btn_cols[4]:
        # Only show Clone button for non-clone (parent) panels
        if show_clone_button:
            if st.button("新建", key=f"clone_{safe}"):
                try:
                    clone_name = f"{conn_meta.name}_copy" if conn_meta.name else None
                    new_conn = manager.create_connection(host=conn_meta.host, port=conn_meta.port, unit=conn_meta.unit, name=clone_name)
                    # record clone relationship in session_state clone_map
                    lst = st.session_state["clone_map"].setdefault(conn_meta.id, [])
                    if new_conn.id not in lst:
                        lst.append(new_conn.id)
                    st.success(f"已创建新连接: {new_conn.id} ({new_conn.name})")
                    rerun()
                except Exception as e:
                    st.error(f"新建连接失败: {e}")

# Render groups: each top-level connection and its clones inside the same box
for parent_id in top_level_ids:
    # parent panel (show clone button)
    render_connection_panel(parent_id, show_clone_button=True)

    # render clones (if any) under the same visual group (no extra divider between parent and its clones)
    children = st.session_state["clone_map"].get(parent_id, [])
    for child_id in children:
        # small visual indentation: use columns to simulate a boxed child area
        cols = st.columns([0.5, 9.5])
        with cols[0]:
            st.write("")  # spacer column for indentation
        with cols[1]:
            render_connection_panel(child_id, show_clone_button=False)

    # after parent + its clones, add a divider to separate groups
    st.markdown("---")

# Read results area
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

    cols = st.columns([2, 2, 3, 1])
    cols[0].markdown("**PLC 地址**")
    cols[1].markdown("**Modbus 地址**")
    cols[2].markdown("**值**")
    cols[3].markdown("**操作**")

    for i, cur in enumerate(read_values):
        addr_modbus = (last_modbus_address + i) if last_modbus_address is not None else i
        addr_plc = (last_plc_address + i) if last_plc_address is not None else i
        c0, c1, c2, c3 = st.columns([2, 2, 3, 1])
        c0.write(addr_plc)
        c1.write(addr_modbus)
        c2.write(cur)
        edit_key = f"edit_btn_{cid}_{addr_modbus}"
        if c3.button("编辑", key=edit_key):
            st.session_state["edit"] = {
                "type": None,
                "plc_addr": addr_plc,
                "address": addr_modbus,
                "value": cur,
                "conn_id": cid,
            }

# Edit modal handling
if "edit" in st.session_state:
    edit = st.session_state["edit"]
    edit_conn_id = edit.get("conn_id")
    conn_meta = manager.get(edit_conn_id)
    if conn_meta is None:
        st.warning("编辑项对应的连接不存在或已被移除")
        st.session_state.pop("edit", None)
    else:
        safe = edit_conn_id.replace("-", "_")
        func_opt = st.session_state.get(f"func_opt_{safe}", None)
        try:
            func_idx = func_display_list.index(func_opt) if func_opt in func_display_list else 2
        except Exception:
            func_idx = 2
        func_type = FUNCTION_OPTIONS[func_idx][1]
        edit["type"] = func_type
        try:
            with st.modal(f"编辑寄存器 PLC {edit['plc_addr']} (Modbus {edit['address']})"):
                st.write(f"连接: {conn_meta.name}  Unit: {conn_meta.unit}")
                st.write(f"寄存器类型: {edit['type']}")
                st.write(f"当前值: {edit['value']}")
                with st.form("edit_form"):
                    if edit["type"] == "coils":
                        new_val = st.checkbox("新值（勾选=1）", value=bool(edit["value"]), key=f"modal_coil_{edit['address']}_{safe}")
                    else:
                        new_val = st.number_input("新值（整数）", value=int(edit["value"]), step=1, key=f"modal_hold_{edit['address']}_{safe}")
                    submitted = st.form_submit_button("写入")
                    cancel = st.form_submit_button("取消")
                    if submitted:
                        try:
                            val_to_write = 1 if (edit["type"] == "coils" and bool(new_val)) else int(new_val)
                            if not conn_meta.connected:
                                ok = conn_meta.connect()
                                if not ok:
                                    st.error("与设备连接失败，无法写入")
                                    raise ConnectionError("connect failed")
                            if hasattr(conn_meta, "write"):
                                conn_meta.write(type=edit["type"], address=int(edit["address"]), value=val_to_write)
                            st.success("写入成功")
                            st.session_state.pop("edit", None)
                            rerun()
                        except Exception as e:
                            st.error(f"写入失败: {e}")
                    if cancel:
                        st.session_state.pop("edit", None)
        except Exception:
            st.warning("当前 Streamlit 版本可能不支持 modal，已切换为页面内编辑")
            st.write(f"连接: {conn_meta.name}  Unit: {conn_meta.unit}")
            if edit["type"] == "coils":
                new_val = st.checkbox("新值（勾选=1）", value=bool(edit["value"]), key=f"inline_coil_{edit['address']}_{safe}")
            else:
                new_val = st.number_input("新值（整数）", value=int(edit["value"]), step=1, key=f"inline_hold_{edit['address']}_{safe}")
            if st.button("写入（页面内）"):
                try:
                    val_to_write = 1 if (edit["type"] == "coils" and bool(new_val)) else int(new_val)
                    if not conn_meta.connected:
                        ok = conn_meta.connect()
                        if not ok:
                            st.error("与设备连接失败，无法写入")
                            raise ConnectionError("connect failed")
                    if hasattr(conn_meta, "write"):
                        conn_meta.write(type=edit["type"], address=int(edit["address"]), value=val_to_write)
                    st.success("写入成功")
                    st.session_state.pop("edit", None)
                    rerun()
                except Exception as e:
                    st.error(f"写入失败: {e}")

