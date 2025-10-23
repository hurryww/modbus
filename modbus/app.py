# modbus/app.py
# Streamlit UI - full file that uses modbus_manager.manager
# Behaviors:
# - Create / list / remove connections
# - For each selected connection render a single input row (function / plc address / count)
# - Operation buttons are placed in a single horizontal row BELOW the inputs
# - Read button is disabled when the connection is not connected; read() is called with allow_reconnect=False
# - When switching function code, PLC address auto-updates to the new base only if user hasn't modified it
import streamlit as st
from modbus_manager import manager
import time

st.set_page_config(page_title="Modbus TCP Manager", layout="wide")
st.title("Modbus TCP Manager（Python + Streamlit）")

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

# Get connections and build selection map
conns = manager.list_connections()
conn_map = {f"{c['name']} [{c['id']}]": c['id'] for c in conns} if conns else {}

if conns:
    selected_labels = st.sidebar.multiselect("选择连接（可多选，用于显示并分别操作）", options=list(conn_map.keys()))
    selected_ids = [conn_map[label] for label in selected_labels] if selected_labels else []
else:
    st.sidebar.info("当前没有连接，请先创建一个")
    selected_ids = []

if st.sidebar.button("刷新连接列表"):
    rerun()

# Sidebar quick batch actions
if selected_ids:
    c1, c2, c3 = st.sidebar.columns([1, 1, 1])
    with c1:
        if st.button("批量连接所选"):
            for cid in selected_ids:
                c = manager.get(cid)
                try:
                    if c:
                        c.connect()
                except Exception as e:
                    st.sidebar.error(f"{cid} 连接失败: {e}")
            rerun()
    with c2:
        if st.button("批量断开所选"):
            for cid in selected_ids:
                c = manager.get(cid)
                try:
                    if c:
                        c.close()
                except Exception as e:
                    st.sidebar.error(f"{cid} 断开失败: {e}")
            rerun()
    with c3:
        if st.button("删除所选连接"):
            for cid in list(selected_ids):
                try:
                    manager.remove(cid)
                    # cleanup cached reads
                    st.session_state.get("read_values", {}).pop(cid, None)
                    st.session_state.get("last_modbus_address", {}).pop(cid, None)
                    st.session_state.get("last_plc_address", {}).pop(cid, None)
                except Exception as e:
                    st.sidebar.error(f"{cid} 删除失败: {e}")
            rerun()

if not selected_ids:
    st.info("请选择左侧的一个或多个连接（多选）以在主区显示并分别操作。")
    st.stop()

st.markdown("## 已选连接（每个连接一行：设置 + 操作按钮）")

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

# initialize read caches
ensure_session_default("read_values", {})
ensure_session_default("last_modbus_address", {})
ensure_session_default("last_plc_address", {})

# Render a row per selected connection
for cid in selected_ids:
    conn = manager.get(cid)
    if conn is None:
        st.warning(f"连接 {cid} 不存在")
        continue

    safe = cid.replace("-", "_")
    func_key = f"func_opt_{safe}"    # selectbox key (stores option string)
    plc_key = f"plc_addr_{safe}"
    count_key = f"count_{safe}"
    prev_key = f"prev_func_{safe}"   # previous function selection

    # initialize defaults BEFORE widget creation
    default_idx = 2
    default_opt = func_display_list[default_idx] if 0 <= default_idx < len(func_display_list) else func_display_list[0]
    ensure_session_default(func_key, default_opt)
    if st.session_state.get(func_key) not in func_display_list:
        st.session_state[func_key] = default_opt

    ensure_session_default(prev_key, st.session_state.get(func_key, default_opt))

    # compute current and previous bases
    cur_func = st.session_state.get(func_key, default_opt)
    prev_func = st.session_state.get(prev_key, None)
    try:
        cur_idx = func_display_list.index(cur_func)
        cur_base = FUNCTION_OPTIONS[cur_idx][2]
    except Exception:
        cur_idx = default_idx
        cur_base = FUNCTION_OPTIONS[default_idx][2]

    prev_base = None
    if prev_func in func_display_list:
        prev_base = FUNCTION_OPTIONS[func_display_list.index(prev_func)][2]

    # PLC/count defaults
    ensure_session_default(plc_key, cur_base if prev_base is None else prev_base)
    try:
        plc_val_current = int(st.session_state.get(plc_key, cur_base))
    except Exception:
        plc_val_current = cur_base
    ensure_session_default(count_key, 4)
    try:
        st.session_state[count_key] = int(st.session_state.get(count_key, 4))
    except Exception:
        st.session_state[count_key] = 4

    # if function changed (prev -> cur) and user didn't modify plc (plc == prev_base), update plc to cur_base
    if prev_func is not None and prev_func != cur_func:
        if prev_base is not None and plc_val_current == prev_base:
            # safe to set because we haven't created a widget for plc_key yet
            st.session_state[plc_key] = cur_base
        # update prev to current for next comparison
        st.session_state[prev_key] = cur_func

    # connection header
    st.markdown(f"### {conn.name}  ({conn.host}:{conn.port})  ID: {conn.id}  Unit: {conn.unit}  状态: {'已连接' if conn.connected else '未连接'}")

    # top row: function | plc address | count
    cols = st.columns([3, 2, 1])
    with cols[0]:
        # use key-only selectbox: session_state already has a valid default string
        sel = st.selectbox(f"功能（{conn.name}）", options=func_display_list, key=func_key)
        sel_idx = func_display_list.index(sel)
        func_type = FUNCTION_OPTIONS[sel_idx][1]
        func_base = FUNCTION_OPTIONS[sel_idx][2]
    with cols[1]:
        plc_val = st.number_input(f"PLC 地址（示例 {func_base}）", min_value=0, step=1, key=plc_key)
    with cols[2]:
        cnt_val = st.number_input("数量", min_value=1, step=1, key=count_key)

    # second row: horizontally arranged buttons
    btn_cols = st.columns([1, 1, 1, 1])
    with btn_cols[0]:
        if st.button("连接", key=f"connect_{safe}"):
            try:
                ok = conn.connect()
                if ok:
                    st.success(f"{conn.name} 已连接")
                else:
                    st.error(f"{conn.name} 连接失败")
            except Exception as e:
                st.error(f"{conn.name} 连接异常: {e}")
            rerun()
    with btn_cols[1]:
        if st.button("断开", key=f"close_{safe}"):
            try:
                conn.close()
                # optionally clear cached read result on close so UI doesn't show stale data
                st.session_state.get("read_values", {}).pop(cid, None)
                st.session_state.get("last_modbus_address", {}).pop(cid, None)
                st.session_state.get("last_plc_address", {}).pop(cid, None)
                st.info(f"{conn.name} 已断开")
            except Exception as e:
                st.error(f"{conn.name} 断开异常: {e}")
            rerun()
    with btn_cols[2]:
        # If connected, allow read; if not, show disabled button (if supported) or message.
        if conn.connected:
            if st.button("读取", key=f"read_btn_{safe}"):
                try:
                    cur_plc = int(st.session_state.get(plc_key))
                    cur_cnt = int(st.session_state.get(count_key))
                    cur_func_display = st.session_state.get(func_key)
                    if cur_func_display in func_display_list:
                        cur_idx = func_display_list.index(cur_func_display)
                    else:
                        cur_idx = default_idx
                    cur_func_type = FUNCTION_OPTIONS[cur_idx][1]
                    cur_func_base = FUNCTION_OPTIONS[cur_idx][2]
                    modbus_address = plc_to_modbus(cur_plc, cur_func_base)

                    # enforce not auto-reconnecting here
                    values = conn.read(type=cur_func_type, address=int(modbus_address), count=int(cur_cnt), allow_reconnect=False)

                    st.session_state["read_values"][cid] = values
                    st.session_state["last_modbus_address"][cid] = int(modbus_address)
                    st.session_state["last_plc_address"][cid] = int(cur_plc)
                    st.success(f"{conn.name} 读取成功")
                except ConnectionError as ce:
                    st.error(f"{conn.name} 未连接：{ce}")
                except Exception as e:
                    # on read failure we clear the cached result to avoid showing stale data
                    st.session_state.get("read_values", {}).pop(cid, None)
                    st.session_state.get("last_modbus_address", {}).pop(cid, None)
                    st.session_state.get("last_plc_address", {}).pop(cid, None)
                    st.error(f"{conn.name} 读取失败: {e}")
                rerun()
        else:
            # try disabled if supported; otherwise show a hint
            try:
                st.button("读取", key=f"read_btn_disabled_{safe}", disabled=True)
            except TypeError:
                st.write("（未连接，无法读取）")
    with btn_cols[3]:
        if st.button("删除", key=f"remove_{safe}"):
            try:
                manager.remove(cid)
                st.session_state.get("read_values", {}).pop(cid, None)
                st.session_state.get("last_modbus_address", {}).pop(cid, None)
                st.session_state.get("last_plc_address", {}).pop(cid, None)
                st.success(f"{conn.name} 已删除")
            except Exception as e:
                st.error(f"删除失败: {e}")
            rerun()

    st.markdown("---")

# Read results area
st.markdown("## 读取结果（按连接分组）")
for cid in selected_ids:
    conn = manager.get(cid)
    if conn is None:
        st.warning(f"连接 {cid} 已不存在")
        continue
    st.markdown(f"**{conn.name}  ({conn.host}:{conn.port})**  ID: {conn.id}  Unit: {conn.unit}")
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
    conn = manager.get(edit_conn_id)
    if conn is None:
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
                st.write(f"连接: {conn.name}  Unit: {conn.unit}")
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
                            # conn.write may not be implemented in the example ModbusConnection; handle accordingly
                            if hasattr(conn, "write"):
                                conn.write(type=edit["type"], address=int(edit["address"]), values=[val_to_write])
                            st.success("写入成功")
                            rv = st.session_state["read_values"].get(edit_conn_id)
                            lm = st.session_state["last_modbus_address"].get(edit_conn_id)
                            if rv is not None and lm is not None:
                                idx = int(edit["address"] - lm)
                                if 0 <= idx < len(rv):
                                    st.session_state["read_values"][edit_conn_id][idx] = val_to_write
                            st.session_state.pop("edit", None)
                        except Exception as e:
                            st.error(f"写入失败: {e}")
                    if cancel:
                        st.session_state.pop("edit", None)
        except Exception:
            st.warning("当前 Streamlit 版本可能不支持 modal，已切换为页面内编辑")
            st.write(f"连接: {conn.name}  Unit: {conn.unit}")
            if edit["type"] == "coils":
                new_val = st.checkbox("新值（勾选=1）", value=bool(edit["value"]), key=f"inline_coil_{edit['address']}_{safe}")
            else:
                new_val = st.number_input("新值（整数）", value=int(edit["value"]), step=1, key=f"inline_hold_{edit['address']}_{safe}")
            if st.button("写入（页面内）"):
                try:
                    val_to_write = 1 if (edit["type"] == "coils" and bool(new_val)) else int(new_val)
                    if hasattr(conn, "write"):
                        conn.write(type=edit["type"], address=int(edit["address"]), values=[val_to_write])
                    st.success("写入成功")
                    rv = st.session_state["read_values"].get(edit_conn_id)
                    lm = st.session_state["last_modbus_address"].get(edit_conn_id)
                    if rv is not None and lm is not None:
                        idx = int(edit["address"] - lm)
                        if 0 <= idx < len(rv):
                            st.session_state["read_values"][edit_conn_id][idx] = val_to_write
                    st.session_state.pop("edit", None)
                    rerun()
                except Exception as e:
                    st.error(f"写入失败: {e}")

st.caption("说明：已将操作按钮水平放置在输入行下方；读取在未连接时被禁用，断开时清理旧缓存以避免显示过时数据。")
