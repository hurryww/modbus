# app.py - Streamlit UI (改进：读取结果持久化到 session_state，编辑弹窗可用)
import streamlit as st
from modbus_manager import manager
import time

st.set_page_config(page_title="Modbus TCP Manager", layout="wide")

st.title("Modbus TCP Manager（Python + Streamlit）")

def rerun():
    try:
        if hasattr(st, "experimental_rerun"):
            try:
                st.experimental_rerun()
                return
            except Exception:
                pass
        try:
            params = dict(st.query_params)
            params["_rerun"] = [str(time.time())]
            st.experimental_set_query_params(**params)
            return
        except Exception:
            pass
        try:
            st.session_state["_rerun_flag"] = time.time()
        except Exception:
            pass
    except Exception:
        pass

# Sidebar: create connection (未改动)
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
                st.success(f"创建连接: {conn.id} ({conn.name}) 连接状态: {'已连接' if conn.connected else '未连接'}")
                rerun()
            except Exception as e:
                st.error(f"创建失败: {e}")

# 选择连接等（未改动）
conns = manager.list_connections()
if conns:
    conn_map = {f"{c['name']} [{c['id']}]": c['id'] for c in conns}
    selected_label = st.sidebar.selectbox("选择连接", options=list(conn_map.keys()))
    selected_id = conn_map[selected_label]
else:
    st.sidebar.info("当前没有连接，先在上方创建一个")
    selected_id = None

if st.sidebar.button("刷新连接列表"):
    rerun()

if selected_id:
    if st.sidebar.button("删除选中连接（停止轮询并移除）"):
        try:
            manager.remove(selected_id)
            st.sidebar.success("已删除")
        except Exception as e:
            st.sidebar.error(f"删除失败: {e}")
        rerun()

if not selected_id:
    st.info("请选择一个连接以查看和操作（左侧）")
    st.stop()

conn = manager.get(selected_id)
if conn is None:
    st.error("所选连接不存在")
    st.stop()

st.subheader(f"连接: {conn.name}  ({conn.host}:{conn.port})")
st.write(f"ID: {conn.id}  Unit: {conn.unit}  状态: {'已连接' if conn.connected else '未连接'}")

col1, col2 = st.columns(2)
with col1:
    if st.button("（重新）连接"):
        try:
            ok = conn.connect()
            if ok:
                st.success("连接成功")
            else:
                st.error("连接失败")
        except Exception as e:
            st.error(f"连接异常: {e}")
with col2:
    if st.button("关闭连接"):
        try:
            conn.close()
            st.info("已关闭连接")
        except Exception as e:
            st.error(f"关闭异常: {e}")

st.markdown("---")

FUNCTION_OPTIONS = [
    ("01 Coil Status (0x) - Coil", "coils", 1),
    ("02 Input Status (1x) - Discrete Input", "discrete", 10001),
    ("03 Holding Register (4x) - Holding", "holding", 40001),
    ("04 Input Registers (3x) - Input Reg", "input", 30001),
]

def plc_to_modbus(plc_addr: int, base: int) -> int:
    try:
        plc_addr = int(plc_addr)
    except Exception:
        raise ValueError("PLC 地址必须是整数")
    if plc_addr >= base:
        return plc_addr - base
    return plc_addr

st.header("读取寄存器（按 PLC 地址输入，例如 40001）")

func_display_list = [opt[0] for opt in FUNCTION_OPTIONS]
selected_func_display = st.selectbox("功能（Function Code / 寄存器类型）", options=func_display_list, index=2, key="func_select")
selected_idx = func_display_list.index(selected_func_display)
func_type = FUNCTION_OPTIONS[selected_idx][1]
func_base = FUNCTION_OPTIONS[selected_idx][2]

# 在 session_state 中存默认 plc_address（避免 widget value/session_state 冲突）
if "plc_address" not in st.session_state or st.session_state.get("last_func_idx") != selected_idx:
    st.session_state["plc_address"] = func_base
    st.session_state["last_func_idx"] = selected_idx

# 读取表单：当读取成功时把结果保存在 session_state 中以便跨 rerun 保留
with st.form("read_form"):
    plc_address = st.number_input(f"PLC 地址 (示例 {func_base})", min_value=0, step=1, key="plc_address")
    count = st.number_input("数量", min_value=1, value=4, step=1)
    do_read = st.form_submit_button("读取")
    try:
        modbus_address = plc_to_modbus(plc_address, func_base)
        st.caption(f"已根据功能码计算 Modbus 起始地址：{modbus_address} （PLC 输入：{plc_address}，基址 {func_base}）")
    except ValueError as e:
        st.error(str(e))
        modbus_address = None

    if do_read:
        if modbus_address is None:
            st.error("无效的 PLC 地址")
        else:
            try:
                read_values = conn.read(type=func_type, address=int(modbus_address), count=int(count))
                # 持久化读取结果到 session_state（关键）
                st.session_state["read_values"] = read_values
                st.session_state["last_modbus_address"] = int(modbus_address)
                st.session_state["last_plc_address"] = int(plc_address)
                # 清除可能的 edit 状态，避免误打开旧编辑
                st.session_state.pop("edit", None)
                # 立即刷新以显示读取结果（可选）
                rerun()
            except Exception as e:
                st.error(f"读取失败: {e}")

# 从 session_state 读取持久化的数据（即使脚本 rerun 也能取到）
read_values = st.session_state.get("read_values")
last_modbus_address = st.session_state.get("last_modbus_address")
last_plc_address = st.session_state.get("last_plc_address")

# 显示读取结果并为每行提供编辑按钮
if read_values:
    st.markdown("---")
    st.subheader("读取结果（点击编辑按钮在弹窗中修改并写入）")
    cols = st.columns([2, 2, 3, 1])
    cols[0].markdown("**PLC 地址**")
    cols[1].markdown("**Modbus 地址**")
    cols[2].markdown("**值**")
    cols[3].markdown("**操作**")

    for i, cur in enumerate(read_values):
        addr_modbus = last_modbus_address + i
        addr_plc = last_plc_address + i
        c0, c1, c2, c3 = st.columns([2, 2, 3, 1])
        c0.write(addr_plc)
        c1.write(addr_modbus)
        c2.write(cur)
        edit_key = f"edit_btn_{selected_id}_{addr_modbus}"
        # 点击按钮时仅写 session_state["edit"]，不要立即清除 read_values
        if c3.button("编辑", key=edit_key):
            st.session_state["edit"] = {
                "type": func_type,
                "plc_addr": addr_plc,
                "address": addr_modbus,
                "value": cur,
                "conn_id": selected_id,
            }
            # 不要立刻强制 rerun（可选），因为当前 run 已会检测 edit 并显示 modal
            # 如果你需要立即刷新也可以调用 rerun()，但 session_state 已保存读取结果，重跑不会丢失上下文.
            # rerun()

# 弹窗编辑（依赖 session_state["edit"]）
if "edit" in st.session_state:
    edit = st.session_state["edit"]
    # 确保连接未变
    if edit.get("conn_id") != selected_id:
        st.warning("编辑项对应的连接已发生变化，请重新读取并重试。")
        st.session_state.pop("edit", None)
    else:
        # 使用 st.modal（如果 Streamlit 版本不支持，可换成页面内显示）
        try:
            with st.modal(f"编辑寄存器 PLC {edit['plc_addr']} (Modbus {edit['address']})"):
                st.write(f"连接: {conn.name}  Unit: {conn.unit}")
                st.write(f"寄存器类型: {edit['type']}")
                st.write(f"当前值: {edit['value']}")
                with st.form("edit_form"):
                    if edit["type"] == "coils":
                        new_val = st.checkbox("新值（勾选=1）", value=bool(edit["value"]), key=f"modal_coil_{edit['address']}")
                    else:
                        new_val = st.number_input("新值（整数）", value=int(edit["value"]), step=1, key=f"modal_hold_{edit['address']}")
                    submitted = st.form_submit_button("写入")
                    cancel = st.form_submit_button("取消写入")
                    if submitted:
                        try:
                            val_to_write = 1 if (edit["type"] == "coils" and bool(new_val)) else int(new_val)
                            conn.write(type=edit["type"], address=int(edit["address"]), values=[val_to_write])
                            st.success("写入成功")
                            # 更新 session_state 中的 read_values，以便 UI 立刻反映新值（避免必须重新读取）
                            idx = int(edit["address"] - st.session_state["last_modbus_address"])
                            if 0 <= idx < len(st.session_state["read_values"]):
                                st.session_state["read_values"][idx] = val_to_write
                            # 关闭 modal
                            st.session_state.pop("edit", None)
                            # 可选择重新读取全部范围以确保一致：下面注释掉，按需启用
                            # time.sleep(0.05); st.session_state.pop("read_values", None); rerun()
                        except Exception as e:
                            st.error(f"写入失败: {e}")
                    if cancel:
                        st.session_state.pop("edit", None)
        except Exception:
            # 如果 st.modal 不存在或异常，退回到页面内编辑（兼容处理）
            st.warning("当前 Streamlit 版本可能不支持 modal，已切换为页面内编辑。")
            # 页面内编辑实现（简化）
            st.write("页面内编辑：")
            if edit["type"] == "coils":
                new_val = st.checkbox("新值（勾选=1）", value=bool(edit["value"]), key=f"inline_coil_{edit['address']}")
            else:
                new_val = st.number_input("新值（整数）", value=int(edit["value"]), step=1, key=f"inline_hold_{edit['address']}")
            if st.button("写入（页面内）"):
                try:
                    val_to_write = 1 if (edit["type"] == "coils" and bool(new_val)) else int(new_val)
                    conn.write(type=edit["type"], address=int(edit["address"]), values=[val_to_write])
                    st.success("写入成功")
                    idx = int(edit["address"] - st.session_state["last_modbus_address"])
                    if 0 <= idx < len(st.session_state["read_values"]):
                        st.session_state["read_values"][idx] = val_to_write
                    st.session_state.pop("edit", None)
                except Exception as e:
                    st.error(f"写入失败: {e}")

st.markdown("---")
st.caption("点击某行的“编辑”按钮会在弹窗中修改并提交写操作；读取结果已在 session_state 中持久化，保证重跑后仍能编辑。")