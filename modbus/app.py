# app.py - Streamlit UI
import streamlit as st
from modbus_manager import manager
import time
from typing import List

st.set_page_config(page_title="Modbus TCP Manager", layout="wide")

st.title("Modbus TCP Manager（Python + Streamlit）")

# 兼容性 rerun 函数：优先使用 st.experimental_rerun（若可用），否则通过修改 query params 或 session_state 触发重载
def rerun():
    try:
        if hasattr(st, "experimental_rerun"):
            # 有些 streamlit 版本中该函数存在但可能在当前上下文不可用，捕获异常
            try:
                st.experimental_rerun()
                return
            except Exception:
                pass
        # 使用新的 API st.query_params 代替已废弃的 experimental_get_query_params
        try:
            params = dict(st.query_params)  # st.query_params 返回类似 {k: [v]} 的映射
            params["_rerun"] = [str(time.time())]
            st.experimental_set_query_params(**params)
            return
        except Exception:
            pass
        # 最后兜底：设置 session_state 值
        try:
            st.session_state["_rerun_flag"] = time.time()
        except Exception:
            # 如果所有手段都失败，静默不报错（避免二次异常遮蔽原错误）
            pass
    except Exception:
        pass

# Sidebar: create connection
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

# list connections and select
conns = manager.list_connections()
if conns:
    conn_map = {f"{c['name']} [{c['id']}]": c['id'] for c in conns}
    selected_label = st.sidebar.selectbox("选择连接", options=list(conn_map.keys()))
    selected_id = conn_map[selected_label]
else:
    st.sidebar.info("当前没有连接，先在上方创建一个")
    selected_id = None

# quick actions: refresh list, delete
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

# Main area: show selected connection details
if not selected_id:
    st.info("请选择一个连接以查看和操作（左侧）")
    st.stop()

conn = manager.get(selected_id)
if conn is None:
    st.error("所选连接不存在")
    st.stop()

st.subheader(f"连接: {conn.name}  ({conn.host}:{conn.port})")
st.write(f"ID: {conn.id}  Unit: {conn.unit}  状态: {'已连接' if conn.connected else '未连接'}")

# connection control
col1, col2, col3 = st.columns(3)
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
with col3:
    if st.button("清空轮询历史"):
        conn.poll_history.clear()
        st.info("已清空历史")

st.markdown("---")

# Tabs: Read / Write / Poll / History
tab1, tab2, tab3, tab4 = st.tabs(["读取寄存器", "写入寄存器", "轮询配置", "轮询历史 / 日志"])

with tab1:
    st.write("读取寄存器")
    with st.form("read_form"):
        r_type = st.selectbox(
            "类型",
            options=["holding", "input", "coils", "discrete"],
            index=0,
            format_func=lambda x: {
                "holding": "保持寄存器 (holding)",
                "input": "输入寄存器 (input)",
                "coils": "线圈 (coils)",
                "discrete": "离散输入 (discrete)"
            }[x]
        )
        address = st.number_input("起始地址", min_value=0, value=0)
        count = st.number_input("数量", min_value=1, value=4)
        do_read = st.form_submit_button("读取")
        if do_read:
            try:
                vals = conn.read(type=r_type, address=int(address), count=int(count))
                # display nicely
                rows = [{"地址": int(address) + i, "值": v} for i, v in enumerate(vals)]
                st.table(rows)
            except Exception as e:
                st.error(f"读取失败: {e}")

with tab2:
    st.write("写入寄存器")
    with st.form("write_form"):
        w_type = st.selectbox(
            "写入类型",
            options=["holding", "coils"],
            index=0,
            format_func=lambda x: {"holding": "保持寄存器 (holding)", "coils": "线圈 (coils)"}[x]
        )
        w_address = st.number_input("起始地址", min_value=0, value=0, key="waddr")
        w_values_str = st.text_input("写入值（逗号分隔，例如: 10,20 或 1,0,1）", value="0")
        do_write = st.form_submit_button("写入")
        if do_write:
            try:
                # parse csv
                parts = [p.strip() for p in w_values_str.split(",") if p.strip() != ""]
                vals = [int(p, 0) for p in parts]  # allow hex like 0x10
                conn.write(type=w_type, address=int(w_address), values=vals)
                st.success("写入成功")
            except Exception as e:
                st.error(f"写入失败: {e}")

with tab3:
    st.write("轮询配置（后台线程）")
    with st.form("poll_form"):
        p_type = st.selectbox(
            "类型",
            options=["holding", "input", "coils", "discrete"],
            index=0,
            format_func=lambda x: {
                "holding": "保持寄存器 (holding)",
                "input": "输入寄存器 (input)",
                "coils": "线圈 (coils)",
                "discrete": "离散输入 (discrete)"
            }[x]
        )
        p_address = st.number_input("起始地址", min_value=0, value=0, key="paddr")
        p_count = st.number_input("数量", min_value=1, value=4, key="pcount")
        p_interval = st.number_input("间隔 (秒)", min_value=0.1, value=2.0, step=0.1, key="pinterval")
        p_max_history = st.number_input("单连接历史最大条数", min_value=1, value=200, key="phist")
        start_poll = st.form_submit_button("开始轮询")
        stop_poll = st.form_submit_button("停止轮询")
        if start_poll:
            try:
                conn.start_poll(type=p_type, address=int(p_address), count=int(p_count), interval=float(p_interval), max_history=int(p_max_history))
                st.success("已开始轮询（后台）")
            except Exception as e:
                st.error(f"启动轮询失败: {e}")
        if stop_poll:
            conn.stop_poll()
            st.info("已停止轮询")

with tab4:
    st.write("轮询历史/日志（最新在下方）")
    st.write(f"历史条数：{len(conn.poll_history)}")
    if conn.poll_history:
        # show latest N
        latest_n = st.number_input(
            "查看最近多少条",
            min_value=1,
            max_value=max(1, len(conn.poll_history)),
            value=min(20, len(conn.poll_history)),
            key="hview"
        )
        entries = conn.poll_history[-latest_n:]
        # show in table
        rows = []
        for e in reversed(entries):
            ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(e["timestamp"]))
            if e.get("error"):
                rows.append({"time": ts, "type": e["type"], "addr": e["address"], "count": e["count"], "values": e["error"]})
            else:
                rows.append({"time": ts, "type": e["type"], "addr": e["address"], "count": e["count"], "values": ", ".join(str(v) for v in e["values"])})
        st.table(rows)
    else:
        st.info("还没有轮询历史。可以在 轮询配置 页签中启动轮询并等待数据。")

st.markdown("---")
st.write("注：若希望前端自动刷新轮询历史，可使用页面顶部的 '刷新连接列表' 或者定期点击刷新。")