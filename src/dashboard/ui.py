import streamlit as st
import pandas as pd
import requests
import plotly.express as px
from datetime import datetime

# --- CONFIGURATION ---
API_URL = "http://localhost:8000/api"

st.set_page_config(
    page_title="Sentinel Dashboard",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- SESSION STATE INITIALIZATION ---
if 'rule_states' not in st.session_state:
    st.session_state.rule_states = {}

# --- API HELPER FUNCTIONS ---
def get_pipelines():
    try:
        response = requests.get(f"{API_URL}/pipelines")
        return response.json() if response.status_code == 200 else []
    except:
        return []

def get_alerts(limit=50):
    try:
        response = requests.get(f"{API_URL}/alerts?limit={limit}")
        return response.json() if response.status_code == 200 else []
    except:
        return []

def create_pipeline(data):
    return requests.post(f"{API_URL}/pipelines", json=data)

def add_rule(pipeline_id, data):
    return requests.post(f"{API_URL}/pipelines/{pipeline_id}/rules", json=data)

def toggle_pipeline_status(p_id, new_status):
    try:
        requests.patch(f"{API_URL}/pipelines/{p_id}/status", json={"status": new_status})
        return True
    except:
        return False

def toggle_rule_status(rule_id, is_active):
    try:
        requests.patch(f"{API_URL}/rules/{rule_id}/status", json={"is_active": is_active})
        return True
    except:
        return False

def delete_rule_api(rule_id):
    try:
        requests.delete(f"{API_URL}/rules/{rule_id}")
        return True
    except:
        return False

def delete_pipeline_api(pipeline_id):
    try:
        requests.delete(f"{API_URL}/pipelines/{pipeline_id}")
        return True
    except:
        return False

# --- SIDEBAR NAVIGATION ---
st.sidebar.title("🛡️ Sentinel")
page = st.sidebar.radio("Navigation", ["Dashboard", "Pipeline Manager", "Rule Engine"])

# --- PAGE: DASHBOARD ---
if page == "Dashboard":
    st.title("🚀 Real-Time Monitoring")
    
    # Auto-refresh button
    if st.button("🔄 Refresh Data"):
        st.rerun()

    # Metrics Row
    pipelines = get_pipelines()
    alerts = get_alerts(100)
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Active Pipelines", len([p for p in pipelines if p['status'] == 'active']))
    col2.metric("Total Alerts (Last 100)", len(alerts))
    
    # Calculate Severity Ratio
    critical_count = sum(1 for a in alerts if a['severity'] == 'critical')
    col3.metric("Critical Threats", critical_count, delta_color="inverse")

    # Visualizations
    if alerts:
        df = pd.DataFrame(alerts)
        df['alert_timestamp'] = pd.to_datetime(df['alert_timestamp'])
        
        c1, c2 = st.columns([2, 1])
        
        with c1:
            st.subheader("Alert Velocity")
            # Group by minute/hour for trend line
            df_trend = df.groupby(df['alert_timestamp'].dt.floor('H')).size().reset_index(name='counts')
            fig_trend = px.line(df_trend, x='alert_timestamp', y='counts', title="Alerts over Time")
            st.plotly_chart(fig_trend, use_container_width=True)
            
        with c2:
            st.subheader("Severity Distribution")
            fig_pie = px.pie(df, names='severity', title="Alert Severity", hole=0.4)
            st.plotly_chart(fig_pie, use_container_width=True)

        st.subheader("🚨 Recent Alerts")
        # Flatten the JSONB transaction data for display
        display_df = df[['id', 'severity', 'rule_description', 'alert_timestamp', 'transaction_data']]
        st.dataframe(
            display_df.style.applymap(
                lambda v: 'color: red; font-weight: bold;' if v == 'critical' else '', subset=['severity']
            ),
            use_container_width=True
        )
    else:
        st.info("No alerts found. System is clean or cold.")

# --- PAGE: PIPELINE MANAGER ---
elif page == "Pipeline Manager":
    st.title("🛠️ Pipeline Configuration")
    
    tab1, tab2 = st.tabs(["View Pipelines", "Create New Pipeline"])
    
    with tab1:
        pipelines = get_pipelines()
        if pipelines:
            for pipeline in pipelines:
                with st.container():
                    col1, col2, col3, col4 = st.columns([3, 2, 1, 1])
                    
                    col1.markdown(f"**{pipeline['name']}**")
                    col1.caption(f"Topic: `{pipeline['kafka_topic']}`")
                    
                    # Status badge
                    status = pipeline['status']
                    status_color = "🟢" if status == "active" else "🟡" if status == "paused" else "🔴"
                    col2.markdown(f"{status_color} **{status.upper()}**")
                    col2.caption(f"Created: {pipeline['created_at'][:10]}")
                    
                    # Toggle Pipeline
                    if status == 'active':
                        if col3.button("⏸️ Pause", key=f"pause_p_{pipeline['id']}"):
                            if toggle_pipeline_status(pipeline['id'], "paused"):
                                st.success("Paused!")
                                st.rerun()
                    else:
                        if col3.button("▶️ Resume", key=f"resume_p_{pipeline['id']}"):
                            if toggle_pipeline_status(pipeline['id'], "active"):
                                st.success("Resumed!")
                                st.rerun()
                    
                    # Delete Pipeline
                    if col4.button("🗑️", key=f"del_p_{pipeline['id']}"):
                        if delete_pipeline_api(pipeline['id']):
                            st.success("Deleted!")
                            st.rerun()
                        else:
                            st.error("Failed to delete")
                    
                    st.divider()
        else:
            st.warning("No pipelines found.")

    with tab2:
        st.subheader("Define Dynamic Schema")
        
        with st.form("new_pipeline_form"):
            col1, col2 = st.columns(2)
            p_name = col1.text_input("Pipeline Name", placeholder="e.g. Stripe Payments")
            p_topic = col2.text_input("Kafka Topic", placeholder="e.g. stripe_txns")
            
            st.markdown("### Schema Definition")
            st.caption("Add fields to define your data structure.")
            
            # The Data Editor allows users to add rows like a spreadsheet!
            schema_df = pd.DataFrame(columns=["name", "type", "nullable"])
            edited_df = st.data_editor(
                schema_df,
                num_rows="dynamic",
                column_config={
                    "type": st.column_config.SelectboxColumn(
                        "Data Type",
                        options=["string", "integer", "float", "timestamp", "boolean"],
                        required=True
                    ),
                    "nullable": st.column_config.CheckboxColumn(
                        "Nullable?",
                        default=True
                    )
                }
            )
            
            submit = st.form_submit_button("🚀 Deploy Pipeline")
            
            if submit:
                if p_name and p_topic and not edited_df.empty:
                    # Convert dataframe back to JSON format for API
                    schema_list = edited_df.to_dict('records')
                    payload = {
                        "name": p_name,
                        "kafka_topic": p_topic,
                        "schema": schema_list
                    }
                    
                    res = create_pipeline(payload)
                    if res.status_code == 200:
                        st.success(f"Pipeline created! ID: {res.json()['id']}")
                        st.rerun()
                    else:
                        st.error(f"Error: {res.text}")
                else:
                    st.error("Please fill in all fields and add at least one schema field.")

# --- PAGE: RULE ENGINE ---
elif page == "Rule Engine":
    st.title("⚡ Pipeline Logic & Controls")
    
    pipelines = get_pipelines()
    if pipelines:
        p_map = {p['name']: p for p in pipelines}
        selected_p_name = st.selectbox("Select Pipeline", list(p_map.keys()))
        selected_p = p_map[selected_p_name]
        p_id = selected_p['id']

        # --- SECTION 1: PIPELINE CONTROLS ---
        st.subheader(f"Manage: {selected_p_name}")
        
        col_stat, col_date, col_action = st.columns(3)
        status = selected_p['status']
        status_emoji = "🟢" if status == 'active' else "🟡" if status == 'paused' else "🔴"
        col_stat.metric("Current Status", f"{status_emoji} {status.upper()}")
        col_date.caption(f"Created: {selected_p['created_at']}")
        
        # Pause/Resume Button
        if status == 'active':
            if col_action.button("⏸️ Pause Pipeline", key="pause_pipeline"):
                if toggle_pipeline_status(p_id, "paused"):
                    st.success("Pipeline paused!")
                    st.rerun()
        else:
            if col_action.button("▶️ Resume Pipeline", key="resume_pipeline"):
                if toggle_pipeline_status(p_id, "active"):
                    st.success("Pipeline resumed!")
                    st.rerun()

        st.divider()

        # --- SECTION 2: RULES LIST ---
        st.subheader("Active Rules")
        
        try:
            rules = requests.get(f"{API_URL}/rules/{p_id}").json()
        except:
            rules = []
        
        if rules:
            # Initialize rule states if not present
            for rule in rules:
                rule_id = rule['id']
                if rule_id not in st.session_state.rule_states:
                    st.session_state.rule_states[rule_id] = rule.get('is_active', True)
            
            for rule in rules:
                rule_id = rule['id']
                
                with st.container():
                    c1, c2, c3, c4 = st.columns([4, 2, 1, 1])
                    
                    c1.markdown(f"**{rule['description']}**")
                    c1.code(rule['rule_expression'], language='sql')
                    
                    severity_emoji = "🔴" if rule['severity'] == 'critical' else "🟡" if rule['severity'] == 'warning' else "🔵"
                    c2.markdown(f"{severity_emoji} **{rule['severity'].upper()}**")
                    
                    # Toggle Switch - Use callback to prevent multiple calls
                    def toggle_callback(rule_id=rule_id):
                        new_state = st.session_state[f"toggle_{rule_id}"]
                        if toggle_rule_status(rule_id, new_state):
                            st.session_state.rule_states[rule_id] = new_state
                    
                    c3.toggle(
                        "Enabled",
                        value=st.session_state.rule_states[rule_id],
                        key=f"toggle_{rule_id}",
                        on_change=toggle_callback
                    )
                    
                    # Delete Button
                    if c4.button("🗑️", key=f"del_{rule_id}"):
                        if delete_rule_api(rule_id):
                            if rule_id in st.session_state.rule_states:
                                del st.session_state.rule_states[rule_id]
                            st.success("Rule deleted!")
                            st.rerun()
                        else:
                            st.error("Failed to delete")
                    
                    st.markdown("---")
        else:
            st.info("No rules defined yet.")
        
        # --- SECTION 3: ADD NEW RULE ---
        with st.expander("➕ Add New Rule"):
            # Fetch Schema to help the user write rules
            try:
                schema_res = requests.get(f"{API_URL}/pipelines/{p_id}/schema")
                if schema_res.status_code == 200:
                    schema_fields = [f['name'] for f in schema_res.json()]
                    st.info(f"📋 Available Fields: `{', '.join(schema_fields)}`")
            except:
                pass

            with st.form("rule_form"):
                desc = st.text_input("Rule Description", placeholder="High Value Transaction")
                
                col1, col2 = st.columns([3, 1])
                expr = col1.text_input("SQL Expression", placeholder="amount > 10000 AND currency = 'USD'")
                severity = col2.selectbox("Severity", ["info", "warning", "critical"])
                
                submit_rule = st.form_submit_button("✅ Add Rule")
                
                if submit_rule:
                    if desc and expr:
                        payload = {
                            "rule_expression": expr,
                            "severity": severity,
                            "description": desc
                        }
                        res = add_rule(p_id, payload)
                        if res.status_code == 200:
                            st.success("Rule added successfully! It will be applied to the next micro-batch.")
                            st.rerun()
                        else:
                            st.error(f"Error: {res.text}")
                    else:
                        st.error("Please fill in description and expression")
    else:
        st.warning("No pipelines found. Create one first in the Pipeline Manager.")