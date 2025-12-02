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
    col1.metric("Active Pipelines", len(pipelines))
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
            df = pd.DataFrame(pipelines)
            st.dataframe(df, use_container_width=True)
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
                    else:
                        st.error(f"Error: {res.text}")
                else:
                    st.error("Please fill in all fields and add at least one schema field.")

# --- PAGE: RULE ENGINE ---
elif page == "Rule Engine":
    st.title("⚡ Dynamic Rule Engine")
    
    pipelines = get_pipelines()
    if not pipelines:
        st.error("Create a pipeline first.")
    else:
        # Create a mapping of Name -> ID
        p_map = {p['name']: p['id'] for p in pipelines}
        selected_p_name = st.selectbox("Select Target Pipeline", list(p_map.keys()))
        selected_p_id = p_map[selected_p_name]
        
        # Fetch Schema to help the user write rules
        try:
            schema_res = requests.get(f"{API_URL}/pipelines/{selected_p_id}/schema")
            if schema_res.status_code == 200:
                schema_fields = [f['name'] for f in schema_res.json()]
                st.info(f"Available Fields for Logic: {', '.join(schema_fields)}")
        except:
            pass

        st.subheader("Add New Detection Rule")
        with st.form("rule_form"):
            desc = st.text_input("Rule Description", placeholder="High Value Transaction")
            
            col1, col2 = st.columns([3, 1])
            expr = col1.text_input("SQL Expression", placeholder="amount > 10000 AND currency = 'USD'")
            severity = col2.selectbox("Severity", ["info", "warning", "critical"])
            
            submit_rule = st.form_submit_button("Add Rule")
            
            if submit_rule:
                payload = {
                    "rule_expression": expr,
                    "severity": severity,
                    "description": desc
                }
                res = add_rule(selected_p_id, payload)
                if res.status_code == 200:
                    st.success("Rule added successfully! It will be applied to the next micro-batch.")
                else:
                    st.error(res.text)