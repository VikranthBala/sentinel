import streamlit as st
import pandas as pd
import requests
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import time

# --- CONFIGURATION ---
API_URL = "http://localhost:8000/api"

st.set_page_config(
    page_title="Sentinel Dashboard",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- CUSTOM CSS ---
st.markdown("""
<style>
    /* Main theme colors */
    :root {
        --accent-color: #FF4B4B;
        --success-color: #00C851;
        --warning-color: #FFBB33;
    }
    
    /* Card styling */
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    .critical-card {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    .success-card {
        background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    /* Pipeline/Rule containers */
    .entity-container {
        background-color: #f8f9fa;
        padding: 20px;
        border-radius: 10px;
        border-left: 4px solid #667eea;
        margin-bottom: 15px;
        transition: all 0.3s ease;
    }
    
    .entity-container:hover {
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        transform: translateY(-2px);
    }
    
    /* Status badges */
    .status-badge {
        display: inline-block;
        padding: 5px 15px;
        border-radius: 20px;
        font-weight: bold;
        font-size: 0.85em;
    }
    
    .status-active {
        background-color: #d4edda;
        color: #155724;
    }
    
    .status-paused {
        background-color: #fff3cd;
        color: #856404;
    }
    
    .status-stopped {
        background-color: #f8d7da;
        color: #721c24;
    }
    
    /* Severity badges */
    .severity-critical {
        background-color: #dc3545;
        color: white;
        padding: 4px 12px;
        border-radius: 15px;
        font-size: 0.8em;
        font-weight: bold;
    }
    
    .severity-warning {
        background-color: #ffc107;
        color: #000;
        padding: 4px 12px;
        border-radius: 15px;
        font-size: 0.8em;
        font-weight: bold;
    }
    
    .severity-info {
        background-color: #17a2b8;
        color: white;
        padding: 4px 12px;
        border-radius: 15px;
        font-size: 0.8em;
        font-weight: bold;
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Better spacing */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    
    /* Improved dataframe styling */
    .dataframe {
        border-radius: 8px;
        overflow: hidden;
    }
</style>
""", unsafe_allow_html=True)

# --- SESSION STATE INITIALIZATION ---
if 'rule_states' not in st.session_state:
    st.session_state.rule_states = {}

# --- API HELPER FUNCTIONS ---
def get_pipelines():
    try:
        response = requests.get(f"{API_URL}/pipelines", timeout=5)
        return response.json() if response.status_code == 200 else []
    except:
        return []

def get_alerts(limit=50):
    try:
        response = requests.get(f"{API_URL}/alerts?limit={limit}", timeout=5)
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
st.sidebar.markdown("### Real-Time Fraud Detection")
st.sidebar.divider()

page = st.sidebar.radio(
    "Navigation",
    ["📊 Dashboard", "⚙️ Pipelines", "🎯 Rules"],
    label_visibility="collapsed"
)

st.sidebar.divider()
st.sidebar.caption("💡 Monitor, Configure, Protect")


# --- PAGE: DASHBOARD ---
if page == "📊 Dashboard":
    # Auto-refresh every 30 seconds
    if st.checkbox("🔄 Auto-refresh (30s)"):
        time.sleep(30)
        st.rerun()

    # Header with refresh
    col_title, col_refresh = st.columns([4, 1])
    with col_title:
        st.title("📊 Real-Time Monitoring")
    with col_refresh:
        if st.button("🔄 Refresh", width='stretch'):
            st.rerun()

    st.divider()

    # Metrics Row with custom styling
    pipelines = get_pipelines()
    alerts = get_alerts(100)
    
    active_count = len([p for p in pipelines if p['status'] == 'active'])
    critical_count = sum(1 for a in alerts if a['severity'] == 'critical')
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown(f"""
        <div class="success-card">
            <h3 style="margin:0;">Active Pipelines</h3>
            <h1 style="margin:10px 0;">{active_count}</h1>
            <p style="margin:0; opacity:0.9;">of {len(pipelines)} total</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <h3 style="margin:0;">Total Alerts</h3>
            <h1 style="margin:10px 0;">{len(alerts)}</h1>
            <p style="margin:0; opacity:0.9;">last 100 records</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="critical-card">
            <h3 style="margin:0;">🚨 Critical Threats</h3>
            <h1 style="margin:10px 0;">{critical_count}</h1>
            <p style="margin:0; opacity:0.9;">requires immediate action</p>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Add filters
    col1, col2 = st.columns(2)
    severity_filter = col1.multiselect("Filter Severity", ["critical", "warning", "info"])
    date_filter = col2.date_input("Date Range")

    # Visualizations
    if alerts:
        df = pd.DataFrame(alerts)
        df['alert_timestamp'] = pd.to_datetime(df['alert_timestamp'])
        
        # Charts side by side
        chart_col1, chart_col2 = st.columns([2, 1])
        
        with chart_col1:
            st.subheader("📈 Alert Velocity")
            df_trend = df.groupby(df['alert_timestamp'].dt.floor('H')).size().reset_index(name='counts')
            
            fig_trend = go.Figure()
            fig_trend.add_trace(go.Scatter(
                x=df_trend['alert_timestamp'],
                y=df_trend['counts'],
                mode='lines+markers',
                fill='tozeroy',
                line=dict(color='#667eea', width=3),
                marker=dict(size=8, color='#764ba2'),
                name='Alerts'
            ))
            fig_trend.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                xaxis=dict(showgrid=True, gridcolor='rgba(0,0,0,0.1)'),
                yaxis=dict(showgrid=True, gridcolor='rgba(0,0,0,0.1)'),
                margin=dict(l=0, r=0, t=30, b=0),
                height=350
            )
            st.plotly_chart(fig_trend, width='stretch')
            
        with chart_col2:
            st.subheader("🎯 Severity Breakdown")
            severity_counts = df['severity'].value_counts()
            
            colors_map = {
                'critical': '#f5576c',
                'warning': '#FFBB33',
                'info': '#4facfe'
            }
            colors = [colors_map.get(sev, '#cccccc') for sev in severity_counts.index]
            
            fig_pie = go.Figure(data=[go.Pie(
                labels=severity_counts.index,
                values=severity_counts.values,
                hole=0.5,
                marker=dict(colors=colors),
                textinfo='label+percent',
                textfont=dict(size=14, color='white')
            )])
            fig_pie.update_layout(
                showlegend=False,
                margin=dict(l=0, r=0, t=30, b=0),
                height=350
            )
            st.plotly_chart(fig_pie, width='stretch')

        # Recent Alerts Table
        st.subheader("🚨 Recent Alerts")
        
        # Create a formatted display
        display_df = df[['id', 'severity', 'rule_description', 'alert_timestamp']].copy()
        display_df['alert_timestamp'] = display_df['alert_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Apply styling
        def highlight_severity(row):
            if row['severity'] == 'critical':
                return ['background-color: #ffebee'] * len(row)
            elif row['severity'] == 'warning':
                return ['background-color: #fff9e6'] * len(row)
            else:
                return [''] * len(row)
        
        styled_df = display_df.style.apply(highlight_severity, axis=1)
        st.dataframe(styled_df, width='stretch', height=400)

        # Add export functionality
        if st.button("📥 Export to CSV"):
            df.to_csv("alerts_export.csv")
            st.success("Exported!")
    else:
        st.info("✅ No alerts found. System is clean!")

# --- PAGE: PIPELINE MANAGER ---
elif page == "⚙️ Pipelines":
    st.title("⚙️ Pipeline Management")
    st.caption("Configure and monitor your data processing pipelines")
    st.divider()
    
    tab1, tab2 = st.tabs(["📋 View Pipelines", "➕ Create Pipeline"])
    
    with tab1:
        pipelines = get_pipelines()
        
        if pipelines:
            # Summary stats
            active = sum(1 for p in pipelines if p['status'] == 'active')
            paused = sum(1 for p in pipelines if p['status'] == 'paused')
            
            stat_col1, stat_col2, stat_col3 = st.columns(3)
            stat_col1.metric("Total Pipelines", len(pipelines))
            stat_col2.metric("Active", active, delta=f"{active}/{len(pipelines)}")
            stat_col3.metric("Paused", paused)
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            # Display each pipeline
            for pipeline in pipelines:
                status = pipeline['status']
                status_class = f"status-{status}"
                status_emoji = "🟢" if status == "active" else "🟡" if status == "paused" else "🔴"
                
                st.markdown(f"""
                <div class="entity-container">
                    <h3 style="margin-top:0;">{status_emoji} {pipeline['name']}</h3>
                    <p style="color: #666; margin: 5px 0;">
                        <strong>Topic:</strong> <code>{pipeline['kafka_topic']}</code> | 
                        <strong>Created:</strong> {pipeline['created_at'][:10]} | 
                        <strong>Alerts:</strong> {pipeline.get('alert_count', 0)}
                    </p>
                </div>
                """, unsafe_allow_html=True)
                
                # Action buttons
                btn_col1, btn_col2, btn_col3, btn_col4 = st.columns([1, 1, 1, 3])
                
                with btn_col1:
                    if status == 'active':
                        if st.button("⏸️ Pause", key=f"pause_p_{pipeline['id']}", width='stretch'):
                            if toggle_pipeline_status(pipeline['id'], "paused"):
                                st.success("✅ Paused!")
                                st.rerun()
                    else:
                        if st.button("▶️ Resume", key=f"resume_p_{pipeline['id']}", width='stretch'):
                            if toggle_pipeline_status(pipeline['id'], "active"):
                                st.success("✅ Resumed!")
                                st.rerun()
                
                with btn_col2:
                    if st.button("🗑️ Delete", key=f"del_p_{pipeline['id']}", width='stretch'):
                        if delete_pipeline_api(pipeline['id']):
                            st.success("✅ Deleted!")
                            st.rerun()
                        else:
                            st.error("❌ Failed to delete")
                
                st.markdown("<br>", unsafe_allow_html=True)
        else:
            st.info("📭 No pipelines found. Create your first pipeline below!")

    with tab2:
        st.subheader("🎯 Create New Pipeline")
        
        with st.form("new_pipeline_form"):
            col1, col2 = st.columns(2)
            p_name = col1.text_input("Pipeline Name *", placeholder="e.g., Stripe Payments")
            p_topic = col2.text_input("Kafka Topic *", placeholder="e.g., stripe_txns")
            
            st.markdown("### 📐 Schema Definition")
            st.caption("Define the structure of your data stream")
            
            schema_df = pd.DataFrame(columns=["name", "type", "nullable"])
            edited_df = st.data_editor(
                schema_df,
                num_rows="dynamic",
                column_config={
                    "name": st.column_config.TextColumn("Field Name", required=True),
                    "type": st.column_config.SelectboxColumn(
                        "Data Type",
                        options=["string", "integer", "float", "timestamp", "boolean"],
                        required=True
                    ),
                    "nullable": st.column_config.CheckboxColumn(
                        "Nullable?",
                        default=True
                    )
                },
                width='stretch'
            )
            
            col_btn1, col_btn2, col_btn3 = st.columns([2, 1, 2])
            submit = col_btn2.form_submit_button("🚀 Deploy Pipeline", width='stretch')
            
            if submit:
                if p_name and p_topic and not edited_df.empty:
                    schema_list = edited_df.to_dict('records')
                    payload = {
                        "name": p_name,
                        "kafka_topic": p_topic,
                        "schema": schema_list
                    }
                    
                    res = create_pipeline(payload)
                    if res.status_code == 200:
                        st.success(f"✅ Pipeline created! ID: {res.json()['id']}")
                        st.balloons()
                        st.rerun()
                    else:
                        st.error(f"❌ Error: {res.text}")
                else:
                    st.error("⚠️ Please fill in all required fields and add at least one schema field.")

# --- PAGE: RULE ENGINE ---
elif page == "🎯 Rules":
    st.title("🎯 Rule Engine")
    st.caption("Define detection logic and manage rule lifecycle")
    st.divider()
    
    pipelines = get_pipelines()
    
    if not pipelines:
        st.warning("⚠️ No pipelines found. Create a pipeline first in the Pipeline Manager.")
    else:
        p_map = {p['name']: p for p in pipelines}
        selected_p_name = st.selectbox("Select Pipeline", list(p_map.keys()), key="rule_pipeline_select")
        selected_p = p_map[selected_p_name]
        p_id = selected_p['id']

        st.markdown("<br>", unsafe_allow_html=True)

        # Pipeline Status Overview
        status = selected_p['status']
        status_emoji = "🟢" if status == 'active' else "🟡" if status == 'paused' else "🔴"
        
        overview_col1, overview_col2, overview_col3 = st.columns([2, 2, 1])
        
        with overview_col1:
            st.markdown(f"### {status_emoji} {selected_p_name}")
            st.caption(f"Topic: `{selected_p['kafka_topic']}`")
        
        with overview_col2:
            st.metric("Status", status.upper())
            st.caption(f"Created: {selected_p['created_at'][:10]}")
        
        with overview_col3:
            if status == 'active':
                if st.button("⏸️ Pause", key="pause_pipeline_rules", width='stretch'):
                    if toggle_pipeline_status(p_id, "paused"):
                        st.success("✅ Paused!")
                        st.rerun()
            else:
                if st.button("▶️ Resume", key="resume_pipeline_rules", width='stretch'):
                    if toggle_pipeline_status(p_id, "active"):
                        st.success("✅ Resumed!")
                        st.rerun()

        st.divider()

        # Rules Section
        st.subheader("📜 Active Rules")
        
        try:
            rules = requests.get(f"{API_URL}/rules/{p_id}", timeout=5).json()
        except:
            rules = []
        
        if rules:
            # Rule statistics
            active_rules = sum(1 for r in rules if r.get('is_active', True))
            rule_stat_col1, rule_stat_col2, rule_stat_col3 = st.columns(3)
            rule_stat_col1.metric("Total Rules", len(rules))
            rule_stat_col2.metric("Active", active_rules)
            rule_stat_col3.metric("Inactive", len(rules) - active_rules)
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            # Initialize rule states
            for rule in rules:
                rule_id = rule['id']
                if rule_id not in st.session_state.rule_states:
                    st.session_state.rule_states[rule_id] = rule.get('is_active', True)
            
            # Display each rule
            for rule in rules:
                rule_id = rule['id']
                severity = rule['severity']
                
                severity_emoji = "🔴" if severity == 'critical' else "🟡" if severity == 'warning' else "🔵"
                severity_class = f"severity-{severity}"
                
                is_active = st.session_state.rule_states[rule_id]
                active_indicator = "✅" if is_active else "⏸️"
                
                st.markdown(f"""
                <div class="entity-container">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <h4 style="margin:0;">{active_indicator} {rule['description']}</h4>
                        <span class="{severity_class}">{severity_emoji} {severity.upper()}</span>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                # Rule expression
                st.code(rule['rule_expression'], language='sql')
                
                # Action buttons
                rule_col1, rule_col2, rule_col3, rule_col4 = st.columns([1, 1, 1, 3])
                
                # Toggle callback
                def toggle_callback(rule_id=rule_id):
                    new_state = st.session_state[f"toggle_{rule_id}"]
                    if toggle_rule_status(rule_id, new_state):
                        st.session_state.rule_states[rule_id] = new_state
                
                with rule_col1:
                    st.toggle(
                        "Active",
                        value=st.session_state.rule_states[rule_id],
                        key=f"toggle_{rule_id}",
                        on_change=toggle_callback
                    )
                
                with rule_col2:
                    if st.button("🗑️ Delete", key=f"del_{rule_id}", width='stretch'):
                        if delete_rule_api(rule_id):
                            if rule_id in st.session_state.rule_states:
                                del st.session_state.rule_states[rule_id]
                            st.success("✅ Rule deleted!")
                            st.rerun()
                        else:
                            st.error("❌ Failed to delete")
                
                st.markdown("<br>", unsafe_allow_html=True)
        else:
            st.info("📭 No rules defined yet. Add your first rule below!")

        # Add New Rule Section
        with st.expander("➕ Add New Rule", expanded=not bool(rules)):
            # Fetch Schema
            try:
                schema_res = requests.get(f"{API_URL}/pipelines/{p_id}/schema", timeout=5)
                if schema_res.status_code == 200:
                    schema_fields = [f['name'] for f in schema_res.json()]
                    st.info(f"📋 **Available Fields:** `{', '.join(schema_fields)}`")
            except:
                pass

            with st.form("rule_form"):
                st.markdown("### Define Detection Logic")
                
                desc = st.text_input("Rule Description *", placeholder="e.g., High Value Transaction Alert")
                
                col1, col2 = st.columns([3, 1])
                expr = col1.text_input(
                    "SQL Expression *",
                    placeholder="e.g., amount > 10000 AND currency = 'USD'",
                    help="Use field names from the schema above"
                )
                severity = col2.selectbox("Severity", ["info", "warning", "critical"])
                
                st.markdown("**Example expressions:**")
                st.code("amount > 5000", language="sql")
                st.code("transaction_count > 10 AND time_window < 60", language="sql")
                st.code("location != 'US' AND amount > 1000", language="sql")
                
                col_btn1, col_btn2, col_btn3 = st.columns([2, 1, 2])
                submit_rule = col_btn2.form_submit_button("✅ Add Rule", width='stretch')
                
                if submit_rule:
                    if desc and expr:
                        payload = {
                            "rule_expression": expr,
                            "severity": severity,
                            "description": desc
                        }
                        res = add_rule(p_id, payload)
                        if res.status_code == 200:
                            st.success("✅ Rule added successfully! It will be applied to the next micro-batch.")
                            st.rerun()
                        else:
                            st.error(f"❌ Error: {res.text}")
                    else:
                        st.error("⚠️ Please fill in both description and expression")