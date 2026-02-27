import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

# --------------------------
# 1. CONFIG & AUTHENTICATION
# --------------------------
PROJECT_ID = "multi-source-data-hub2738"
DATASET_ID = "retail_warehouse"
KEY_PATH = "C:/Users/sanga/OneDrive/Desktop/Retail_Data_Integration/config/multi-source-key.json"

st.set_page_config(page_title="Retail Executive Hub", layout="wide", initial_sidebar_state="expanded")

# --------------------------
# 2. ENHANCED POWER BI CSS
# --------------------------
st.markdown("""
    <style>
        .main { overflow: hidden; max-height: 100vh; background-color: #0e1117; }
        .block-container { padding: 1rem 2rem 0rem 2rem; }
        button[kind="headerNoPadding"] {
            background-color: #3795BD !important;
            color: white !important;
            border-radius: 0 50% 50% 0 !important;
            left: 0px !important; top: 10px !important;
            height: 40px !important; width: 40px !important;
            z-index: 1000001 !important; opacity: 1 !important;
        }
        div[data-testid="stMetric"] {
            background-color: #1e2130; border: 1px solid #3d446e;
            padding: 15px; border-radius: 12px;
        }
        div[data-testid="stMetricLabel"] > div { color: #8e94ab !important; }
        div[data-testid="stMetricValue"] > div { color: #ffffff !important; }
        section[data-testid="stSidebar"] { background-color: #161b22; border-right: 1px solid #3d446e; }
    </style>
    """, unsafe_allow_html=True)

@st.cache_resource
def get_bq_client():
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)

client = get_bq_client()

@st.cache_data(ttl=600)
def get_dashboard_data():
    query = f"""
    SELECT f.sales, f.profit, d.order_date, d.year, c.segment, p.category
    FROM `{PROJECT_ID}.{DATASET_ID}.fact_sales` f
    JOIN `{PROJECT_ID}.{DATASET_ID}.dim_date` d ON f.order_date = d.date_key
    JOIN `{PROJECT_ID}.{DATASET_ID}.dim_customer` c ON f.customer_key = c.customer_key
    JOIN `{PROJECT_ID}.{DATASET_ID}.dim_product` p ON f.product_key = p.product_key
    WHERE c.is_current = 1 AND p.is_current = 1
    """
    return client.query(query).to_dataframe()

try:
    df = get_dashboard_data()
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
    df['category'] = df['category'].fillna('Jewellery')
    df['segment'] = df['segment'].fillna('Consumer')
except Exception as e:
    st.error(f"Error: {e}"); st.stop()

# --------------------------
# 3. SIDEBAR WITH DROPDOWN FILTERS
# --------------------------
with st.sidebar:
    st.markdown("### 🔍 Global Filters")
    cat_options = sorted(df['category'].unique())
    sel_cat = st.selectbox("Category", options=["All"] + cat_options)
    
    seg_options = sorted(df['segment'].unique())
    sel_seg = st.selectbox("Segment", options=["All"] + seg_options)
    
    years = sorted(df['year'].unique(), reverse=True)
    sel_year = st.selectbox("Year", options=["All"] + [str(y) for y in years])

# Apply Filter Logic
f_df = df.copy()
if sel_cat != "All": f_df = f_df[f_df['category'] == sel_cat]
if sel_seg != "All": f_df = f_df[f_df['segment'] == sel_seg]
if sel_year != "All": f_df = f_df[f_df['year'] == int(sel_year)]

# --------------------------
# 4. MAIN LAYOUT (2x2 Grid)
# --------------------------
st.markdown("<h1 style='color: white; margin-bottom: 20px;'>🚀 Retail Executive Performance Hub</h1>", unsafe_allow_html=True)

# KPI Row
k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Sales", f"${f_df['sales'].sum():,.0f}")
k2.metric("Total Profit", f"${f_df['profit'].sum():,.0f}")
k3.metric("Transactions", f"{len(f_df):,}")
k4.metric("Avg Order", f"${f_df['sales'].mean():,.2f}" if not f_df.empty else "$0")

st.write("") 

# ROW 1
r1_col1, r1_col2 = st.columns(2)

with r1_col1:
    fig1 = px.line(f_df.groupby('order_date')['sales'].sum().reset_index(), 
                  x='order_date', y='sales', title="Sales Trend", template="plotly_dark")
    fig1.update_layout(height=300, margin=dict(l=0, r=0, t=40, b=0))
    st.plotly_chart(fig1, use_container_width=True)

with r1_col2:
    if sel_cat != "All":
        total_global_sales = df['sales'].sum()
        cat_sales = f_df['sales'].sum()
        fig2 = go.Figure(go.Pie(values=[cat_sales, total_global_sales - cat_sales], 
                                labels=[sel_cat, "Other Categories"], hole=0.5))
        fig2.update_layout(title=f"{sel_cat} vs Market Share")
    else:
        fig2 = px.pie(f_df, values='sales', names='category', hole=0.5, title="Category Share")
    
    fig2.update_layout(height=300, template="plotly_dark", showlegend=True, margin=dict(l=0, r=0, t=40, b=0))
    st.plotly_chart(fig2, use_container_width=True)

# ROW 2
r2_col1, r2_col2 = st.columns(2)

with r2_col1:
    if sel_cat != "All":
        others_profit = df[df['category'] != sel_cat]['profit'].sum()
        selected_profit = f_df['profit'].sum()
        comp_df = pd.DataFrame({'Label': [sel_cat, 'Rest of Business'], 'Profit': [selected_profit, others_profit]})
        fig3 = px.bar(comp_df, x='Label', y='Profit', title=f"Profit: {sel_cat} vs Rest", 
                      template="plotly_dark", color='Label', color_discrete_map={sel_cat: '#3795BD', 'Rest of Business': '#4a4a4a'})
    else:
        fig3 = px.bar(f_df.groupby('category')['profit'].sum().reset_index(), 
                     x='category', y='profit', title="Profit by Category", template="plotly_dark")
    
    fig3.update_layout(height=300, showlegend=False, margin=dict(l=0, r=0, t=40, b=0))
    st.plotly_chart(fig3, use_container_width=True)

with r2_col2:
    if sel_seg != "All":
        others_sales = df[df['segment'] != sel_seg]['sales'].sum()
        selected_sales = f_df['sales'].sum()
        comp_seg_df = pd.DataFrame({'Label': [sel_seg, 'Rest of Market'], 'Sales': [selected_sales, others_sales]})
        fig5 = px.bar(comp_seg_df, y='Label', x='Sales', orientation='h', title=f"Sales: {sel_seg} vs Rest", 
                      template="plotly_dark", color='Label', color_discrete_map={sel_seg: '#3795BD', 'Rest of Market': '#4a4a4a'})
    else:
        fig5 = px.bar(f_df.groupby('segment')['sales'].sum().reset_index(), 
                     y='segment', x='sales', orientation='h', title="Sales by Segment", template="plotly_dark")
    
    fig5.update_layout(height=300, showlegend=False, margin=dict(l=0, r=0, t=40, b=0))
    st.plotly_chart(fig5, use_container_width=True)