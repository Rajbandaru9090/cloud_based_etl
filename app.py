import streamlit as st
import pandas as pd
import psycopg2
import boto3
import json
from sqlalchemy import create_engine

# ----------------------------
# 🔐 Fetch credentials from AWS Secrets Manager
# ----------------------------
def get_db_credentials(secret_name, region):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region)
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response['SecretString'])
    return secret

# 🔐 Your secret config
SECRET_NAME = "rds!db-ef6dbf42-82c0-4ad0-b878-4fe309f88270"
REGION = "eu-north-1"
creds = get_db_credentials(SECRET_NAME, REGION)

# ✅ Set DB connection info
host = "myproject1.clc8eu6ssz1t.eu-north-1.rds.amazonaws.com"
port = 5432
dbname = "postgres"
username = creds["username"]
password = creds["password"]

# 🌐 Create DB connection
conn_str = f"postgresql://{username}:{password}@{host}:{port}/{dbname}"
engine = create_engine(conn_str)

# ----------------------------
# 🌟 Streamlit App Starts
# ----------------------------
st.set_page_config(page_title="Sales Dashboard", layout="wide")
st.title("📊 Sales Dashboard from AWS RDS")

# ----------------------------
# 📥 Load Data
# ----------------------------
@st.cache_data
def load_data():
    df = pd.read_sql("SELECT * FROM sales_data", engine)

    st.write("🧾 Columns in data:", df.columns.tolist())  # Debugging helper

    # Safely create revenue column
    try:
        if 'revenue' not in df.columns:
            if 'price' in df.columns and 'quantity' in df.columns:
                df['revenue'] = df['price'] * df['quantity']
            elif 'unit_price' in df.columns and 'quantity_sold' in df.columns:
                df['revenue'] = df['unit_price'] * df['quantity_sold']
            else:
                st.warning("⚠️ Revenue not calculated. No valid columns found.")
                df['revenue'] = 0.0  # fallback column to prevent crash
    except Exception as e:
        st.error(f"❌ Error calculating revenue: {e}")
        df['revenue'] = 0.0

    return df

df = load_data()

# ----------------------------
# 🎛️ Filters
# ----------------------------
with st.sidebar:
    st.header("🔎 Filters")
    if 'product' in df.columns:
        products = df['product'].unique()
        selected_products = st.multiselect("Select Product(s)", products, default=products)
        df = df[df['product'].isin(selected_products)]

# ----------------------------
# 📊 Key Metrics
# ----------------------------
st.subheader("📌 Key Metrics")

try:
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Revenue", f"${df['revenue'].sum():,.2f}")
    col2.metric("Total Orders", len(df))
    col3.metric("Avg Revenue / Order", f"${df['revenue'].mean():,.2f}")
except Exception as e:
    st.error(f"❌ Error displaying metrics: {e}")

# ----------------------------
# 📈 Charts
# ----------------------------
if 'product' in df.columns and 'revenue' in df.columns:
    try:
        st.subheader("📊 Revenue by Product")
        revenue_by_product = df.groupby("product")["revenue"].sum()
        st.bar_chart(revenue_by_product)
    except Exception as e:
        st.error(f"❌ Error plotting chart: {e}")

# ----------------------------
# 🧾 Raw Data Preview
# ----------------------------
st.subheader("📄 Sample Data")
st.dataframe(df.head(100))
