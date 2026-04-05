import os
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

st.set_page_config(page_title="Online Retail Dashboard", layout="wide")
st.title("Online Real-Time E-commerce Analytics")

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)


@st.cache_data(ttl=60)
def load_kpi_data():
    query = """
    SELECT
        sales_date,
        total_orders,
        total_revenue,
        total_failed_payments,
        avg_order_value
    FROM agg_daily_sales
    ORDER BY sales_date DESC
    LIMIT 1;
    """
    return pd.read_sql(query, engine)


@st.cache_data(ttl=60)
def load_hourly_data():
    query = """
    SELECT
        sales_hour,
        total_orders,
        total_revenue
    FROM agg_hourly_orders
    ORDER BY sales_hour ASC;
    """
    return pd.read_sql(query, engine)


@st.cache_data(ttl=60)
def load_top_products():
    query = """
    SELECT
        snapshot_date,
        product_id,
        product_name,
        category,
        total_quantity,
        total_revenue
    FROM agg_top_products
    ORDER BY total_revenue DESC
    LIMIT 10;
    """
    return pd.read_sql(query, engine)


@st.cache_data(ttl=60)
def load_recent_orders():
    query = """
    SELECT
        order_id,
        customer_id,
        product_id,
        quantity,
        total_amount,
        payment_status,
        order_status,
        order_created_at
    FROM fact_orders
    ORDER BY order_created_at DESC
    LIMIT 20;
    """
    return pd.read_sql(query, engine)


kpi_df = load_kpi_data()
hourly_df = load_hourly_data()
top_products_df = load_top_products()
recent_orders_df = load_recent_orders()

if not kpi_df.empty:
    latest = kpi_df.iloc[0]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Orders", int(latest["total_orders"]))
    col2.metric("Total Revenue", f"${float(latest['total_revenue']):,.2f}")
    col3.metric("Failed Payments", int(latest["total_failed_payments"]))
    col4.metric("Avg Order Value", f"${float(latest['avg_order_value']):,.2f}")
else:
    st.warning("No KPI data found in agg_daily_sales.")

st.subheader("Hourly Revenue Raw Data")
st.write("Hourly rows:", len(hourly_df))
st.dataframe(hourly_df, use_container_width=True)

st.subheader("Hourly Revenue Trend")
if not hourly_df.empty:
    hourly_chart_df = hourly_df.copy()
    hourly_chart_df["sales_hour"] = pd.to_datetime(hourly_chart_df["sales_hour"])
    hourly_chart_df["total_revenue"] = pd.to_numeric(hourly_chart_df["total_revenue"], errors="coerce")
    hourly_chart_df = hourly_chart_df.dropna(subset=["sales_hour", "total_revenue"])
    hourly_chart_df = hourly_chart_df.sort_values("sales_hour")
    hourly_chart_df = hourly_chart_df.set_index("sales_hour")

    if len(hourly_chart_df) == 1:
        st.bar_chart(hourly_chart_df[["total_revenue"]], use_container_width=True)
    else:
        st.line_chart(hourly_chart_df[["total_revenue"]], use_container_width=True)
else:
    st.info("No hourly data found.")

st.subheader("Top Products")
if not top_products_df.empty:
    st.dataframe(top_products_df, use_container_width=True)
else:
    st.info("No top product data found.")

st.subheader("Recent Orders")
if not recent_orders_df.empty:
    st.dataframe(recent_orders_df, use_container_width=True)
else:
    st.info("No recent order data found.")