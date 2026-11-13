"""Streamlit dashboard over the gold Delta tables."""
import os

import pandas as pd
import streamlit as st
from deltalake import DeltaTable

LAKE = os.getenv("LAKE_PATH", "data/lake")

st.set_page_config(page_title="Real-time fraud monitor", layout="wide")
st.title("Real-time card fraud monitor")


@st.cache_data(ttl=10)
def load(path: str) -> pd.DataFrame:
    try:
        return DeltaTable(f"{LAKE}/{path}").to_pandas()
    except Exception:
        return pd.DataFrame()


kpis = load("gold/kpis_per_minute")
alerts = load("gold/fraud_alerts")

if kpis.empty:
    st.info("Waiting for the streaming job to write its first batch...")
    st.stop()

kpis = kpis.groupby("minute", as_index=False)[["tx_count", "tx_amount", "blocked", "review"]].sum().sort_values("minute")
c1, c2, c3 = st.columns(3)
c1.metric("Transactions (last 15 min)", int(kpis.tail(15)["tx_count"].sum()))
c2.metric("Blocked (last 15 min)", int(kpis.tail(15)["blocked"].sum()))
c3.metric("Sent to review (last 15 min)", int(kpis.tail(15)["review"].sum()))

st.subheader("Volume and alerts per minute")
st.line_chart(kpis.set_index("minute")[["tx_count", "blocked", "review"]])

st.subheader("Latest alerts")
if not alerts.empty:
    st.dataframe(alerts.sort_values("scored_at", ascending=False).head(50), use_container_width=True)
