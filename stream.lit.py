# streamlit_app.py
import os
import json
from io import BytesIO
from typing import Optional, Tuple, List

import streamlit as st
import pandas as pd
import requests
from PIL import Image
import base64
import plotly.express as px
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# ------------------------------
# App configuration & constants
# ------------------------------
st.set_page_config(page_title="PhonePe Transaction Insights",
                   layout="wide",
                   initial_sidebar_state="expanded")

ASSETS_DIR = "assets"
DATA_DIR = "data"
LOGO_PATH = os.path.join(ASSETS_DIR, "logo.jpg")  # put your logo here
GEO_URL = ("https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112"
           "/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson")

quarter_map = {
    "Q1 (Jan-Mar)": 1,
    "Q2 (Apr-Jun)": 2,
    "Q3 (Jul-Sep)": 3,
    "Q4 (Oct-Dec)": 4
}

# ------------------------------
# Helpers: DB connection factory
# - Uses st.secrets['mysql'] if present (Streamlit Cloud recommended)
# - Else uses environment variables
# - Else offers SQLite fallback (data/phonepe.db or CSVs)
# ------------------------------
def get_engine() -> Tuple[Optional[Engine], str]:
    """
    Returns (sqlalchemy.Engine or None, db_type_string)
    db_type_string is 'mysql' or 'sqlite' or 'none'
    """
    # 1) Streamlit secrets (preferred on Streamlit Cloud)
    try:
        mysql_secret = st.secrets["mysql"]  # expects host, port, user, password, database
        host = mysql_secret.get("host")
        port = mysql_secret.get("port", 3306)
        user = mysql_secret.get("user")
        password = mysql_secret.get("password")
        database = mysql_secret.get("database")
        if all([host, user, password, database]):
            uri = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}"
            engine = create_engine(uri, pool_recycle=3600)
            return engine, "mysql"
    except Exception:
        # no secrets or malformed secrets — continue to next option
        pass

    # 2) Environment variables (if user sets them)
    env_host = os.environ.get("DB_HOST")
    env_user = os.environ.get("DB_USER")
    env_pass = os.environ.get("DB_PASSWORD")
    env_db = os.environ.get("DB_NAME")
    env_port = os.environ.get("DB_PORT", "3306")
    if env_host and env_user and env_pass and env_db:
        uri = f"mysql+mysqlconnector://{env_user}:{env_pass}@{env_host}:{env_port}/{env_db}"
        try:
            engine = create_engine(uri, pool_recycle=3600)
            return engine, "mysql"
        except Exception:
            pass

    # 3) If running locally and user has provided connector in code (not recommended)
    #    Attempt to read from local config file 'local_db.json' (NOT committed)
    local_cfg = "local_db.json"
    if os.path.exists(local_cfg):
        try:
            cfg = json.load(open(local_cfg, "r"))
            host = cfg.get("host")
            user = cfg.get("user")
            password = cfg.get("password")
            database = cfg.get("database")
            port = cfg.get("port", 3306)
            if all([host, user, password, database]):
                uri = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}"
                engine = create_engine(uri, pool_recycle=3600)
                return engine, "mysql"
        except Exception:
            pass

    # 4) SQLite fallback (file inside data/phonepe.db) - create folder if missing
    os.makedirs(DATA_DIR, exist_ok=True)
    sqlite_file = os.path.join(DATA_DIR, "phonepe.db")
    uri = f"sqlite:///{sqlite_file}"
    engine = create_engine(uri, connect_args={"check_same_thread": False})
    return engine, "sqlite"


# ------------------------------
# Run SQL safely
# ------------------------------
def run_query(query: str, params: Optional[Tuple] = None) -> pd.DataFrame:
    engine, db_type = get_engine()
    if engine is None:
        st.error("No database engine available.")
        return pd.DataFrame()

    try:
        # Pandas read_sql works with SQLAlchemy engine
        df = pd.read_sql(query, con=engine, params=params)
        return df
    except Exception as e:
        st.error(f"Error running query: {e}")
        return pd.DataFrame()


# ------------------------------
# UI Utilities
# ------------------------------
def load_geojson():
    try:
        resp = requests.get(GEO_URL, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        st.error("Could not load GeoJSON for India map. Check network or GEO_URL.")
        return None


def show_logo_and_header():
    if not os.path.exists(LOGO_PATH):
        st.warning(f"Logo not found at `{LOGO_PATH}`. Upload your logo to the `assets/` folder and name it `logo.jpg`.")
        st.title("PhonePe Transaction Insights")
        return

    try:
        img = Image.open(LOGO_PATH)
        buffered = BytesIO()
        img.save(buffered, format="JPEG")
        logo_b64 = base64.b64encode(buffered.getvalue()).decode()
        st.markdown(f"""
            <div style="display:flex; align-items:center; gap:15px">
                <img src="data:image/jpeg;base64,{logo_b64}" style="height:64px; border-radius:8px;">
                <h1 style="margin:0">PhonePe Transaction Insights</h1>
            </div>
            <p style="margin-top:0.25rem; color:var(--text-muted)">Explore India's Digital Economy with Data</p>
            <hr />
        """, unsafe_allow_html=True)
    except Exception as e:
        st.title("PhonePe Transaction Insights")


# ------------------------------
# Sidebar Navigation
# ------------------------------
with st.sidebar:
    st.write("## Navigation")
    page = st.radio("", ["Home", "Business Case Study", "Case Study Insights", "About"])
    st.markdown("---")
    # Quick DB info
    engine, db_type = get_engine()
    st.write("**DB Mode:**", db_type)
    if db_type == "sqlite":
        st.info("Using SQLite fallback. Place CSVs in /data/ and run `scripts/prepare_db.py` to load into SQLite.")
    else:
        st.success("Connected to MySQL (via secrets/env/local_db.json).")


# ------------------------------
# Load geojson
# ------------------------------
geo = load_geojson()

# ------------------------------
# Home Page
# ------------------------------
if page == "Home":
    show_logo_and_header()

    st.markdown("### 🔍 Outcome Highlights")
    st.markdown(
        """
        - 💳 **Aggregated Transactions:** View transaction volumes by type, region, and trends over time.  
        - 👩🏻‍💻 **User Engagement:** Explore how users interact with the app across brands, states, and districts.  
        - 🏛️ **Insurance Analytics:** Analyze state-wise and district-level adoption of insurance services.  
        - 🏞️ **Interactive Visuals:** Choropleths, bar charts, line graphs, and pie charts.  
        - 🎨 **Custom Filters:** Filter insights by year and quarter across India.
        """
    )

# ------------------------------
# Business Case Study Page
# ------------------------------
elif page == "Business Case Study":
    show_logo_and_header()
    st.header("📋 Business Case Study")
    sub_tab = st.selectbox("Explore analytics by:", ["Transaction", "User", "Insurance"])

    # Year & Quarter Dropdown (once only)
    years = ["All"] + [str(y) for y in range(2018, 2026)]
    quarters = ["All"] + list(quarter_map.keys())

    col1, col2 = st.columns(2)
    selected_year = col1.selectbox("Select Year", years, index=0)
    selected_quarter = col2.selectbox("Select Quarter", quarters, index=0)

    year = int(selected_year) if selected_year != "All" else None
    quarter = quarter_map[selected_quarter] if selected_quarter != "All" else None

    conditions = []
    params: List = []

    if year is not None:
        conditions.append("Years = :year")
        params.append(("year", year))
    if quarter is not None:
        conditions.append("Quarter = :quarter")
        params.append(("quarter", quarter))

    # Build where clause and params dict for SQLAlchemy/pandas
    where_clause = ""
    params_dict = {}
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)
        params_dict = {k: v for k, v in params}

    # Transaction tab
    if sub_tab == "Transaction":
        st.subheader("💳 Transaction Overview")
        q = f"""
            SELECT SUM(Transaction_count) AS TotalTransactions,
                   AVG(Transaction_count) AS AvgTransactions,
                   SUM(Transaction_amount) AS TotalRevenue,
                   AVG(Transaction_amount) AS AvgRevenue
            FROM aggregate_transaction
            {where_clause}
        """
        df = run_query(q, params=params_dict)
        if not df.empty:
            # Safe access with checks
            try:
                total_tx = df.iloc[0].get("TotalTransactions", 0) or 0
                avg_tx = df.iloc[0].get("AvgTransactions", 0) or 0
                total_revenue = df.iloc[0].get("TotalRevenue", 0) or 0
                avg_revenue = df.iloc[0].get("AvgRevenue", 0) or 0

                c1, c2 = st.columns(2)
                c1.metric("Total Transactions", f"{int(total_tx):,}")
                c1.metric("Average Transactions", f"{avg_tx:,.2f}")
                c2.metric("Total Revenue (₹)", f"{total_revenue:,.2f}")
                c2.metric("Avg Revenue (₹)", f"{avg_revenue:,.2f}")
            except Exception:
                st.info("No aggregate transaction numbers available.")

        # Map by state
        df_map = run_query(f"SELECT States, SUM(Transaction_count) AS TotalTransactions FROM map_transaction {where_clause} GROUP BY States", params=params_dict)
        if not df_map.empty and geo is not None:
            try:
                fig = px.choropleth(df_map, geojson=geo, locations="States",
                                    featureidkey="properties.ST_NM", color="TotalTransactions",
                                    color_continuous_scale="Reds", title="State-wise Total Transactions")
                fig.update_geos(fitbounds="locations", visible=False)
                st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.warning("Could not plot map: " + str(e))

        st.markdown("### 📌 Top 10 States by Transaction Volume")
        if not df_map.empty:
            st.dataframe(df_map.sort_values("TotalTransactions", ascending=False).head(10), use_container_width=True)
        else:
            st.info("No state-level transaction data available.")

    # User tab
    elif sub_tab == "User":
        st.subheader("📱 User Analytics")
        df_total = run_query(f"SELECT SUM(RegisteredUser) as TotalUsers, SUM(AppOpens) as TotalOpens FROM map_user {where_clause}", params=params_dict)
        if not df_total.empty:
            t_users = int(df_total.iloc[0].get("TotalUsers", 0) or 0)
            t_opens = int(df_total.iloc[0].get("TotalOpens", 0) or 0)
            st.metric("Total Registered Users", f"{t_users:,}")
            st.metric("Total App Opens", f"{t_opens:,}")
        else:
            st.info("No user summary available.")

        tab1, tab2, tab3 = st.tabs(["States", "Districts", "Pincodes"])
        with tab1:
            df_states = run_query(f"SELECT States, SUM(RegisteredUser) as TotalUsers FROM map_user {where_clause} GROUP BY States ORDER BY TotalUsers DESC LIMIT 10", params=params_dict)
            if not df_states.empty:
                st.dataframe(df_states, use_container_width=True)
            else:
                st.info("No states user data.")

        with tab2:
            df_districts = run_query(f"SELECT Districts, SUM(RegisteredUser) as TotalUsers FROM map_user {where_clause} GROUP BY Districts ORDER BY TotalUsers DESC LIMIT 10", params=params_dict)
            if not df_districts.empty:
                st.dataframe(df_districts, use_container_width=True)
            else:
                st.info("No districts data.")

        with tab3:
            df_pincodes = run_query(f"SELECT Pincodes, SUM(RegisteredUser) as TotalUsers FROM top_user {where_clause} GROUP BY Pincodes ORDER BY TotalUsers DESC LIMIT 10", params=params_dict)
            if not df_pincodes.empty:
                st.dataframe(df_pincodes, use_container_width=True)
            else:
                st.info("No pincodes data.")

    # Insurance tab
    elif sub_tab == "Insurance":
        st.subheader("🏛️ Insurance Insights")
        df_total = run_query(f"SELECT SUM(Transaction_count) as TotalTransactions, SUM(Transaction_amount) as TotalAmount FROM map_insurance {where_clause}", params=params_dict)
        if not df_total.empty:
            ttx = df_total.iloc[0].get("TotalTransactions")
            tamt = df_total.iloc[0].get("TotalAmount")
            st.metric("Total Insurance Transactions", f"{int(ttx):,}" if ttx else "data unavailable")
            st.metric("Total Insurance Amount (₹)", f"{int(tamt):,}" if tamt else "data unavailable")
        else:
            st.info("No insurance totals available.")

# ------------------------------
# Case Study Insights Page
# ------------------------------
elif page == "Case Study Insights":
    show_logo_and_header()
    st.header("📈 Case Study Insights")

    years = ["All"] + [str(y) for y in range(2018, 2026)]
    quarters = ["All"] + list(quarter_map.keys())
    col1, col2 = st.columns(2)
    selected_year = col1.selectbox("Select Year", years)
    selected_quarter = col2.selectbox("Select Quarter", quarters)

    year = int(selected_year) if selected_year != "All" else None
    quarter = quarter_map[selected_quarter] if selected_quarter != "All" else None

    conditions = []
    params = []
    if year is not None:
        conditions.append("Years = :year")
        params.append(("year", year))
    if quarter is not None:
        conditions.append("Quarter = :quarter")
        params.append(("quarter", quarter))
    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    params_dict = {k: v for k, v in params}

    case_option = st.selectbox("Select Case Study", [
        "Decoding Transaction Dynamics",
        "Device Dominance and User Engagement",
        "Insurance Penetration and Growth Potential",
        "Transaction Analysis for Market Expansion",
        "User Engagement and Growth Strategy",
    ])

    if case_option == "Decoding Transaction Dynamics":
        df_type = run_query(f"SELECT Transaction_type, SUM(Transaction_count) AS TotalCount FROM aggregate_transaction {where_clause} GROUP BY Transaction_type", params=params_dict)
        if not df_type.empty:
            st.plotly_chart(px.bar(df_type, x="Transaction_type", y="TotalCount", title="Transactions by Type"))

        df_map = run_query(f"SELECT States, SUM(Transaction_count) AS TotalTransactions FROM map_transaction {where_clause} GROUP BY States", params=params_dict)
        if not df_map.empty and geo is not None:
            fig_map = px.choropleth(df_map, geojson=geo, locations="States", featureidkey="properties.ST_NM", color="TotalTransactions", title="State-wise Transaction Volume")
            fig_map.update_geos(fitbounds="locations", visible=False)
            st.plotly_chart(fig_map)

        df_trend = run_query(f"SELECT Years, SUM(Transaction_amount) AS Amount FROM aggregate_transaction {where_clause} GROUP BY Years", params=params_dict)
        if not df_trend.empty:
            st.plotly_chart(px.line(df_trend, x="Years", y="Amount", markers=True, title="Transaction Trend Over Years"))

    elif case_option == "Device Dominance and User Engagement":
        df_users = run_query(f"SELECT Brands, SUM(Transaction_count) AS Users FROM aggregate_user {where_clause} GROUP BY Brands", params=params_dict)
        if not df_users.empty:
            st.plotly_chart(px.bar(df_users, x="Brands", y="Users", title="Users by Device Brand"))

        df_opens = run_query(f"""
            SELECT au.Brands, SUM(mu.AppOpens) AS AppOpens
            FROM aggregate_user au
            JOIN map_user mu ON au.States = mu.States AND au.Years = mu.Years AND au.Quarter = mu.Quarter
            {where_clause.replace('Years', 'au.Years').replace('Quarter', 'au.Quarter')}
            GROUP BY au.Brands
        """, params=params_dict)
        if not df_opens.empty:
            st.plotly_chart(px.bar(df_opens, x="Brands", y="AppOpens", title="App Opens by Device Brand"))

    elif case_option == "Insurance Penetration and Growth Potential":
        df_state_yearly = run_query(f"SELECT States, Years, SUM(Total_count) AS TotalCount FROM aggregated_insurance {where_clause} GROUP BY States, Years", params=params_dict)
        if not df_state_yearly.empty:
            fig = px.bar(df_state_yearly.sort_values("TotalCount", ascending=False), x="States", y="TotalCount", color="Years", title="Insurance Transactions by State and Year")
            st.plotly_chart(fig)
        else:
            st.info("No insurance state-year data available.")

    elif case_option == "Transaction Analysis for Market Expansion":
        df_amount = run_query(f"SELECT States, SUM(Transaction_amount) AS Amount FROM aggregate_transaction {where_clause} GROUP BY States", params=params_dict)
        if not df_amount.empty:
            st.plotly_chart(px.bar(df_amount.sort_values("Amount", ascending=False), x="States", y="Amount", title="States by Transaction Value"))

        df_yearwise = run_query(f"SELECT Years, SUM(Transaction_amount) AS Total FROM aggregate_transaction {where_clause} GROUP BY Years", params=params_dict)
        if not df_yearwise.empty:
            st.plotly_chart(px.scatter(df_yearwise, x="Years", y="Total", title="Transaction Value Over Years"))

    elif case_option == "User Engagement and Growth Strategy":
        df_app = run_query(f"SELECT States, SUM(AppOpens) AS Opens FROM map_user {where_clause} GROUP BY States", params=params_dict)
        if not df_app.empty:
            st.plotly_chart(px.bar(df_app.sort_values("Opens", ascending=False).head(10), x="States", y="Opens", title="Top States by App Opens"))

        df_reg = run_query(f"SELECT Years, SUM(RegisteredUser) AS Users FROM map_user {where_clause} GROUP BY Years", params=params_dict)
        if not df_reg.empty:
            st.plotly_chart(px.line(df_reg, x="Years", y="Users", markers=True, title="User Registrations Over Time"))

# ------------------------------
# About Page
# ------------------------------
elif page == "About":
    show_logo_and_header()
    st.markdown("### About this App")
    st.markdown("""
    - Built for exploring PhonePe transaction datasets at state/district/pincode granularity.
    - To run on Streamlit Cloud: configure your database in **Settings → Secrets** as `mysql` (host, port, user, password, database).
    - If you do not have a cloud MySQL, use SQLite fallback by populating `/data/phonepe.db` or by placing CSVs in `/data/` (e.g. aggregate_transaction.csv, map_transaction.csv, map_user.csv, etc.)
    """)

# ------------------------------
# Footer
# ------------------------------
st.markdown("<hr style='margin-top:2rem'>", unsafe_allow_html=True)
st.caption("PhonePe Transaction Insights — Streamlit Dashboard")
