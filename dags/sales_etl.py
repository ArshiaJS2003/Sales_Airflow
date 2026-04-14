"""
Real-Time Sales Analysis DAG with Visualization in Logs
"""

from airflow.sdk import Asset, dag, task, chain
from pendulum import datetime, duration
from tabulate import tabulate
import pandas as pd
import duckdb
import logging
import os
import base64

from include.custom_functions.sales_functions import (
    generate_sales_batch,
    compute_revenue_summary,
    compute_top_products,
    create_revenue_by_region_chart,
    create_revenue_by_category_chart,
    create_top_products_chart,
    create_payment_method_chart,
    create_sales_timeline_chart,
)

t_log = logging.getLogger("airflow.task")

# ---------------- CONFIG ---------------- #
_DUCKDB_INSTANCE = os.getenv("DUCKDB_INSTANCE_NAME", "include/astronomy.db")
_SALES_TABLE = "sales_transactions"
_REVENUE_TABLE = "sales_revenue_summary"
_TOP_PRODUCTS_TABLE = "sales_top_products"
_BATCH_SIZE = int(os.getenv("SALES_BATCH_SIZE", "50"))
_CHARTS_OUTPUT_DIR = os.getenv("SALES_CHARTS_DIR", "include/sales_charts")

_SALES_ASSET_URI = f"duckdb://{_DUCKDB_INSTANCE}/{_SALES_TABLE}"

# ---------------- HELPER ---------------- #
def show_chart_in_logs(path):
    """Display chart in logs using base64 (REAL-TIME VIEW)"""
    try:
        with open(path, "rb") as f:
            encoded = base64.b64encode(f.read()).decode()
        print("\n===== REAL-TIME CHART =====\n")
        print(f"data:image/png;base64,{encoded}")
        print("\n===== END CHART =====\n")
    except Exception as e:
        print(f"Error displaying chart: {e}")

# ---------------- DAG ---------------- #
@dag(
    start_date=datetime(2025, 4, 1),
    schedule="*/5 * * * *",
    max_consecutive_failed_dag_runs=5,
    max_active_runs=1,
    catchup=False,
    default_args={
        "owner": "analytics-team",
        "retries": 2,
        "retry_delay": duration(seconds=15),
    },
    tags=["sales", "real-time", "analytics"],
)
def sales_realtime_analysis():

    # -------- SETUP -------- #
    @task
    def create_sales_tables(db_path: str = _DUCKDB_INSTANCE):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        os.makedirs(_CHARTS_OUTPUT_DIR, exist_ok=True)

        conn = duckdb.connect(db_path)

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {_SALES_TABLE} (
                transaction_id VARCHAR PRIMARY KEY,
                timestamp TIMESTAMP,
                product_name VARCHAR,
                category VARCHAR,
                region VARCHAR,
                quantity INTEGER,
                unit_price DOUBLE,
                total_amount DOUBLE,
                payment_method VARCHAR
            )
        """)

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {_REVENUE_TABLE} (
                batch_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                category VARCHAR,
                region VARCHAR,
                total_revenue DOUBLE,
                total_quantity INTEGER,
                num_transactions INTEGER,
                avg_order_value DOUBLE
            )
        """)

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {_TOP_PRODUCTS_TABLE} (
                batch_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                rank INTEGER,
                product_name VARCHAR,
                total_revenue DOUBLE,
                units_sold INTEGER,
                num_orders INTEGER
            )
        """)

        conn.close()
        t_log.info("Tables ready")

    # -------- EXTRACT -------- #
    @task
    def extract_sales():
        df = generate_sales_batch(_BATCH_SIZE)
        t_log.info(f"Extracted {len(df)} records")
        return df

    # -------- TRANSFORM -------- #
    @task
    def analyze_revenue(df: pd.DataFrame):
        summary = compute_revenue_summary(df)
        return summary

    @task
    def analyze_top_products(df: pd.DataFrame):
        return compute_top_products(df, 5)

    # -------- LOAD -------- #
    @task(outlets=[Asset(_SALES_ASSET_URI)])
    def load_transactions(df: pd.DataFrame):
        conn = duckdb.connect(_DUCKDB_INSTANCE)
        conn.execute(f"INSERT OR IGNORE INTO {_SALES_TABLE} BY NAME SELECT * FROM df")
        count = conn.execute(f"SELECT COUNT(*) FROM {_SALES_TABLE}").fetchone()[0]
        conn.close()
        return count

    @task
    def load_revenue(summary: pd.DataFrame):
        conn = duckdb.connect(_DUCKDB_INSTANCE)
        conn.execute(f"""
            INSERT INTO {_REVENUE_TABLE}
            SELECT * FROM summary
        """)
        conn.close()

    @task
    def load_top(top: pd.DataFrame):
        conn = duckdb.connect(_DUCKDB_INSTANCE)
        conn.execute(f"""
            INSERT INTO {_TOP_PRODUCTS_TABLE}
            SELECT * FROM top
        """)
        conn.close()

    # -------- VISUALIZE -------- #
    @task
    def visualize_all(df: pd.DataFrame, summary: pd.DataFrame, top: pd.DataFrame):

        paths = []

        # Region
        p1 = create_revenue_by_region_chart(summary, _CHARTS_OUTPUT_DIR)
        show_chart_in_logs(p1)
        paths.append(p1)

        # Category
        p2 = create_revenue_by_category_chart(summary, _CHARTS_OUTPUT_DIR)
        show_chart_in_logs(p2)
        paths.append(p2)

        # Top Products
        p3 = create_top_products_chart(top, _CHARTS_OUTPUT_DIR)
        show_chart_in_logs(p3)
        paths.append(p3)

        # Payment
        p4 = create_payment_method_chart(df, _CHARTS_OUTPUT_DIR)
        show_chart_in_logs(p4)
        paths.append(p4)

        # Timeline
        p5 = create_sales_timeline_chart(df, _CHARTS_OUTPUT_DIR)
        show_chart_in_logs(p5)
        paths.append(p5)

        return paths

    # -------- REPORT -------- #
    @task
    def report(summary, top, count, charts):
        t_log.info("===== REPORT =====")
        t_log.info(f"Total records: {count}")

        t_log.info("\nRevenue Summary:")
        t_log.info(tabulate(summary, headers="keys"))

        t_log.info("\nTop Products:")
        t_log.info(tabulate(top, headers="keys"))

        t_log.info("\nCharts:")
        for c in charts:
            t_log.info(c)

    # -------- FLOW -------- #
    tables = create_sales_tables()
    raw = extract_sales()

    chain(tables, raw)

    revenue = analyze_revenue(raw)
    top = analyze_top_products(raw)

    total = load_transactions(raw)
    load1 = load_revenue(revenue)
    load2 = load_top(top)

    charts = visualize_all(raw, revenue, top)

    final = report(revenue, top, total, charts)

    chain([total, load1, load2, charts], final)


sales_realtime_analysis()
