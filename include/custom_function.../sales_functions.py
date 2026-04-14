from airflow.decorators import dag, task
from pendulum import datetime
import pandas as pd
import random
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import os
import duckdb
import base64
import logging

t_log = logging.getLogger("airflow.task")

DB_PATH = "include/sales.db"
CHART_DIR = "include/sales_charts"

# ---------------- HELPER ---------------- #
def show_chart(path):
    """Display chart in logs using base64"""
    try:
        with open(path, "rb") as f:
            encoded = base64.b64encode(f.read()).decode()
        t_log.info(f"VIEW CHART → data:image/png;base64,{encoded}")
    except Exception as e:
        t_log.error(f"Error showing chart: {e}")

# ---------------- DAG ---------------- #
@dag(
    start_date=datetime(2025, 4, 1),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["sales", "realtime", "analytics"]
)
def sales_realtime_pipeline():

    # -------- SETUP -------- #
    @task
    def setup():
        os.makedirs("include", exist_ok=True)
        os.makedirs(CHART_DIR, exist_ok=True)

        conn = duckdb.connect(DB_PATH)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sales (
                product VARCHAR,
                quantity INTEGER,
                price INTEGER,
                revenue INTEGER
            )
        """)
        conn.close()

        t_log.info("Setup done")

    # -------- EXTRACT -------- #
    @task
    def extract():
        products = ["Laptop", "Phone", "Tablet"]

        df = pd.DataFrame({
            "product": [random.choice(products) for _ in range(20)],
            "quantity": [random.randint(1, 5) for _ in range(20)],
            "price": [random.choice([15000, 20000, 50000]) for _ in range(20)]
        })

        t_log.info(f"Extracted Data:\n{df}")
        return df

    # -------- TRANSFORM -------- #
    @task
    def transform(df: pd.DataFrame):
        df["revenue"] = df["quantity"] * df["price"]
        summary = df.groupby("product")["revenue"].sum().reset_index()

        t_log.info(f"Summary:\n{summary}")
        return df, summary

    # -------- LOAD -------- #
    @task
    def load(df: pd.DataFrame):
        conn = duckdb.connect(DB_PATH)
        conn.execute("INSERT INTO sales SELECT * FROM df")
        count = conn.execute("SELECT COUNT(*) FROM sales").fetchone()[0]
        conn.close()

        t_log.info(f"Total records in DB: {count}")
        return count

    # -------- VISUALIZE -------- #
    @task
    def visualize(summary: pd.DataFrame):

        bar_path = f"{CHART_DIR}/bar_chart.png"
        pie_path = f"{CHART_DIR}/pie_chart.png"

        # Bar chart
        plt.figure()
        plt.bar(summary["product"], summary["revenue"])
        plt.title("Revenue by Product")
        plt.savefig(bar_path)
        plt.close()

        # Pie chart
        plt.figure()
        plt.pie(summary["revenue"], labels=summary["product"], autopct="%1.1f%%")
        plt.title("Revenue Distribution")
        plt.savefig(pie_path)
        plt.close()

        # Show in logs (REAL-TIME)
        show_chart(bar_path)
        show_chart(pie_path)

        t_log.info(f"Charts saved at {CHART_DIR}")

        return [bar_path, pie_path]

    # -------- REPORT -------- #
    @task
    def report(count, charts):
        t_log.info("===== FINAL REPORT =====")
        t_log.info(f"Total records: {count}")
        for c in charts:
            t_log.info(f"Chart: {c}")

    # -------- FLOW -------- #
    s = setup()
    df = extract()
    transformed = transform(df)
    count = load(transformed[0])
    charts = visualize(transformed[1])
    report(count, charts)

sales_realtime_pipeline()
