# dags/retail_sales_api_etl.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
from pathlib import Path
import polars as pl

# -----------------------------------------
# 1. Project Path Setup
# -----------------------------------------
AIRFLOW_HOME = Path(__file__).resolve().parents[1]   # airflow folder
PROJECT_ROOT = AIRFLOW_HOME                          # main ETL project

SCRIPTS_DIR = PROJECT_ROOT / "scripts"
DATA_DIR = PROJECT_ROOT / "data" / "processed"

sys.path.insert(0, str(PROJECT_ROOT))

# Import ETL components
from scripts.extract_api import extract_from_api
from scripts.transform import clean_and_transform, write_analytics
from scripts.load import load_data

API_URL = "https://dummyjson.com/carts?limit=100&skip=0"


# -----------------------------------------
# 2. Extract Task
# -----------------------------------------
def extract_task_func(**kwargs):
    df = extract_from_api(API_URL)

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    out_path = DATA_DIR / "extracted.parquet"
    df.write_parquet(out_path)

    print(f"Extracted data saved at: {out_path}")
    return str(out_path)


# -----------------------------------------
# 3. Transform Task
# -----------------------------------------
def transform_task_func(**kwargs):
    extracted_file = DATA_DIR / "extracted.parquet"

    if not extracted_file.exists():
        raise FileNotFoundError("❌ extracted.parquet missing — Extract step failed")

    df = pl.read_parquet(extracted_file)
    df_clean = clean_and_transform(df)

    df_clean.write_parquet(DATA_DIR / "clean_sales.parquet")

    write_analytics(df_clean)

    print("Transformation + analytics done.")


# -----------------------------------------
# 4. Load Task
# -----------------------------------------
def load_task_func(**kwargs):
    load_data()
    print("MySQL load completed successfully.")


# -----------------------------------------
# 5. Define Airflow DAG
# -----------------------------------------
default_args = {
    "start_date": datetime(2024, 1, 1)
}

with DAG(
    dag_id="retail_sales_api_etl",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract_task_func,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform_task_func,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load_task_func,
    )

    extract_task >> transform_task >> load_task
