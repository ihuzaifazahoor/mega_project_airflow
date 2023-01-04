from airflow import DAG
from datetime import datetime
from utils_for_huzaifa.tasks import *

stocks = [{"ticker": "AAPL", "company_name": "Apple"}]

with DAG(
    dag_id="reddit_pipeline",
    description="""Reddit Pipeline by Huzaifa""",
    schedule_interval="@daily",
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:

    result = fetch_data_from_reddit(stocks)

    insert_data_to_database(result)
