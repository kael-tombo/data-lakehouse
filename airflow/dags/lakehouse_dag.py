from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

P = "/opt/data-lakehouse"
with DAG("lakehouse_daily", start_date=datetime(2026, 10, 1), schedule="@daily", catchup=False,
         default_args={"retries": 2, "retry_delay": timedelta(minutes=5)}, tags=["lakehouse", "spark"]) as dag:
    BashOperator(task_id="bronze_silver_gold_quality", bash_command=(
        f"cd {P} && python -m lakehouse.pipeline --orders data/raw/orders_{{{{ ds_nodash }}}}.csv "
        f"--customers data/raw/customers.csv --batch {{{{ ds_nodash }}}}"))
