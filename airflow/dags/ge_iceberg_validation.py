from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG("ge_iceberg_validation", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False) as dag:
    validate = BashOperator(
        task_id="run_ge_check",
        bash_command="docker exec spark-master spark-submit /opt/spark_jobs/ge_validate_iceberg.py"
    )
