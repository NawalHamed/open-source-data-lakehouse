from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="load_from_silver_layer_to_gold_layer_summary_data",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    run_spark_job = BashOperator(
        task_id="run_spark_job",
        bash_command="docker exec spark-master spark-submit /opt/spark_jobs/load_from_silver_layer_to_gold_layer_summary_data.py",
    )