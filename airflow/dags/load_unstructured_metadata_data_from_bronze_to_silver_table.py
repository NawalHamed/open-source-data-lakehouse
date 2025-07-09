from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="load_unstratutured_metadata_data_from_bronze_to_silver_table",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    run_spark_job = BashOperator(
        task_id="run_spark_job",
        bash_command="docker exec spark-master spark-submit /opt/spark_jobs/load_from_bronze_layer_to_silver_layer_unstructured_data_table.py",
    )
