from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# ========== Default Arguments for the DAG ==========
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

# ========== Define the DAG ==========
with DAG(
    dag_id="load_unstratutured_metadata_data_from_bronze_to_silver_table",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # ========== Task: Run Spark Job ==========
    run_spark_job = BashOperator(
        task_id="run_spark_job",

        # Command to:
        # 1. Enter the 'spark-master' Docker container
        # 2. Run 'spark-submit' to start a PySpark job
        # 3. Execute the ETL script for moving data from Bronze → Silver layer
        bash_command="docker exec spark-master spark-submit /opt/spark_jobs/load_from_bronze_layer_to_silver_layer_unstructured_data_table.py",
    )
