# This DAG is for testing purpose only

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="load_raw_data_from_bronze_to_postgres",
    default_args=default_args,
    schedule_interval=None, # No automatic schedule (manual trigger only)
    catchup=False,
) as dag:

    # Task: Run Spark job that loads raw data from Bronze Layer into PostgreSQL
    run_spark_job = BashOperator(
        task_id="run_spark_job_2",
        
        # Bash command to:
        # 1. Execute into 'spark-master' container
        # 2. Run 'spark-submit' to submit the job to Spark
        # 3. Specify Spark master URL for cluster mode
        # 4. Include PostgreSQL JDBC driver so Spark can connect to Postgres
        # 5. Run the Python ETL script inside /opt/spark_jobs/
        bash_command=(
            "docker exec spark-master spark-submit "
            "--master spark://spark-master:7077 "
            "--jars https://jdbc.postgresql.org/download/postgresql-42.7.3.jar "
            "/opt/spark_jobs/load_from_bronze_layer_to_postgres.py"
        ),
    )
