FROM apache/airflow:2.7.0

USER airflow

# Install additional Python packages as the airflow user
# Pin the docker provider version for known compatibility with Airflow 2.7.0
RUN pip install --no-cache-dir \
    apache-airflow-providers-docker==3.8.0 \
    pandas \
    requests \
    minio \
    python-dotenv \
	pyarrow