# Use official Airflow base image
FROM apache/airflow:2.7.0

# Switch to root for system-level package installations
USER root

# Install required Python libraries, including Pillow (PIL)
RUN pip install --no-cache-dir \
    apache-airflow-providers-docker==3.8.0 \
    pandas \
    requests \
    minio \
    python-dotenv \
    pyarrow \
    Pillow \
    faker \
    matplotlib

# Return to airflow user for security best practices
USER airflow
