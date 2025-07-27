from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import great_expectations as gx
import boto3
import io
import requests

# ========================
# Microsoft Teams Notification
# ========================
TEAMS_WEBHOOK_URL = "https://rihalom598.webhook.office.com/webhookb2/847565b1-bf59-4ce3-b74b-e47d69197c20@6f1bd9ba-810d-45ab-9cc6-e34cb343de1d/IncomingWebhook/ffdf6b2f5cfe4dddae5a7191b96c1cb7/e0de8690-8392-4934-b037-c8c3dbb85f51/V2k23R6sj5i-Rs1JdC59Nv6qo3jzKZXVDJ_KVJB-ioQWQ1"  # Replace with your actual webhook URL

def send_teams_notification(context, status):
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = task_instance.task_id
    log_url = task_instance.log_url
    color = "00FF00" if status == "Success" else "FF0000"

    message = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": color,
        "summary": f"Airflow Task {status}",
        "sections": [{
            "activityTitle": f"**Airflow Task {status}**",
            "facts": [
                {"name": "DAG", "value": dag_id},
                {"name": "Task", "value": task_id},
                {"name": "Status", "value": status},
                {"name": "Log URL", "value": log_url}
            ],
            "markdown": True
        }]
    }

    headers = {'Content-Type': 'application/json'}
    response = requests.post(TEAMS_WEBHOOK_URL, json=message, headers=headers)
    response.raise_for_status()

def on_success_callback(context):
    send_teams_notification(context, "Success")

def on_failure_callback(context):
    send_teams_notification(context, "Failed")

# ========================
# Great Expectations Validation Function
# ========================
def run_great_expectations_validation_from_minio():
    MINIO_ENDPOINT = "http://minio:9009"
    MINIO_ACCESS_KEY = "minioadmin"
    MINIO_SECRET_KEY = "minioadmin"
    MINIO_BUCKET_NAME = "lakehouse"

    now = datetime.utcnow()
    year, month, day = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
    bronze_weather_prefix = f"bronze_layer/{year}/{month}/{day}/csv/weather_data/"
    bronze_country_key = "bronze_layer/master/countries_data.csv"
    bronze_city_key = "bronze_layer/master/cities_data.csv"

    df_country, df_city, df_weather = None, None, None

    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=boto3.session.Config(signature_version='s3v4'),
            verify=False
        )
        print("MinIO S3 client initialized.")

        def read_csv_from_minio(bucket, key):
            print(f"Reading s3a://{bucket}/{key}...")
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            return pd.read_csv(io.BytesIO(obj['Body'].read()))

        df_country = read_csv_from_minio(MINIO_BUCKET_NAME, bronze_country_key)
        print(f"df_country loaded. Shape: {df_country.shape}")
        print(df_country.head())

        df_city = read_csv_from_minio(MINIO_BUCKET_NAME, bronze_city_key)
        print(f"df_city loaded. Shape: {df_city.shape}")
        print(df_city.head())

        weather_dfs = []
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=MINIO_BUCKET_NAME, Prefix=bronze_weather_prefix)

        for page in pages:
            if "Contents" in page:
                for obj_info in page['Contents']:
                    object_key = obj_info['Key']
                    if object_key.endswith('.csv'):
                        print(f"Found weather CSV: {object_key}")
                        df_part = read_csv_from_minio(MINIO_BUCKET_NAME, object_key)
                        weather_dfs.append(df_part)
            else:
                print(f"No objects found under prefix: {bronze_weather_prefix}")

        if not weather_dfs:
            print("No weather CSV files found for the current date. Creating empty DataFrame.")
            df_weather = pd.DataFrame()
        else:
            df_weather = pd.concat(weather_dfs, ignore_index=True)

        print(f"df_weather loaded. Total shape: {df_weather.shape}")
        print(df_weather.head())

    except Exception as e:
        print(f"An error occurred during MinIO data loading: {e}")
        raise Exception(f"Failed to load data from MinIO: {e}") 

    print("\n--- Starting Great Expectations Validation ---")
    all_validations_successful = True

    if not df_country.empty:
        print("\nValidating df_country...")
        validator_country = gx.from_pandas(df_country)
        validator_country.expect_column_to_exist("name")
        results_country = validator_country.validate()
        print(f"df_country Validation Success: {results_country.success}")
        if not results_country.success:
            all_validations_successful = False
            print("df_country Validation Failures:")
            for result in results_country.results:
                if not result.success:
                    print(f"- {result.expectation_config.expectation_type} (Column: {result.expectation_config.kwargs.get('column')}): {result.result}")
    else:
        print("df_country is empty, skipping validation.")

    print("\n--- Great Expectations Validation Process Completed ---")
    if all_validations_successful:
        print("✅ All Great Expectations validations passed.")
    else:
        print("❌ Some validations failed. Check logs for details.")

# ========================
# DAG Definition
# ========================
with DAG(
    dag_id='data_quality_great_expectations',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['data_quality', 'minio', 'great_expectations', 'pandas', 'lakehouse', 'bronze'],
    doc_md="""
    ### MinIO Bronze Layer Data Quality Check with Great Expectations and Teams Alerts
    This DAG reads daily weather and master data (countries, cities) from the
    MinIO bronze layer into Pandas DataFrames. It performs Great Expectations
    validation and notifies a Microsoft Teams channel of task success or failure.
    """
) as dag:

    data_quality_check_task = PythonOperator(
        task_id='run_minio_data_quality_checks',
        python_callable=run_great_expectations_validation_from_minio,
        on_success_callback=on_success_callback,
        on_failure_callback=on_failure_callback,
    )
