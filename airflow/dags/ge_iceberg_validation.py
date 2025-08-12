
# ========================
# Import required libraries
# ========================
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import great_expectations as gx
import boto3 # AWS SDK for Python (used here to connect to MinIO via S3 API)
import io
import requests # For sending HTTP requests (used to send Teams notifications)

# ========================
# Webhook URL for sending notifications to a Microsoft Teams channel
# ========================
TEAMS_WEBHOOK_URL = "https://rihalom598.webhook.office.com/webhookb2/aa0dc364-e78a-4384-b733-59b516f1f7f1@6f1bd9ba-810d-45ab-9cc6-e34cb343de1d/IncomingWebhook/2001d59c96c44405ac5dc30da4ec6b6c/8a7137b6-5a10-474a-8467-6bdbbfb589b7/V2J_N8S32Dx7sNpkMY29xrnjreuUDrV-hOIzLkHYigedg1" 

def send_teams_notification(context, status):
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = task_instance.task_id
    log_url = task_instance.log_url
    color = "00FF00" if status == "Success" else "FF0000"  # Set notification color based on success or failure

    # Build the Teams message payload using MessageCard format
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
    
    # Send the HTTP POST request to the Teams webhook
    headers = {'Content-Type': 'application/json'}
    response = requests.post(TEAMS_WEBHOOK_URL, json=message, headers=headers)
    response.raise_for_status() # Raise an error if the request failed

def on_success_callback(context):
    send_teams_notification(context, "Success")

def on_failure_callback(context):
    send_teams_notification(context, "Failed")

# ========================
# Great Expectations Validation Function
# ========================

def run_great_expectations_validation_from_minio():
    # MinIO connection settings
    MINIO_ENDPOINT = "http://minio:9009"
    MINIO_ACCESS_KEY = "minioadmin"
    MINIO_SECRET_KEY = "minioadmin"
    MINIO_BUCKET_NAME = "lakehouse"

    # Get current UTC date to build file paths for today's data
    now = datetime.utcnow()
    year, month, day = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")


    # File paths in the MinIO bucket for weather and master data
    bronze_weather_prefix = f"bronze_layer/{year}/{month}/{day}/csv/weather_data/"
    bronze_country_key = "bronze_layer/master/countries_data.csv"
    bronze_city_key = "bronze_layer/master/cities_data.csv"

     # DataFrames to hold loaded data
    df_country, df_city, df_weather = None, None, None

    try:
        # Initialize S3 client to connect to MinIO
        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=boto3.session.Config(signature_version='s3v4'),
            verify=False # Skip SSL verification (MinIO local setup often uses self-signed certs)
        )
        print("MinIO S3 client initialized.")

        # Function to read a CSV file from MinIO into a Pandas DataFrame
        def read_csv_from_minio(bucket, key):
            print(f"Reading s3a://{bucket}/{key}...")
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            return pd.read_csv(io.BytesIO(obj['Body'].read()))
        
        # Load country data
        df_country = read_csv_from_minio(MINIO_BUCKET_NAME, bronze_country_key)
        print(f"df_country loaded. Shape: {df_country.shape}")
        print(df_country.head())

        # Load city data
        df_city = read_csv_from_minio(MINIO_BUCKET_NAME, bronze_city_key)
        print(f"df_city loaded. Shape: {df_city.shape}")
        print(df_city.head())
        
        # Load weather data (can have multiple CSVs for the day)
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

        # Combine all weather CSVs into one DataFrame
        if not weather_dfs:
            print("No weather CSV files found for the current date. Creating empty DataFrame.")
            df_weather = pd.DataFrame()
        else:
            df_weather = pd.concat(weather_dfs, ignore_index=True)

        print(f"df_weather loaded. Total shape: {df_weather.shape}")
        print(df_weather.head())

    except Exception as e:
        # Catch and rethrow errors from MinIO data loading
        print(f"An error occurred during MinIO data loading: {e}")
        raise Exception(f"Failed to load data from MinIO: {e}") 

    
    # ========================
    # Great Expectations Validation
    # ========================
    
    print("\n--- Starting Great Expectations Validation ---")
    all_validations_successful = True

    #Rule1
    if not df_country.empty:
        # Validate that df_country contains the column "name"
        print("\nValidating df_country...")
        validator_country = gx.from_pandas(df_country)
        validator_country.expect_column_to_exist("name")
        results_country = validator_country.validate()
        
        print(f"df_country Validation Success: {results_country.success}")
        
        # If validation failed, mark as unsuccessful and print details
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
        print("All Great Expectations validations passed.")
    else:
        print("Some validations failed. Check logs for details.")

# ========================
# Airflow DAG Definition
# ========================
with DAG(
    dag_id='data_quality_great_expectations',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None, # This DAG is run manually (no schedule)
    catchup=False,
    tags=['data_quality', 'minio', 'great_expectations', 'pandas', 'lakehouse', 'bronze'],
    doc_md="""
    ### MinIO Bronze Layer Data Quality Check with Great Expectations and Teams Alerts
    This DAG reads daily weather and master data (countries, cities) from the
    MinIO bronze layer into Pandas DataFrames. It performs Great Expectations
    validation and notifies a Microsoft Teams channel of task success or failure.
    """
) as dag:
    # Task: Run Great Expectations validations on MinIO data
    data_quality_check_task = PythonOperator(
        task_id='run_minio_data_quality_checks',
        python_callable=run_great_expectations_validation_from_minio, # The function to execute
        on_success_callback=on_success_callback, # Send Teams message on success
        on_failure_callback=on_failure_callback, # Send Teams message on failure
    )
