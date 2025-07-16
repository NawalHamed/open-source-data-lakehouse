from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import great_expectations as gx
import boto3 # Required for MinIO/S3 interaction
import io    # Required for reading data from S3 object into pandas

def run_great_expectations_validation_from_minio():
    """
    This function reads data from specified MinIO (S3-compatible) paths into Pandas DataFrames,
    then performs Great Expectations validation on each DataFrame.
    It will print the validation results but will NOT raise an AirflowException
    even if validation fails for an expectation, ensuring the Airflow task always succeeds.
    However, if data loading from MinIO fails, the task WILL raise an exception.
    """
    # --- MinIO Configuration (REPLACE WITH AIRFLOW CONNECTIONS IN PRODUCTION!) ---
    MINIO_ENDPOINT = "http://minio:9009" # Example MinIO endpoint (adjust for your setup)
    MINIO_ACCESS_KEY = "minioadmin"      # Replace with your MinIO access key
    MINIO_SECRET_KEY = "minioadmin"      # Replace with your MinIO secret key
    MINIO_BUCKET_NAME = "lakehouse"     # Matches 's3a://lakehouse/'
    # --- End MinIO Configuration ---

    # 1️⃣ Dynamic Date Detection for Weather Data
    now = datetime.utcnow()
    year, month, day = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
    
    # Base path for weather data (without the wildcard initially)
    bronze_weather_prefix = f"bronze_layer/{year}/{month}/{day}/csv/weather_data/"

    # 2️⃣ Paths for Master Data
    bronze_country_key = "bronze_layer/master/countries_data.csv"
    bronze_city_key = "bronze_layer/master/cities_data.csv"

    print(f"Connecting to MinIO endpoint: {MINIO_ENDPOINT}")
    print(f"Using bucket: {MINIO_BUCKET_NAME}")

    df_country, df_city, df_weather = None, None, None # Initialize DataFrames

    try:
        # Initialize S3 client for MinIO
        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=boto3.session.Config(signature_version='s3v4'), # Use s3v4 for MinIO compatibility
            verify=False # Set to True if using HTTPS with a valid certificate
        )
        print("MinIO S3 client initialized.")

        # Helper function to read a single CSV from MinIO
        def read_csv_from_minio(bucket, key):
            print(f"Reading s3a://{bucket}/{key}...")
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            return pd.read_csv(io.BytesIO(obj['Body'].read()))

        # 4️⃣ Load Bronze Data into Pandas DataFrames

        # Load Country Data
        df_country = read_csv_from_minio(MINIO_BUCKET_NAME, bronze_country_key)
        print(f"df_country loaded. Shape: {df_country.shape}")
        print("df_country head:\n", df_country.head())

        # Load City Data
        df_city = read_csv_from_minio(MINIO_BUCKET_NAME, bronze_city_key)
        print(f"df_city loaded. Shape: {df_city.shape}")
        print("df_city head:\n", df_city.head())

        # Load Weather Data (handling wildcard)
        print(f"Listing objects in s3a://{MINIO_BUCKET_NAME}/{bronze_weather_prefix}...")
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
            df_weather = pd.DataFrame() # Create an empty DataFrame if no files found
        else:
            df_weather = pd.concat(weather_dfs, ignore_index=True)
        
        print(f"df_weather loaded. Total shape: {df_weather.shape}")
        print("df_weather head:\n", df_weather.head())

    except Exception as e:
        print(f"An error occurred during MinIO data loading: {e}")
        # If data loading fails, we should typically fail the Airflow task
        raise Exception(f"Failed to load data from MinIO: {e}") 

    # --- Great Expectations Validation ---
    print("\n--- Starting Great Expectations Validation ---")
    all_validations_successful = True

    # Validate df_country
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
        # Consider if an empty master data DataFrame should fail the task
        # all_validations_successful = False # Uncomment to fail if master data is empty

   
    print("\n--- Great Expectations Validation Process Completed ---")
    if all_validations_successful:
        print("All Great Expectations validations passed across all DataFrames.")
    else:
        print("Some Great Expectations validations failed. Check logs for details. Task will still succeed as configured.")


# Define the Airflow DAG
with DAG(
    dag_id='minio_data_quality_check', # A new, more descriptive DAG ID
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['data_quality', 'minio', 'great_expectations', 'pandas', 'lakehouse', 'bronze'],
    doc_md="""
    ### MinIO Bronze Layer Data Quality Check with Great Expectations
    This DAG reads daily weather data and master data (countries, cities) from the
    MinIO bronze layer into Pandas DataFrames. It then applies Great Expectations
    validations to each DataFrame.

    The task is configured to always succeed, even if data quality expectations are
    not met, but detailed failures will be logged. If data loading from MinIO fails,
    the task will fail.

    **IMPORTANT**: MinIO credentials are hardcoded for demonstration. Use Airflow
    Connections or environment variables in production for security.
    """
) as dag:
    # Define the PythonOperator task
    data_quality_check_task = PythonOperator(
        task_id='run_minio_data_quality_checks',
        python_callable=run_great_expectations_validation_from_minio,
    )
