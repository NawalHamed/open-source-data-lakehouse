#!/usr/bin/env python3
from pyspark.sql import SparkSession
from great_expectations.data_context import EphemeralDataContext
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.expectation_suite import ExpectationSuite


def validate_iceberg_data():
    # Step 1: Initialize Spark with Iceberg + Nessie + MinIO
    spark = SparkSession.builder \
        .appName("GE Iceberg Validation") \
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1") \
        .config("spark.sql.catalog.nessie.ref", "main") \
        .config("spark.sql.catalog.nessie.warehouse", "s3a://lakehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9009") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    try:
        # Step 2: Load Iceberg table as Spark DataFrame
        df = spark.table("nessie.silver_layer.flight_data")

        # Debug: Print schema and row count
        print("=== Schema ===")
        df.printSchema()
        print("=== Row Count ===")
        print(df.count())

        # Step 3: Create Ephemeral GE context
        context = EphemeralDataContext(project_config={
            "config_version": 3.0,
            "datasources": {
                "spark_datasource": {
                    "class_name": "Datasource",
                    "module_name": "great_expectations.datasource",
                    "execution_engine": {
                        "class_name": "SparkDFExecutionEngine",
                        "module_name": "great_expectations.execution_engine"
                    },
                    "data_connectors": {
                        "default_runtime_data_connector": {
                            "class_name": "RuntimeDataConnector",
                            "module_name": "great_expectations.datasource.data_connector",
                            "batch_identifiers": ["run_id"]
                        }
                    }
                }
            },
            "stores": {
                "expectations_store": {
                    "class_name": "ExpectationsStore",
                    "module_name": "great_expectations.data_context.store",
                    "store_backend": {"class_name": "InMemoryStoreBackend"}
                },
                "validation_results_store": {
                    "class_name": "ValidationResultsStore",
                    "module_name": "great_expectations.data_context.store",
                    "store_backend": {"class_name": "InMemoryStoreBackend"}
                }
            },
            "expectations_store_name": "expectations_store",
            "validation_results_store_name": "validation_results_store",
            "anonymous_usage_statistics": {"enabled": False}
        })

        # Optional debug: Check datasource was registered
        print("✅ Datasources in context:", context.list_datasources())

        # Step 4: Define expectation suite in memory
        suite = ExpectationSuite(name="flight_data_expectations")

        # Step 5: Build batch request from Spark DataFrame
        batch_request = RuntimeBatchRequest(
            datasource_name="spark_datasource",  # must match context
            data_connector_name="default_runtime_data_connector",
            d
