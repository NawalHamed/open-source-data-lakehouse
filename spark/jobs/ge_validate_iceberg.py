#!/usr/bin/env python3
from pyspark.sql import SparkSession
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import EphemeralDataContext
from great_expectations.core.expectation_suite import ExpectationSuite

def validate_iceberg_data():
    # Step 1: Initialize Spark with Iceberg, Nessie, and MinIO configs
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
        # Step 2: Load Iceberg table from Nessie catalog
        df = spark.table("nessie.silver_layer.flight_data")

        # Step 3: Print schema and row count to ensure DataFrame is valid
        print("=== Schema ===")
        df.printSchema()
        print("=== Row Count ===")
        print(df.count())

        # Step 4: Configure Ephemeral GE DataContext
        context = EphemeralDataContext(project_config={
            "config_version": 3.0,
            "datasources": {
                "spark_datasource": {
                    "class_name": "Datasource",
                    "module_name": "great_expectations.datasource",
                    "execution_engine": {
                        "class_name": "SparkDFExecutionEngine",
                        "module_name": "great_expectations.execution_engine",
                        "force_reuse_spark_context": True
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
            "validations_store_name": "validation_results_store",
            "anonymous_usage_statistics": {"enabled": False}
        })

        print("✅ GE Datasource loaded:", context.datasources)

        # Step 5: Create expectation suite
        suite = ExpectationSuite(name="flight_data_expectations")

        # Step 6: Build RuntimeBatchRequest using in-memory Spark DataFrame
        batch_request = RuntimeBatchRequest(
            datasource_name="spark_datasource",
            data_connector_name="default_runtime_data_connector",
            data_asset_name="flight_data_asset",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"run_id": "flight_validation_1"}
        )

        # Step 7: Get validator and attach suite
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite=suite
        )

        # Step 8: Define expectations
        validator.expect_column_values_to_not_be_null("flight_id")
        validator.expect_column_values_to_be_in_set(
            "status", ["scheduled", "departed", "landed", "delayed", "cancelled"]
        )

        # Step 9: Validate and print results
        results = validator.validate()

        print("✅ Validation complete.")
        return {
            "success": results.success,
            "statistics": results.statistics,
            "results": [str(result) for result in results.results]
        }

    finally:
        print("🧹 Stopping Spark...")
        spark.stop()

if __name__ == "__main__":
    validation_results = validate_iceberg_data()
    print("📊 Validation Results:")
    print(validation_results)
