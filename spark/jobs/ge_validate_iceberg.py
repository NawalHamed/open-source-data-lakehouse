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

        # Step 3: Create Ephemeral GE context with proper datasource configuration
        context = EphemeralDataContext(project_config={
            "config_version": 3.0,
            "datasources": {
                "spark_datasource": {
                    "class_name": "Datasource",
                    "execution_engine": {
                        "class_name": "SparkDFExecutionEngine",
                        "force_reuse_spark_context": True,
                        "spark_config": {
                            "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog",
                            "spark.sql.catalog.nessie.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
                            "spark.sql.catalog.nessie.uri": "http://nessie:19120/api/v1",
                            "spark.sql.catalog.nessie.ref": "main",
                            "spark.sql.catalog.nessie.warehouse": "s3a://lakehouse/",
                            "spark.hadoop.fs.s3a.endpoint": "http://minio:9009",
                            "spark.hadoop.fs.s3a.access.key": "minioadmin",
                            "spark.hadoop.fs.s3a.secret.key": "minioadmin",
                            "spark.hadoop.fs.s3a.path.style.access": "true",
                            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false"
                        }
                    },
                    "data_connectors": {
                        "default_runtime_data_connector": {
                            "class_name": "RuntimeDataConnector",
                            "batch_identifiers": ["run_id"]
                        }
                    }
                }
            },
            "stores": {
                "expectations_store": {
                    "class_name": "ExpectationsStore",
                    "store_backend": {"class_name": "InMemoryStoreBackend"}
                },
                "validation_results_store": {
                    "class_name": "ValidationResultsStore",
                    "store_backend": {"class_name": "InMemoryStoreBackend"}
                }
            },
            "expectations_store_name": "expectations_store",
            "validation_results_store_name": "validation_results_store",
            "anonymous_usage_statistics": {"enabled": False}
        })

        # Debug: Verify datasource is properly registered
        print("✅ Datasources in context:", context.list_datasources())
        print("✅ Available data connectors:", 
              context.datasources["spark_datasource"].get_available_data_connector_names())

        # Step 4: Define expectation suite in memory
        suite = ExpectationSuite(name="flight_data_expectations")

        # Step 5: Build batch request from Spark DataFrame
        batch_request = RuntimeBatchRequest(
            datasource_name="spark_datasource",
            data_connector_name="default_runtime_data_connector",
            data_asset_name="flight_data_asset",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"run_id": "flight_validation_1"}
        )

        # Step 6: Get validator and run expectations
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite=suite
        )

        # Add expectations
        validator.expect_column_values_to_not_be_null("flight_id")
        validator.expect_column_values_to_be_in_set(
            "status", ["scheduled", "departed", "landed", "delayed", "cancelled"]
        )

        # Step 7: Run validation
        results = validator.validate()

        # Step 8: Return or log results
        print("✅ Validation Complete")
        return {
            "success": results.success,
            "statistics": results.statistics,
            "results": [str(result) for result in results.results]
        }

    except Exception as e:
        print(f"❌ Error during validation: {str(e)}")
        raise
    finally:
        print("🧹 Stopping Spark...")
        spark.stop()


if __name__ == "__main__":
    validation_results = validate_iceberg_data()
    print("📊 Validation Results:")
    print(validation_results)
