from pyspark.sql import SparkSession
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import EphemeralDataContext
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.validator.validator import Validator

def validate_iceberg_data():
    # Initialize SparkSession with Iceberg & MinIO settings
    spark = SparkSession.builder \
        .appName("GE Iceberg Validation") \
        .master("spark://spark-master:7077") \
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
        # Load data from Iceberg table
        df = spark.table("nessie.silver_layer.flight_data")

        # Configure ephemeral GE context (in-memory only)
        context = EphemeralDataContext(project_config={
            "config_version": 3.0,
            "datasources": {
                "spark_datasource": {
                    "class_name": "Datasource",
                    "execution_engine": {
                        "class_name": "SparkDFExecutionEngine",
                        "force_reuse_spark_context": True
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
                    "store_backend": {
                        "class_name": "InMemoryStoreBackend"
                    }
                },
                "validation_results_store": {
                    "class_name": "ValidationResultsStore",
                    "store_backend": {
                        "class_name": "InMemoryStoreBackend"
                    }
                }
            },
            "expectations_store_name": "expectations_store",
            "validations_store_name": "validation_results_store",
            "anonymous_usage_statistics": {
                "enabled": False
            }
        })

        # ⚠️ Don't call create_expectation_suite → use this instead:
        suite = ExpectationSuite(expectation_suite_name="flight_data_expectations")

        batch_request = RuntimeBatchRequest(
            datasource_name="spark_datasource",
            data_connector_name="default_runtime_data_connector",
            data_asset_name="flight_data_asset",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"run_id": "flight_validation_1"}
        )

        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite=suite
        )

        # Add expectations
        validator.expect_column_values_to_not_be_null("flight_id")
        validator.expect_column_values_to_be_in_set(
            "status", ["scheduled", "departed", "landed", "delayed", "cancelled"]
        )
        validator.expect_column_values_to_match_regex(
            "flight_number", r"^[A-Z]{2}\d{3,4}$"
        )

        # Run validation
        results = validator.validate()

        return {
            "success": results.success,
            "statistics": results.statistics,
            "results": [{
                "expectation_type": r.expectation_config.expectation_type,
                "success": r.success,
                "result": r.result
            } for r in results.results]
        }

    except Exception as e:
        return {"success": False, "error": str(e)}

    finally:
        spark.stop()

if __name__ == "__main__":
    print(validate_iceberg_data())
