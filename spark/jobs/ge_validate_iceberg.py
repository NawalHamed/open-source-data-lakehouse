from pyspark.sql import SparkSession
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import EphemeralDataContext
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.validator.validator import Validator

def validate_iceberg_data():
    # Initialize Spark with Iceberg configuration
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
        # Load Iceberg table
        df = spark.table("nessie.silver_layer.flight_data")

        # Initialize Ephemeral DataContext with updated configuration for GE 1.1.0
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
                "validation_results_store": {  # Updated name and class for GE 1.1.0
                    "class_name": "ValidationResultsStore",
                    "store_backend": {
                        "class_name": "InMemoryStoreBackend"
                    }
                },
                "evaluation_parameter_store": {
                    "class_name": "EvaluationParameterStore"
                }
            },
            "expectations_store_name": "expectations_store",
            "validations_store_name": "validation_results_store",  # Updated to match store name
            "evaluation_parameter_store_name": "evaluation_parameter_store",
            "data_docs_sites": {},
            "anonymous_usage_statistics": {
                "enabled": False
            },
            "checkpoint_store_name": None,
            "profiler_store_name": None
        })

        # Create expectation suite
        suite = context.add_expectation_suite("flight_data_expectations")

        # Create batch request
        batch_request = RuntimeBatchRequest(
            datasource_name="spark_datasource",
            data_connector_name="default_runtime_data_connector",
            data_asset_name="flight_data_asset",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"run_id": "flight_validation_1"}
        )

        # Create validator
        validator: Validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite=suite
        )

        # Define expectations
        validator.expect_column_values_to_not_be_null("flight_id")
        validator.expect_column_values_to_be_in_set(
            "status", 
            ["scheduled", "departed", "landed", "delayed", "cancelled"]
        )
        validator.expect_column_values_to_match_regex(
            "flight_number",
            r"^[A-Z]{2}\d{3,4}$"  # Example: AA123 or BA1234
        )

        # Save expectations (optional)
        validator.save_expectation_suite(discard_failed_expectations=False)

        # Run validation
        results = validator.validate()
        
        # Return comprehensive results
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
        return {
            "error": str(e),
            "success": False
        }
    finally:
        spark.stop()

if __name__ == "__main__":
    validation_results = validate_iceberg_data()
    print(validation_results)
