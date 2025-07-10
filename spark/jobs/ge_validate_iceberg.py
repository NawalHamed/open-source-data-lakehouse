#!/usr/bin/env python3
from pyspark.sql import SparkSession
from great_expectations import get_context
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
    DatasourceConfig,
    DataConnectorConfig,
    SparkDFExecutionEngineConfig
)

import great_expectations as ge
print("GE Version:", ge.__version__)
print("Spark Version:", spark.version)


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

        # Step 3: Create GE context with proper configuration
        store_backend_defaults = InMemoryStoreBackendDefaults()
        data_context_config = DataContextConfig(
            store_backend_defaults=store_backend_defaults,
            datasources={
                "spark_datasource": DatasourceConfig(
                    class_name="Datasource",
                    execution_engine=SparkDFExecutionEngineConfig(force_reuse_spark_context=True),
                    data_connectors={
                        "default_runtime_data_connector": DataConnectorConfig(
                            class_name="RuntimeDataConnector",
                            batch_identifiers=["run_id"]
                        )
                    }
                )
            },
            anonymous_usage_statistics={"enabled": False}
        )
        
        context = get_context(project_config=data_context_config)

        # Verify datasource is properly registered
        print("✅ Datasources in context:", context.list_datasources())
        
        # Step 4: Define expectation suite
        suite = context.create_expectation_suite("flight_data_expectations", overwrite_existing=True)

        # Step 5: Build batch request
        batch_request = RuntimeBatchRequest(
            datasource_name="spark_datasource",
            data_connector_name="default_runtime_data_connector",
            data_asset_name="flight_data_asset",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"run_id": "flight_validation_1"}
        )

        # Step 6: Get validator and add expectations
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite=suite
        )

        validator.expect_column_values_to_not_be_null("flight_id")
        validator.expect_column_values_to_be_in_set(
            "status", ["scheduled", "departed", "landed", "delayed", "cancelled"]
        )

        # Step 7: Run validation
        results = validator.validate()

        # Step 8: Return results
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
