from pyspark.sql import SparkSession
from great_expectations.dataset import SparkDFDataset

def validate_dummy_data():
    # Step 1: Initialize Spark
    spark = SparkSession.builder \
        .appName("GE Dummy Data Validation") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    # Step 2: Create Dummy DataFrame
    data = [
        ("John", 28, "john@example.com"),
        ("Alice", 34, "alice@example.com"),
        ("Bob", None, "bob@example.com"),
        ("Charlie", 45, None),
        ("David", 29, "david@example.com")
    ]
    columns = ["name", "age", "email"]

    df = spark.createDataFrame(data, columns)

    # Step 3: Wrap with Great Expectations SparkDFDataset
    ge_df = SparkDFDataset(df)

    # Step 4: Define Expectations
    ge_df.expect_column_to_exist("email")
    ge_df.expect_column_values_to_not_be_null("name")
    ge_df.expect_column_values_to_be_between("age", min_value=18, max_value=60)
    ge_df.expect_column_values_to_match_regex("email", r"^[^@]+@[^@]+\.[^@]+$", mostly=0.9)

    # Step 5: Validate and Collect Results
    validation_results = ge_df.validate()

    # Step 6: Print the Results
    import json
    print(json.dumps(validation_results, indent=2))

    # Stop Spark
    spark.stop()

if __name__ == "__main__":
    validate_dummy_data()
