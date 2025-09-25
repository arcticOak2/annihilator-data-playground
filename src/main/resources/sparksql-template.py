#!/usr/bin/env python3
# SparkSQL Query Execution Script
# Generated on: ${timestamp}
# Playground ID: ${playgroundId}
# Query ID: ${queryId}
# Unique ID: ${uniqueId}

import os
import sys
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"

print("=== Environment Check ===")
print("Python executable:", sys.executable)
print("Python version:", sys.version)
print("PYSPARK_PYTHON:", os.environ.get("PYSPARK_PYTHON"))

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("DataPhantomSparkSQL") \
        .enableHiveSupport() \
        .config("spark.hadoop.hive.enforce.bucketing", "true") \
        .config("spark.hadoop.hive.enforce.sorting", "true") \
        .getOrCreate()

    try:
        print("Starting SparkSQL query execution - Playground: ${playgroundId}, Query: ${queryId}")

        # Execute the SQL query
        print("Executing SparkSQL query...")
        print("Query: ${query}")

        # Execute the query
        df = spark.sql("${query}")

        # Show schema without forcing full execution
        print("Schema:")
        df.printSchema()

        print("Query executed successfully - processing results...")

        # Write results directly to S3
        s3_output_path = "s3://${outputBucket}/${pathPrefix}/sparksql-output/${currentDate}/${playgroundId}/${queryId}/${uniqueId}/"
        print(f"Writing results directly to S3: {s3_output_path}")

        # Write DataFrame directly to S3 as CSV file
        df.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .option("sep", "\t") \
            .csv(s3_output_path)

        print(f"Successfully wrote results to S3: {s3_output_path}")

        print("Sample data:")
        try:
            df.show(5, truncate=False)
        except Exception as show_error:
            print(f"Could not show sample data: {show_error}")

        print("SparkSQL query executed successfully!")

    except Exception as e:
        print(f"Error in SparkSQL query: {e}")
        print("Full traceback:")
        traceback.print_exc()
        sys.exit(1)
    finally:
        try:
            spark.stop()
        except Exception as stop_err:
            print(f"Warning: Could not stop Spark session cleanly: {stop_err}")


if __name__ == "__main__":
    main()
