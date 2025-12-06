#Imports Python's system module to access command-line arguments.
import sys

#Imports all AWS Glue transformation functions (though none are explicitly used in this script).
from awsglue.transforms import *

#Imports utility to parse job parameters passed to the Glue job.
from awsglue.utils import getResolvedOptions

#Imports GlueContext, which wraps SparkContext and provides Glue-specific functionality.
from awsglue.context import GlueContext

#Imports Job class for managing Glue job lifecycle and bookmarking.
from awsglue.job import Job

#Imports DynamicFrame, Glue's data structure that handles schema variations better than Spark DataFrames.
from awsglue.dynamicframe import DynamicFrame

#Imports SparkContext, the entry point for Spark functionality.
from pyspark.context import SparkContext

#Imports PySpark SQL functions for data transformations.
from pyspark.sql.functions import (
    col, to_date, trim, upper, when, year, month
)

# ---------------------------------------------------------------------------------
# Initialize Glue Job
# ---------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---------------------------------------------------------------------------------
# Read raw CSV data from S3 using the Glue Data Catalog table
# ---------------------------------------------------------------------------------
raw_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="raw_data_catalog",
    table_name="orders_csv_table",   # update with your catalog table name
    transformation_ctx="raw_dyf"
)

# ---------------------------------------------------------------------------------
# Column Standardization
# ---------------------------------------------------------------------------------
df = raw_dyf.toDF()

# Standardize column names (Spark-friendly)
df = df.toDF(*[c.lower().replace(" ", "_") for c in df.columns])

# ---------------------------------------------------------------------------------
# Clean & Transform Data
# ---------------------------------------------------------------------------------

# Trim whitespace
for column in df.columns:
    df = df.withColumn(column, trim(col(column)))

# Convert datatypes
df = (
    df.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
      .withColumn("quantity", col("quantity").cast("int"))
      .withColumn("unit_price", col("unit_price").cast("double"))
)

# Remove invalid rows
df = df.filter(col("order_id").isNotNull() & col("customer_id").isNotNull())

# Fix negative values (replace with null or filter)
df = df.withColumn("quantity", when(col("quantity") < 0, None).otherwise(col("quantity")))
df = df.withColumn("unit_price", when(col("unit_price") < 0, None).otherwise(col("unit_price")))

# Create derived columns
df = df.withColumn("total_price", col("quantity") * col("unit_price"))
df = df.withColumn("order_year", year(col("order_date")))
df = df.withColumn("order_month", month(col("order_date")))

# Remove duplicates
df = df.dropDuplicates()

# ---------------------------------------------------------------------------------
# Convert back to DynamicFrame
# ---------------------------------------------------------------------------------
final_dyf = DynamicFrame.fromDF(df, glueContext, "final_dyf")

# ---------------------------------------------------------------------------------
# Write to Clean S3 Zone (Partitioned)
# ---------------------------------------------------------------------------------
output_path = "s3://my-data-clean-bucket/clean/orders/"  # update with your path

glueContext.write_dynamic_frame.from_options(
    frame=final_dyf,
    connection_type="s3",
    connection_options={
        "path": output_path,
        "partitionKeys": ["order_year", "order_month"]
    },
    format="parquet",
    transformation_ctx="datasink"
)

job.commit()
