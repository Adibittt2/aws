import sys
import json
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# ==========================
# 1️⃣ Resolve Glue Arguments
# ==========================

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "raw_bucket",
        "raw_key",
        "target_bucket",
        "target_prefix"
    ]
)

# ==========================
# 2️⃣ Initialize Glue Context
# ==========================

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

print("Job started successfully")

# ==========================
# 3️⃣ Read Raw Data from S3
# ==========================

raw_path = f"s3://{args['raw_bucket']}/{args['raw_key']}"

print(f"Reading data from: {raw_path}")

df = spark.read.option("header", True).csv(raw_path)

print("Schema of input data:")
df.printSchema()

# ==========================
# 4️⃣ Basic Transformation
# Example: OMOP-style mapping
# ==========================

# Example transformation:
# Convert person_id to integer
if "person_id" in df.columns:
    df = df.withColumn(
        "person_id",
        F.col("person_id").cast(IntegerType())
    )

# Add audit column
df = df.withColumn("etl_processed_timestamp", F.current_timestamp())

# Remove duplicate rows
df = df.dropDuplicates()

print("Transformation complete")

# ==========================
# 5️⃣ Write to Target S3
# ==========================

target_path = f"s3://{args['target_bucket']}/{args['target_prefix']}"

print(f"Writing output to: {target_path}")

(
    df.write
    .mode("overwrite")
    .format("parquet")
    .save(target_path)
)

print("Data successfully written")

# ==========================
# 6️⃣ Commit Glue Job
# ==========================

job.commit()

print("Job committed successfully")
