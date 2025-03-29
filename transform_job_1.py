import boto3
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from functools import reduce


# Step-1 Load environment variables, Initialize Boto3 S3 client, connect s3 and get the response

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
)

# S3 bucket and prefix

s3_prefix = "flight_prices/"  # --> Folder containing all fetch dates

# List all fetch dates available in S3
response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=s3_prefix, Delimiter='/')

# Extract available fetch dates
fetch_dates = [obj['Prefix'].split('/')[-2] for obj in response.get('CommonPrefixes', [])]
print(fetch_dates)

# Initialize Spark session with S3 support
spark = SparkSession.builder \
    .appName("FlightPriceETL") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key",AWS_SECRET_KEY ) \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026") \
    .getOrCreate()

# Configure Spark to access S3
spark.conf.set("fs.s3a.endpoint", "s3.amazonaws.com")


# Read data for each fetch date dynamically
dfs = []
for fetch_date in fetch_dates:
    s3_path = f"s3a://{S3_BUCKET}/{s3_prefix}{fetch_date}/*.json"  # Load all departure dates under each fetch date
    print(f"Reading data from: {s3_path}")
    
    try:
        df = spark.read.json(s3_path)
        if df.head(1):  # Ensure DataFrame is not empty
            df = df.withColumn("fetch_date", lit(fetch_date))  # Add fetch_date as a column
            dfs.append(df)
        else:
            print(f" No data found at {s3_path}")
    except Exception as e:
        print(f"Error reading data for {fetch_date}: {str(e)}")

# Merge all DataFrames if any data was read
if dfs:
    flight_data = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), dfs)
else:
    print(" No valid data found in S3.")

flight_data.coalesce(1).write.mode("overwrite").json(f"s3a://{S3_BUCKET}/merged-flight-data")
