import boto3
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, unix_timestamp, lit, lead
from functools import reduce
from pyspark.sql.types import StringType


# Step-0 Load environment variables, Initialize Boto3 S3 client, connect s3 and get the response

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

merged_df = spark.read.json("s3://S3_BUCKET/merged-flight-data")

# Step 1: Flatten first-level struct fields
df_flattened = merged_df.select("fetch_date",
    explode(col("data.itineraries")).alias("itineraries")  # Exploding arrays
)

# Step 2: Extract fields from the exploded 'itineraries' struct
df_itineraries = df_flattened.select("fetch_date",
    col("itineraries.price.formatted").alias("formattedPrice"),  # Extract price
    explode(col("itineraries.legs")).alias("legs")  # Exploding legs
)

# Step 3: Extract fields from the 'legs' struct
df_legs = df_itineraries.select("fetch_date",
    "formattedPrice",  # Keep extracted price fields
    col("legs.departure").alias("departure"),
    col("legs.arrival").alias("arrival"),
    col("legs.durationInMinutes").alias("duration"),
    explode(col("legs.segments")).alias("segments")
)

# Step 4: Extract fields from the 'segments' struct
df_segments = df_legs.select("fetch_date",
    "formattedPrice",  # Keep price details
    "departure", "arrival", "duration",
    col("segments.flightNumber").alias("flightNumber"),
    col("segments.marketingCarrier.name").alias("marketingCarrier"),
    col("segments.operatingCarrier.name").alias("operatingCarrier"),
    col("segments.origin.displayCode").alias("originCode"),
    col("segments.destination.displayCode").alias("destinationCode")
)

# Step 5: Add a column to calculate layover duration (for consecutive legs of the same flight)
window_spec = Window.partitionBy("flightNumber").orderBy("departure")

df_with_layover = df_segments.withColumn(
    "next_departure", lead("departure").over(window_spec)
).withColumn(
    "next_arrival", lead("arrival").over(window_spec)
)

# Calculate layover duration in minutes
df_with_layover = df_with_layover.withColumn(
    "layover_duration",
    (unix_timestamp("next_departure") - unix_timestamp("arrival")) / 60
)

# Step 6: Merge layover rows (rows with the same flightNumber)
df_final = df_with_layover.groupBy("fetch_date",
   "flightNumber", "formattedPrice", "departure", "arrival", "duration", "marketingCarrier", 
    "operatingCarrier", "originCode", "destinationCode"
).agg(
    lit(0).alias("layover_duration")  # Add layover duration as 0 for non-layover rows
).withColumnRenamed("layover_duration", "layover")

