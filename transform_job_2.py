import boto3
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, unix_timestamp, lead, array_except, array, size, split, when, array_join, first, last, collect_list,lit
from functools import reduce
from pyspark.sql.window import Window
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


merged_df_test= spark.read.json(f"s3a://{S3_BUCKET}/merged-flight-data")

# Flatten JSON: Extract Itineraries
df_flattened = merged_df_test.select(
    col("fetch_date"),
    explode(col("data.itineraries")).alias("itinerary")
)

# Extract Price & Legs
df_itineraries = df_flattened.select(
    col("fetch_date"),
    col("itinerary.price.formatted").alias("formattedPrice"),
    explode(col("itinerary.legs")).alias("legs")
)

# Extract Flight Segment Details
df_segments = df_itineraries.select(
    col("fetch_date"),
    col("formattedPrice"),
    col("legs.departure").alias("departure"),
    col("legs.arrival").alias("arrival"),
    col("legs.durationInMinutes").alias("duration"),
    explode(col("legs.segments")).alias("segment")
).select(
    col("fetch_date"),
    col("formattedPrice"),
    col("departure"),
    col("arrival"),
    col("duration"),
    col("segment.flightNumber").alias("flightNumber"),
    col("segment.marketingCarrier.name").alias("marketingCarrier"),
    col("segment.operatingCarrier.name").alias("operatingCarrier"),
    col("segment.origin.displayCode").alias("origin"),
    col("segment.destination.displayCode").alias("destination")
)

# Window Spec for Layover Calculation
window_spec = Window.partitionBy("fetch_date", "flightNumber").orderBy("departure")

# Add Layover Information
df_with_layover = df_segments.withColumn(
    "next_departure", lead("departure").over(window_spec)
).withColumn(
    "next_arrival", lead("arrival").over(window_spec)
).withColumn(
    "layover_duration",
    (unix_timestamp("next_departure") - unix_timestamp("arrival")) / 60
)

# Group Flights & Calculate Layovers
df_grouped = df_segments.groupBy("fetch_date", "formattedPrice", "departure") \
    .agg(
        first("origin").alias("origin"),
        last("destination").alias("destination"),
        collect_list("destination").alias("layovers"),
        first("duration").alias("duration"),
        first("marketingCarrier").alias("marketingCarrier"),
        first("operatingCarrier").alias("operatingCarrier"),
        first("flightNumber").alias("flightNumber")
    ).withColumn(
        "layover",
        array_except(col("layovers"), array(col("origin"), col("destination")))
    ).withColumn(
        "layover", array_join(col("layover"), ", ")
    ).withColumn(
        "numStops", when(col("layover") == "", 0).otherwise(size(split(col("layover"), ", ")))
    )

# Select Final Columns
df_final = df_grouped.select(
    "fetch_date", "flightNumber", "formattedPrice", "departure", "duration",
    "marketingCarrier", "operatingCarrier", "origin", "destination", "layover", "numStops"
)

# Save Cleaned Data to S3
df_final.write.mode("overwrite").csv(f"s3a://{S3_BUCKET}/flight_prices_toload.csv", header=True)

df_final.show(n=1000, truncate=False)
