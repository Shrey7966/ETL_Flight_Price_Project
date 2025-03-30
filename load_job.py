import requests
import pandas as pd
import boto3
import os
import re
import psycopg2
from pyspark.sql import SparkSession

# Load environment variables
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")

spark = SparkSession.builder \
    .appName("FlightPriceETL") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026") \
    .getOrCreate()

df_post = spark.read.csv(f"s3a://{S3_BUCKET}/flight_prices_toload.csv/", header=True, inferSchema=True)
pandas_df = df_post.toPandas()

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="flight-price-db-github",  # Default PostgreSQL database
    user="shreyas",
    password="SG7966.cgi",
    host="flight-price-db-github.chkqymm0yro0.eu-north-1.rds.amazonaws.com",
    port="5432"
)
cur = conn.cursor()

# Function to clean price (convert ₹ 51,898 → 51898)
def clean_price(price_str):
    return int(re.sub(r"[^\d]", "", price_str)) if isinstance(price_str, str) else price_str

# Insert data into PostgreSQL table
for _, row in pandas_df.iterrows():
    try:
        fetch_date = row["fetch_date"]
        flight_number = row["flightNumber"]
        origin = row["origin"]
        destination = row["destination"]
        price = clean_price(row["formattedPrice"])
        departure_time = row["departure"]
        duration = row["duration"]
        marketingCarrier = row["marketingCarrier"]
        operatingCarrier = row["operatingCarrier"]
        layover = row["layover"]
        numStops = row["numStops"]

# SQL query to create the table if it doesn't exist
create_table_query = """
CREATE TABLE IF NOT EXISTS flight_prices (
    id SERIAL PRIMARY KEY,
    fetch_date DATE NOT NULL,
    flight_number VARCHAR(50) NOT NULL,
    origin VARCHAR(10) NOT NULL,
    destination VARCHAR(10) NOT NULL,
    price INT,
    departure_time TIMESTAMP NOT NULL,
    duration INT,
    marketing_carrier VARCHAR(100),
    operating_carrier VARCHAR(100),
    layover VARCHAR(50),
    num_stops INT
);
"""

# Execute the query
cursor.execute(create_table_query)

    except Exception as e:
        print(f" Error inserting row {row}: {e}")

# Commit and close connection
conn.commit()
cur.close()
conn.close()

print("Cleaned flight price data successfully stored in PostgreSQL!")
