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

df_post = spark.read.csv(f"s3a://{S3_BUCKET}/flight_prices_toload.csv/", header=True, inferSchema=True)

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="flight-price-db-github",  # Default PostgreSQL database
    user="shreyas",
    password="SG7966.cgi",
    host="flight-price-db-github.chkqymm0yro0.eu-north-1.rds.amazonaws.com",
    port="5432"
)
cur = conn.cursor()

# Insert data into PostgreSQL table
for _, row in pandas_df.iterrows():
    fetch_date = row["fetch_date"]
    flight_number = row["flightNumber"]
    origin = row["origin"]
    destination = row["destination"]
    price = clean_price(row["formattedPrice"])  # Convert ₹ 51,898 → 51898
    departure_time = row["departure"]  # Ensure this is in correct datetime format
    duration = row["duration"]
    marketingCarrier = row["marketingCarrier"]
    operatingCarrier = row["operatingCarrier"]
    layover = row["layover"]
    numStops = row["numStops"]
   
cur.execute("SELECT current_database();")
print(cur.fetchone())

cur.execute("""
    INSERT INTO flight_prices (fetch_date, flight_number, origin, destination, price, departure_time, duration, marketingCarrier, operatingCarrier, layover, numStops)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""", (fetch_date, flight_number, origin, destination, price, departure_time, duration, marketingCarrier, operatingCarrier, layover, numStops))
conn.commit()
 


# Commit and close connection
conn.commit()
cur.close()
conn.close()
print("✅ Cleaned flight price data successfully stored in PostgreSQL!")
