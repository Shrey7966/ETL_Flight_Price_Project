import requests
import pandas as pd
import boto3
import os
from datetime import datetime
import json

# STEP -1  Load environment variables
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")


# Fetch date (when we are collecting data)
fetch_date = datetime.today().strftime('%Y-%m-%d')

# Depart date (when the flight is scheduled)
depart_date = "2025-05-31"

#Step-2 Call Flights API 
url = "https://flights-sky.p.rapidapi.com/flights/search-one-way"

querystring = {"fromEntityId":"BLR",
               "toEntityId":"DFW",
               "departDate":depart_date,
               "market":"IN",
               "locale":"en-US",
               "currency":"INR",
               "stops":"direct,1stop,2stops",
               "cabinClass":"economy",
               "adults":"1",
               "sort":"cheapest_first"}

headers = {
	"x-rapidapi-key": "fdcf5105b0mshb60125cb25ee57ep1565acjsndd740fc93e14",
	"x-rapidapi-host": "flights-sky.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)
data = response.json()

#Step-3 Store the response in s3 bucket

s3_key = f"flight_prices/{fetch_date}/{depart_date}.json" # New path format
    
s3 = boto3.client("s3")
s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=json.dumps(data))
    
print(f"Saved data to s3://{S3_BUCKET}/{s3_key}")
