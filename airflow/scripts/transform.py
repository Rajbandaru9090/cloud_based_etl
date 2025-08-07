import os
import pandas as pd
import boto3
from datetime import date
import logging

# Ensure logs folder exists
os.makedirs("logs", exist_ok=True)

# Setup logging
logging.basicConfig(filename='logs/transform.log', level=logging.INFO)

# S3 details
bucket = 'data-project-rajjj'  
raw_file = f"raw/raw_{date.today().isoformat()}.json"
clean_file = f"clean/clean_{date.today().isoformat()}.csv"

s3 = boto3.client('s3')

local_raw = f"raw_{date.today().isoformat()}.json"
s3.download_file(bucket, raw_file, local_raw)

# Load into DataFrame
df = pd.read_json(local_raw)

# Clean data
df = df.dropna()
if 'price' in df.columns and 'quantity' in df.columns:
    df["revenue"] = df["price"] * df["quantity"]




local_clean = f"clean_{date.today().isoformat()}.csv"
df.to_csv(local_clean, index=False)

# Upload cleaned file to S3
s3.upload_file(local_clean, bucket, clean_file)


logging.info(f"Uploaded cleaned file to s3://{bucket}/{clean_file} with {len(df)} rows on {date.today()}")

print(f"Transform complete: s3://{bucket}/{clean_file}")
