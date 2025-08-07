import boto3
import pandas as pd
import json
from datetime import date
import logging

def main():
    logging.basicConfig(filename='logs/extract.log', level=logging.INFO)

    df = pd.read_json(r"/opt/airflow/dags/scripts/simulated_api_data.json")  # Update path if needed
    df["date_pulled"] = date.today().isoformat()
    
    file_name = f"raw_{date.today().isoformat()}.json"
    df.to_json(file_name, orient="records", lines=False)

    s3 = boto3.client('s3')
    bucket = 'data-project-rajjj'  
    s3_path = f"raw/{file_name}"
    s3.upload_file(file_name, bucket, s3_path)
    
    logging.info(f"Uploaded {file_name} to s3://{bucket}/{s3_path}")
    print(f"✅ Upload complete: s3://{bucket}/{s3_path}")
