import pandas as pd
import boto3
from datetime import date
import logging



logging.basicConfig(filename='extract.log', level=logging.INFO)

df = pd.read_json(r"C:\Users\Raj bandaru\Desktop\project 1\simulated_api_data.json")


df["date_pulled"] = date.today().isoformat()


file_name = f"raw_{date.today().isoformat()}.json"
df.to_json(file_name, orient="records", lines=False)


s3 = boto3.client('s3')

bucket = 'data-project-rajjj'  
s3_path = f"raw/{file_name}"

# Upload file to S3
s3.upload_file(file_name, bucket, s3_path)

# Log success
logging.info(f"Uploaded {file_name} to s3://{bucket}/{s3_path} with {len(df)} rows on {date.today()}")

print(f"Upload complete: s3://{bucket}/{s3_path}")
