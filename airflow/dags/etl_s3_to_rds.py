from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import pandas as pd
import boto3
import json
from sqlalchemy import create_engine
from datetime import date
import logging

# =====================
# 🔐 Reusable credentials logic
# =====================
def get_db_credentials():
    secret_name = "rds!db-ef6dbf42-82c0-4ad0-b878-4fe309f88270"
    region = "eu-north-1"
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region)
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    return json.loads(get_secret_value_response['SecretString'])

# =====================
# 1. Extract
# =====================
def extract_from_api_to_s3():
    logging.basicConfig(filename='logs/extract.log', level=logging.INFO)
    df = pd.read_json('/opt/airflow/scripts/simulated_api_data.json')
    df["date_pulled"] = date.today().isoformat()

    file_name = f"raw_{date.today().isoformat()}.json"
    df.to_json(file_name, orient="records", lines=False)

    s3 = boto3.client('s3')
    bucket = 'data-project-rajjj'
    s3_path = f"raw/{file_name}"
    s3.upload_file(file_name, bucket, s3_path)
    logging.info(f"Uploaded {file_name} to s3://{bucket}/{s3_path}")

# =====================
# 2. Transform
# =====================
def transform_clean_and_upload():
    logging.basicConfig(filename='logs/transform.log', level=logging.INFO)
    bucket = 'data-project-rajjj'
    raw_file = f"raw/raw_{date.today().isoformat()}.json"
    clean_file = f"clean/clean_{date.today().isoformat()}.csv"

    s3 = boto3.client('s3')
    local_raw = f"raw_{date.today().isoformat()}.json"
    s3.download_file(bucket, raw_file, local_raw)

    df = pd.read_json(local_raw)
    df = df.dropna()

    if 'price' in df.columns and 'quantity' in df.columns:
        df["revenue"] = df["price"] * df["quantity"]

    local_clean = f"clean_{date.today().isoformat()}.csv"
    df.to_csv(local_clean, index=False)
    s3.upload_file(local_clean, bucket, clean_file)
    logging.info(f"Uploaded cleaned file to s3://{bucket}/{clean_file}")

# =====================
# 3. Load to RDS
# =====================
def load_to_rds():
    creds = get_db_credentials()
    host = "myproject1.clc8eu6ssz1t.eu-north-1.rds.amazonaws.com"
    port = 5432
    dbname = "postgres"
    username = creds["username"]
    password = creds["password"]

    conn_str = f"postgresql://{username}:{password}@{host}:{port}/{dbname}"
    engine = create_engine(conn_str)

    csv_file = f"clean_{date.today().isoformat()}.csv"
    df = pd.read_csv(csv_file)
    df.to_sql("sales_data", engine, if_exists="replace", index=False)

# =====================
# DAG Definition
# =====================
with DAG(
    dag_id='etl_s3_to_rds',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'project1'],
) as dag:

    t1 = PythonOperator(
        task_id='extract_api_data',
        python_callable=extract_from_api_to_s3
    )

    t2 = PythonOperator(
        task_id='transform_data',
        python_callable=transform_clean_and_upload
    )

    t3 = PythonOperator(
        task_id='load_data_to_rds',
        python_callable=load_to_rds
    )

    t1 >> t2 >> t3
