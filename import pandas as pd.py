import streamlit as st
import pandas as pd
import boto3
import json
from sqlalchemy import create_engine

# ----------------------------
# 🔐 Fetch credentials from AWS Secrets Manager
# ----------------------------
def get_db_credentials(secret_name, region):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region)

    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response['SecretString'])
    return secret

# Secret & region
SECRET_NAME = "rds!db-ef6dbf42-82c0-4ad0-b878-4fe309f88270"
REGION = "eu-north-1"

# Load credentials from Secrets Manager
creds = get_db_credentials(SECRET_NAME, REGION)

# Database connection info
host = "myproject1.clc8eu6ssz1t.eu-north-1.rds.amazonaws.com"
port = 5432
dbname = "postgres"
username = creds["username"]
password = creds["password"]

# Create SQLAlchemy connection string
conn_str = f"postgresql://{username}:{password}@{host}:{port}/{dbname}"
engine = create_engine(conn_str)
