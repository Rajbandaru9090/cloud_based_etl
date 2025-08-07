import boto3
import pandas as pd
import json
from sqlalchemy import create_engine
from datetime import date

# 🔐 Fetch secret from Secrets Manager
def get_db_credentials(secret_name, region_name):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    return json.loads(get_secret_value_response['SecretString'])

# 👉 Your actual secret name and region
secret_name = "rds!db-ef6dbf42-82c0-4ad0-b878-4fe309f88270"
region = "eu-north-1"
creds = get_db_credentials(secret_name, region)

# 🧠 Set missing info manually
host = "myproject1.clc8eu6ssz1t.eu-north-1.rds.amazonaws.com"  # 🔁 FROM AWS RDS console
port = 5432
dbname = "postgres"

# Extract from secret
username = creds["username"]
password = creds["password"]

# 🧬 Create SQLAlchemy connection string
conn_str = f"postgresql://{username}:{password}@{host}:{port}/{dbname}"
engine = create_engine(conn_str)

# 📂 Load cleaned CSV
csv_file = f"clean_{date.today().isoformat()}.csv"
df = pd.read_csv(csv_file)

# 🚀 Load to RDS PostgreSQL
df.to_sql("sales_data", engine, if_exists="replace", index=False)
print("✅ Successfully loaded data to RDS PostgreSQL!")
