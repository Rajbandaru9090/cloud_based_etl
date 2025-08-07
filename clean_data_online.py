import pandas as pd
import numpy as np

data=pd.read_csv(r"C:\Users\Raj bandaru\Desktop\project 1\my_project\olist_orders_dataset.csv")
df=pd.DataFrame(data)
date_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]
for col in date_cols:
    df[col] = pd.to_datetime(df[col])


print("Nulls:\n", df.isnull().sum())

df = df.dropna(subset=["order_purchase_timestamp"])

df["order_delivered_customer_date"].fillna(pd.NaT, inplace=True)
    
df["order_status"] = df["order_status"].str.lower().str.strip()

df = df[df["order_estimated_delivery_date"] >= df["order_purchase_timestamp"]]

df.to_csv("orders.csv", index=False)



