import requests
import pandas as pd
import json
from datetime import datetime
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv
import os
from sqlalchemy.dialects.mssql import NVARCHAR

# -----------------------------
# 1️⃣ 設定資料庫連線
# -----------------------------


# Load variables from .env file
load_dotenv()
server = os.getenv("DBSERVER")
userName = os.getenv("DBUSER")
password = os.getenv("DBPWD")
database = os.getenv("DATABASE")


odbc_str = (
    "DRIVER={ODBC Driver 17 for SQL Server}"
    + ";SERVER="
    + server
    + ";DATABASE="
    + database
    + ";UID="
    + userName
    + ";PWD="
    + password
)

params = quote_plus(odbc_str)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# -----------------------------
# 2️⃣ 下載資料
# -----------------------------
url = "https://newdatacenter.taichung.gov.tw/api/v1/no-auth/resource.download?rid=ed5ef436-fb62-40ba-9ad7-a165504cd953"


def fetch_data():
    print("=====extract=====")
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"API request failed: {response.status_code}")
    return response


# -----------------------------
# 3️⃣ 解析資料並處理 dict 欄位
# -----------------------------
def transform(response):
    print("=====transform=====")
    data = response.json()
    batch_time = data["updated_at"]
    records = json.loads(data["retVal"])
    df = pd.DataFrame(records)

    # 加 batch_id 與 ingest_time
    df["batch_id"] = pd.to_datetime(batch_time)
    df["ingest_time"] = pd.Timestamp.now()

    # dict 欄位轉 JSON string
    dict_cols = ["act", "sbi_detail"]
    for col in dict_cols:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: json.dumps(x) if isinstance(x, dict) else x
            )

    return df, batch_time


# -----------------------------
# 4️⃣ 檢查增量 / 寫入 SQL Server
# -----------------------------
def load_incremental(df, batch_time):
    print("=====load (incremental)=====")

    # 先檢查 batch_id 是否已存在
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT COUNT(1) FROM taichungyoubike_data WHERE batch_id = :batch_id"
            ),
            {"batch_id": batch_time},
        ).scalar()

    if result > 0:
        print(f"Batch {batch_time} already exists. Skipping insert.")
        return

    # 為所有 object 欄位指定 NVARCHAR
    dtype_mapping = {
        col: NVARCHAR(length=4000)
        for col in df.select_dtypes(include=["object", "str"]).columns
    }

    # 寫入資料
    df.to_sql(
        "taichungyoubike_data",
        engine,
        if_exists="append",
        index=False,
        dtype=dtype_mapping,
    )  # 或 replace
    print(f"{len(df)} rows inserted for batch {batch_time}.")


# -----------------------------
# 5️⃣ Pipeline
# -----------------------------
def run_pipeline():
    response = fetch_data()
    df, batch_time = transform(response)
    load_incremental(df, batch_time)


# -----------------------------
# 6️⃣ 執行
# -----------------------------
if __name__ == "__main__":
    run_pipeline()
