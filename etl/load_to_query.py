from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import pandas_gbq
from etl.db_connection import get_engine
from datetime import datetime

# --------------------------
# CONFIG
# --------------------------
PROJECT_ID = "multi-source-data-hub2738"
DATASET_ID = "retail_warehouse"
KEY_PATH = "C:/Users/sanga/OneDrive/Desktop/Retail_Data_Integration/config/multi-source-key.json"

# Dimensions to process
DIMENSIONS = {
    "dim_customer": {
        "columns": ["customer_id", "customer_name", "segment"],
        "scd2": True,
        "key_col": "customer_key"
    },
    "dim_product": {
        "columns": ["product_id", "product_name", "category"],  # removed 'brand'
        "scd2": True,
        "key_col": "product_key"
    },
    "dim_store": {
        "columns": ["store_id", "store_name", "city", "region"],
        "scd2": True,
        "key_col": "store_key"
    },
    "dim_date": {
        "columns": ["order_date"],
        "scd2": False,
        "key_col": "date_key"
    }
}

FACT_TABLE = {
    "name": "fact_sales",
    "columns": ["order_id", "order_date", "customer_id", "product_id", "store_id", "sales", "profit"]
}

# --------------------------
# ETL FUNCTION
# --------------------------
def load_to_bigquery():
    print("🚀 Starting BigQuery Star Schema Load...")

    # --------------------------
    # AUTHENTICATION
    # --------------------------
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

    # --------------------------
    # CHECK / CREATE DATASET
    # --------------------------
    dataset_ref = client.dataset(DATASET_ID)
    try:
        client.get_dataset(dataset_ref)
        print(f"✅ Dataset '{DATASET_ID}' already exists.")
    except Exception:
        print(f"⚠️ Dataset '{DATASET_ID}' not found. Creating dataset...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"✅ Dataset '{DATASET_ID}' created successfully.")

    # --------------------------
    # EXTRACT FROM MYSQL
    # --------------------------
    engine = get_engine()
    df = pd.read_sql("SELECT * FROM staging_sales", engine)
    print("✅ Rows fetched from MySQL:", len(df))
    df["order_date"] = pd.to_datetime(df["order_date"])
    today = pd.to_datetime(datetime.today().date())

    # --------------------------
    # LOAD DIMENSIONS
    # --------------------------
    dim_keys = {}  # store mapping of natural keys -> surrogate keys

    for dim_name, dim_info in DIMENSIONS.items():
        print(f"🔄 Processing {dim_name}...")

        # Use only columns that exist in df
        cols_in_df = [c for c in dim_info["columns"] if c in df.columns]
        if not cols_in_df:
            print(f"⚠️ Skipping {dim_name} because no columns found in source data")
            continue

        src = df[cols_in_df].drop_duplicates()
        key_col = dim_info["key_col"]

        if dim_info["scd2"]:
            # Check existing dimension in BigQuery
            try:
                existing_dim = pandas_gbq.read_gbq(
                    f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{dim_name}`",
                    project_id=PROJECT_ID,
                    credentials=credentials
                )
                print(f"Existing {dim_name} found:", len(existing_dim))
            except Exception:
                print(f"No existing {dim_name} found. Initial load.")
                existing_dim = pd.DataFrame()

            if existing_dim.empty:
                src = src.copy()
                src[key_col] = range(1, len(src) + 1)
                src["effective_date"] = today
                src["expiry_date"] = pd.NaT
                src["is_current"] = 1
                final_dim = src.copy()
            else:
                final_dim = existing_dim.copy()
                max_key = final_dim[key_col].max()
                for _, row in src.iterrows():
                    natural_id = row[cols_in_df[0]]  # first column as natural key
                    existing_record = final_dim[
                        (final_dim[cols_in_df[0]] == natural_id) &
                        (final_dim["is_current"] == 1)
                    ]
                    if existing_record.empty:
                        max_key += 1
                        new_row = row.to_dict()
                        new_row.update({
                            key_col: max_key,
                            "effective_date": today,
                            "expiry_date": pd.NaT,
                            "is_current": 1
                        })
                        final_dim = pd.concat([final_dim, pd.DataFrame([new_row])], ignore_index=True)
                    else:
                        existing_row = existing_record.iloc[0]
                        changed = any(existing_row[col] != row[col] for col in cols_in_df[1:])
                        if changed:
                            final_dim.loc[
                                final_dim[key_col] == existing_row[key_col],
                                ["expiry_date", "is_current"]
                            ] = [today, 0]
                            max_key += 1
                            new_row = row.to_dict()
                            new_row.update({
                                key_col: max_key,
                                "effective_date": today,
                                "expiry_date": pd.NaT,
                                "is_current": 1
                            })
                            final_dim = pd.concat([final_dim, pd.DataFrame([new_row])], ignore_index=True)
        else:
            # Non-SCD dimension (dim_date)
            final_dim = src.copy()
            if dim_name == "dim_date":
                final_dim["date_key"] = final_dim["order_date"].dt.strftime("%Y%m%d").astype(int)
                final_dim["year"] = final_dim["order_date"].dt.year
                final_dim["quarter"] = final_dim["order_date"].dt.quarter
                final_dim["month"] = final_dim["order_date"].dt.month
                final_dim["weekday"] = final_dim["order_date"].dt.weekday

        # Upload dimension
        pandas_gbq.to_gbq(
            final_dim,
            f"{DATASET_ID}.{dim_name}",
            project_id=PROJECT_ID,
            credentials=credentials,
            if_exists="replace"
        )
        print(f"✅ {dim_name} loaded successfully.")

        # Store key mapping for fact table
        if dim_info["scd2"]:
            dim_keys[cols_in_df[0]] = final_dim[[cols_in_df[0], key_col, "is_current"]].query("is_current==1").drop(columns="is_current")
        else:
            dim_keys[cols_in_df[0]] = final_dim[[cols_in_df[0], key_col]]

    # --------------------------
    # BUILD FACT TABLE
    # --------------------------
    print(f"🔄 Processing {FACT_TABLE['name']}...")

    # Use only columns that exist in df
    cols_in_fact = [c for c in FACT_TABLE["columns"] if c in df.columns]
    fact = df[cols_in_fact].copy()

    # Map surrogate keys
    for col, mapping in dim_keys.items():
        key_col = mapping.columns[1]
        if col in fact.columns:
            fact = fact.merge(mapping, on=col, how="left")
            fact.drop(columns=col, inplace=True)
            fact.rename(columns={key_col: col.replace("_id", "_key")}, inplace=True)

    # Upload fact table
    pandas_gbq.to_gbq(
        fact,
        f"{DATASET_ID}.{FACT_TABLE['name']}",
        project_id=PROJECT_ID,
        credentials=credentials,
        if_exists="replace"
    )
    print(f"✅ {FACT_TABLE['name']} loaded successfully.")
    print("🎉 BigQuery Star Schema Load Completed Successfully!")


if __name__ == "__main__":
    load_to_bigquery()