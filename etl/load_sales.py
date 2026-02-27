import pandas as pd
from etl.db_connection import get_engine

def load_sales():
    print("Loading Superstore sales data...")

    df = pd.read_csv(
        "data/rawdata/Sample - Superstore.csv",
        encoding="latin1"
    )

    # Clean column names properly
    df.columns = (
        df.columns
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )

    # Convert date columns
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["ship_date"] = pd.to_datetime(df["ship_date"])

    engine = get_engine()

    df.to_sql(
        name="staging_sales",
        con=engine,
        if_exists="append",
        index=False
    )

    print("Sales data loaded successfully.")