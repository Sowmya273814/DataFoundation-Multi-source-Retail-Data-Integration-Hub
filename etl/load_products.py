import pandas as pd
from etl.db_connection import get_engine

def load_products():
    print("Loading product data...")

    df = pd.read_csv("data/rawdata/dummy_retail_sales.csv")

    # Basic cleaning
    df = df.drop_duplicates()
    df = df.fillna(0)

    engine = get_engine()

    df.to_sql(
        name="staging_product",
        con=engine,
        if_exists="append",
        index=False
    )

    print("Product data loaded successfully.")