from etl.load_sales import load_sales
from etl.load_to_query import load_to_query

def run_etl():
    print("Starting ETL process...")
    load_sales()
    load_to_query()
    print("Full ETL completed successfully.")

if __name__ == "__main__":
    run_etl()