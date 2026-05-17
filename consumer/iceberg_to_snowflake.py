import duckdb
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

duck_con = duckdb.connect()

duck_con.execute("INSTALL iceberg; LOAD iceberg;")

duck_con.execute("""
    SET s3_endpoint='localhost:9000';
    SET s3_access_key_id='minioadmin';
    SET s3_secret_access_key='minioadmin';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
    SET unsafe_enable_version_guessing=true;
""")

snow_con = snowflake.connector.connect(
    account='UIYTTRG-SI84205',
    user='TECHYNURSE',
    password='Sophy2002051120020511',
    warehouse='dev_adoption_wh',
    database='fintech_streaming',
    schema='raw'
)

def load_iceberg_to_snowflake():
    print("Reading from Iceberg...")
    
    df = duck_con.execute("""
        SELECT *
        FROM iceberg_scan('s3://iceberg-warehouse/fintech_db/transactions')
    """).fetchdf()
    
    print(f"Read {len(df)} transactions from Iceberg")
    
    df.columns = [col.upper() for col in df.columns]
    
    print("Writing to Snowflake...")
    
    success, nchunks, nrows, _ = write_pandas(
        snow_con,
        df,
        'TRANSACTIONS_RAW',
        auto_create_table=True,
        overwrite=True
    )
    
    print(f"Successfully loaded {nrows} rows into Snowflake")

if __name__ == "__main__":
    load_iceberg_to_snowflake()
    
