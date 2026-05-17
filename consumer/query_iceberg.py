import duckdb

con = duckdb.connect('C:/Users/LENOVO/Downloads/fintech-streaming-pipeline/dbt_pipeline/fintech_iceberg.duckdb')

con.execute("""
    INSTALL iceberg;
    LOAD iceberg;
""")

con.execute("""
    SET s3_endpoint='localhost:9000';
    SET s3_access_key_id='minioadmin';
    SET s3_secret_access_key='minioadmin';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
""")

metadata_path = "s3://iceberg-warehouse/fintech_db/transactions/metadata/00132-2f0441b8-8f49-4898-9998-7011dc209e44.metadata.json"

result = con.execute(f"""
    SELECT *
    FROM iceberg_scan('{metadata_path}')
    LIMIT 10
""").fetchdf()

print(result)

result1 = con.execute(f"""
    SELECT 
        country,
        COUNT(*) as total_transactions,
        ROUND(SUM(amount_usd), 2) as total_volume_usd,
        ROUND(AVG(amount_usd), 2) as avg_transaction_usd
    FROM iceberg_scan('{metadata_path}')
    GROUP BY country
    ORDER BY total_volume_usd DESC
""").fetchdf()

print("\n--- Transaction Volume by Country ---")
print(result1)

result2 = con.execute(f"""
    SELECT
        status,
        COUNT(*) as total,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage
    FROM iceberg_scan('{metadata_path}')
    GROUP BY status
    ORDER BY total DESC
""").fetchdf()

print("\n--- Transaction Status Breakdown ---")
print(result2)

result3 = con.execute(f"""
    SELECT
        transaction_id,
        amount,
        amount_usd,
        country,
        merchant_name,
        timestamp
    FROM iceberg_scan('{metadata_path}')
    WHERE is_flagged = true
    ORDER BY amount DESC
    LIMIT 10
""").fetchdf()

print("\n--- Top Flagged Transactions ---")
print(result3)

early_snapshot = 8145985492175459783
recent_snapshot = 8756539790185265866

early_count = con.execute(f"""
    SELECT COUNT(*) as total_transactions
    FROM iceberg_scan('{metadata_path}', snapshot_from_id={early_snapshot})
""").fetchdf()

recent_count = con.execute(f"""
    SELECT COUNT(*) as total_transactions
    FROM iceberg_scan('{metadata_path}', snapshot_from_id={recent_snapshot})
""").fetchdf()

print("\n--- Time Travel Comparison ---")
print(f"Transactions at snapshot 8 (early):   {early_count['total_transactions'][0]}")
print(f"Transactions at snapshot 132 (latest): {recent_count['total_transactions'][0]}")

transactions_by_country = con.execute("""
    SELECT * FROM fintech_iceberg.main.fct_transactions_by_country
    ORDER BY total_transactions DESC
""").fetchdf()

print("\n--- Transactions by Country ---")
print(transactions_by_country)