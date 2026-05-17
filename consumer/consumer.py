import json
import pyarrow as pa
from kafka import KafkaConsumer
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, DoubleType, BooleanType
)

catalog = SqlCatalog(
    "fintech",
    **{
        "uri": "sqlite:///iceberg_catalog.db",
        "warehouse": "s3://iceberg-warehouse",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin",
        "s3.region": "us-east-1",
    }
)

schema = Schema(
    NestedField(1, "transaction_id", StringType()),
    NestedField(2, "timestamp", StringType()),
    NestedField(3, "amount", DoubleType()),
    NestedField(4, "currency", StringType()),
    NestedField(5, "amount_usd", DoubleType()),
    NestedField(6, "exchange_rate", DoubleType()),
    NestedField(7, "status", StringType()),
    NestedField(8, "payment_method", StringType()),
    NestedField(9, "merchant_id", StringType()),
    NestedField(10, "merchant_name", StringType()),
    NestedField(11, "customer_id", StringType()),
    NestedField(12, "customer_name", StringType()),
    NestedField(13, "country", StringType()),
    NestedField(14, "is_flagged", BooleanType())
)

def create_table_if_not_exists():
    try:
        catalog.create_namespace("fintech_db")
    except Exception:
        pass
    
    try:
        table = catalog.create_table(
            "fintech_db.transactions",
            schema=schema
        )
        print("Created Iceberg table: fintech_db.transactions")
        return table
    except Exception:
        table = catalog.load_table("fintech_db.transactions")
        print("Loaded existing Iceberg table: fintech_db.transactions")
        return table
    
def write_to_iceberg(table, transactions):
    df = pa.table({
        "transaction_id": pa.array([t["transaction_id"] for t in transactions], type=pa.string()),
        "timestamp": pa.array([t["timestamp"] for t in transactions], type=pa.string()),
        "amount": pa.array([t["amount"] for t in transactions], type=pa.float64()),
        "currency": pa.array([t["currency"] for t in transactions], type=pa.string()),
        "amount_usd": pa.array([t["amount_usd"] for t in transactions], type=pa.float64()),
        "exchange_rate": pa.array([t["exchange_rate"] for t in transactions], type=pa.float64()),
        "status": pa.array([t["status"] for t in transactions], type=pa.string()),
        "payment_method": pa.array([t["payment_method"] for t in transactions], type=pa.string()),
        "merchant_id": pa.array([t["merchant_id"] for t in transactions], type=pa.string()),
        "merchant_name": pa.array([t["merchant_name"] for t in transactions], type=pa.string()),
        "customer_id": pa.array([t["customer_id"] for t in transactions], type=pa.string()),
        "customer_name": pa.array([t["customer_name"] for t in transactions], type=pa.string()),
        "country": pa.array([t["country"] for t in transactions], type=pa.string()),
        "is_flagged": pa.array([t["is_flagged"] for t in transactions], type=pa.bool_()),
    })
    
    table.append(df)
    print(f"Written {len(transactions)} transactions to Iceberg")
    
def run_consumer():
    print("Starting fintech transaction consumer...")
    
    table = create_table_if_not_exists()
    
    consumer = KafkaConsumer(
        'transactions',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='fintech-consumer-group'
    )
    
    print("Listening for transactions...")
    
    batch = []
    
    for message in consumer:
        transaction = message.value
        batch.append(transaction)
        
        if len(batch) >= 10:
            write_to_iceberg(table, batch)
            batch = []
            
if __name__ == "__main__":
    run_consumer()

