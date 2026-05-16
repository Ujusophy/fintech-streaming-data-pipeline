# Real-Time Fintech Transaction Pipeline

> This is not a real payment system.
> But it is built the way a real one would be.

---

## What This Project Does

This project simulates a fintech payment processor operating across five African markets. A Python script generates realistic fake transactions continuously, pushes them through Kafka, stores them in Apache Iceberg, and loads them into Snowflake where dbt transforms everything into clean, testable models for a Power BI dashboard.

The goal was to build something that mirrors what companies like Paystack or Flutterwave run in production.

---

## Architecture

```
Python Producer
     |
     v
Apache Kafka (streaming)
     |
     v
Apache Iceberg on MinIO (storage with time travel)
     |
     v
DuckDB (queries Iceberg, exports to Snowflake)
     |
     v
Snowflake (cloud warehouse)
     |
     v
dbt (transformation + testing + documentation)
     |
     v
Power BI (dashboard)
```

---

## Stack

| Layer | Tool |
|---|---|
| Event simulation | Python + Faker |
| Streaming | Apache Kafka |
| Storage format | Apache Iceberg |
| Object storage | MinIO (local S3) |
| Query engine | DuckDB |
| Cloud warehouse | Snowflake |
| Transformation | dbt |
| Visualization | Power BI |
| Containerization | Docker |

---

## Project Structure

```
fintech-streaming-data-pipeline/
  ├── producer/
  │     └── producer.py
  ├── consumer/
  │     ├── consumer.py          
  │     ├── query_iceberg.py         
  │     └── iceberg_to_snowflake.py   
  ├── dbt_pipeline/
  │     └── models/
  │           ├── staging/
  │           │     ├── stg_transactions.sql     
  │           │     └── sources.yml              
  │           └── marts/
  │                 ├── fct_transactions_by_country.sql        
  │                 ├── fct_transactions_by_payment_method.sql 
  │                 ├── fct_flagged_transactions.sql          
  │                 └── marts.yml                              
  └── docker-compose.yml           
```

---

## About the Data

All transactions are fake generated using the Python Faker library. Fields include merchant name, customer name, payment method, country, amount, and status. Nothing here represents real financial activity.

One real element: each transaction is enriched with a live NGN to USD exchange rate pulled from the ExchangeRate API. The conversion values are accurate even though the transactions themselves are not. This mirrors how real cross-border payment systems handle currency conversion.

Transactions above 300,000 NGN are automatically flagged as suspicious. In a real system this rule would be far more sophisticated, but the point here is to show the pattern flag at ingestion, surface in the dashboard.

---

## How to Run It

**1. Clone the repo**
```bash
git clone https://github.com/Ujusophy/fintech-streaming-pipeline
cd fintech-streaming-data-pipeline
```

**2. Install Python dependencies**
```bash
pip install -r requirements.txt
```

**3. Start the infrastructure**
```bash
docker-compose up -d
```

Go to `http://localhost:9001`, log in with `minioadmin/minioadmin`, and create a bucket called `iceberg-warehouse`.

**4. Start the producer**
```bash
python producer/producer.py
```

Leave this running. It sends 1 to 5 transactions every 2 seconds.

**5. Start the consumer**

Open a second terminal:
```bash
python consumer/consumer.py
```

Reads from Kafka and writes batches of 10 transactions into Iceberg.

**6. Load into Snowflake**
```bash
python consumer/iceberg_to_snowflake.py
```

**7. Run dbt**
```bash
cd dbt_pipeline
dbt run
dbt test
```

**8. View the docs**
```bash
dbt docs generate
dbt docs serve
```

---

[Medium](your-link)
