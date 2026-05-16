# Real-Time Fintech Transaction Pipeline

> This is not a real payment system.
> But it is built the way a real one would be.

---

## What This Project Does

Simulates a fintech payment processor operating across five African markets. A Python script generates realistic fake transactions continuously, pushes them through Kafka, stores them in Apache Iceberg, and loads them into Snowflake where dbt transforms everything into clean, testable models for a Power BI dashboard.

The goal was to build something that mirrors what companies like Paystack or Flutterwave run in production — not a tutorial project, but an actual streaming architecture with real tools talking to each other.

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
fintech-streaming-pipeline/
  ├── producer/
  │     └── producer.py               # generates fake transactions, enriches with live exchange rates
  ├── consumer/
  │     ├── consumer.py               # reads from Kafka, writes to Iceberg
  │     ├── query_iceberg.py          # queries Iceberg with DuckDB including time travel
  │     └── iceberg_to_snowflake.py   # loads Iceberg data into Snowflake
  ├── dbt_pipeline/
  │     └── models/
  │           ├── staging/
  │           │     ├── stg_transactions.sql     # cleans and renames raw columns
  │           │     └── sources.yml              # registers Snowflake raw table as dbt source
  │           └── marts/
  │                 ├── fct_transactions_by_country.sql        # volume and success rate by country
  │                 ├── fct_transactions_by_payment_method.sql # breakdown by payment method
  │                 ├── fct_flagged_transactions.sql           # transactions above 300,000 NGN
  │                 └── marts.yml                              # dbt tests
  └── docker-compose.yml              # spins up Kafka, Zookeeper, and MinIO
```

---

## About the Data

All transactions are fake — generated using the Python Faker library. Fields include merchant name, customer name, payment method, country, amount, and status. Nothing here represents real financial activity.

One real element: each transaction is enriched with a live NGN to USD exchange rate pulled from the ExchangeRate API. The conversion values are accurate even though the transactions themselves are not. This mirrors how real cross-border payment systems handle currency conversion — fetch a live rate, apply it to the transaction, move on.

Transactions above 300,000 NGN are automatically flagged as suspicious. In a real system this rule would be far more sophisticated, but the point here is to show the pattern — flag at ingestion, surface in the dashboard.

---

## How to Run It

**1. Clone the repo**
```bash
git clone https://github.com/your-username/fintech-streaming-pipeline
cd fintech-streaming-pipeline
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

## Time Travel

Every batch written to Iceberg creates a snapshot. You can query any snapshot by its ID:

```python
# Table as it looked early on
SELECT COUNT(*) FROM iceberg_scan('...', snapshot_from_id=8145985492175459783)

# Table right now
SELECT COUNT(*) FROM iceberg_scan('...', snapshot_from_id=8756539790185265866)
```

Same table. Two different moments. In a real system this is how you recover from bad data — go back to the last clean snapshot instead of trying to unpick what went wrong.

---

## Notes

The producer batches exchange rate API calls rather than calling once per transaction. One call per batch, rate cached for that window. Keeps the pipeline within free tier limits and matches how production systems handle external API dependencies.

dbt tests run automatically with `dbt test` and check for nulls and duplicate transaction IDs across all three mart models.

---

Built by [Your Name] | [LinkedIn](your-link) | [Medium](your-link) | [GitHub](your-link)