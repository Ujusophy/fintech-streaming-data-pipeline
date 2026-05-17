SELECT 
    transaction_id,
    timestamp AS transaction_time,
    amount,
    currency,
    amount_usd,
    exchange_rate,
    status,
    payment_method,
    merchant_id,
    merchant_name,
    customer_id,
    customer_name,
    country,
    is_flagged 
FROM {{ source('raw', 'transactions_raw') }}
