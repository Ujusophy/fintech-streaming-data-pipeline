SELECT
    country,
    COUNT(DISTINCT transaction_id) AS total_transactions,
    COUNT(CASE WHEN status = 'success' THEN 1 END) AS successful_transactions,
    ROUND(SUM(amount_usd), 2) AS total_amount_usd,
    ROUND(AVG(amount_usd), 2) AS average_amount_usd,
    ROUND(COUNT(CASE WHEN status = 'success' THEN 1 END) * 100.0 / COUNT(*), 1) AS success_rate,
    COUNT(CASE WHEN is_flagged = TRUE THEN 1 END) AS flagged_transactions
FROM {{ ref('stg_transactions') }}
GROUP BY country
ORDER BY total_transactions DESC