SELECT * FROM {{ref('stg_transactions')}}
WHERE is_flagged = TRUE
ORDER BY amount DESC
