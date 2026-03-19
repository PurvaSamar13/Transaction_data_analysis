{% set raw_transactions = source('bronze','transactions_raw') %}

SELECT

    transaction_id,
    account_id,

    CASE
        WHEN regexp_like(UPPER(TRIM(transaction_type)), '^(CR|CREDIT)') THEN 'CREDIT'
        WHEN regexp_like(UPPER(TRIM(transaction_type)), '^(DR|DEBIT|DRT)') THEN 'DEBIT'
        ELSE null
    END AS transaction_type,

    regexp_replace(amount,'[^0-9.]','')::NUMBER(12,2) AS amount,

    CASE
        WHEN UPPER(currency_code) LIKE '%INR%' THEN 'INR'
        WHEN UPPER(currency_code) LIKE '%USD%' THEN 'USD'
        ELSE null
    END AS currency,

    TRIM(regexp_replace(merchant_name,'[^A-Za-z0-9 ]','')) AS merchant_name,

    CASE
        WHEN UPPER(status) IN ('SUCCESS','COMPLETED','S') THEN 'SUCCESS'
        WHEN UPPER(status) IN ('PENDING','PROCESSING') THEN 'PENDING'
        WHEN UPPER(status) IN ('FAILED','FAIL','F') THEN 'FAIL'
        ELSE 'FAIL'
    END AS status,

    CURRENT_TIMESTAMP() AS updated_at

FROM {{ raw_transactions }}