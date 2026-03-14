SELECT

    transaction_id,
    account_id,

    CASE
        WHEN REGEXP_LIKE(UPPER(TRIM(transaction_type)), '^(CR|CREDIT)') THEN 'CREDIT'
        WHEN REGEXP_LIKE(UPPER(TRIM(transaction_type)), '^(DR|DEBIT|DRT)') THEN 'DEBIT'
        ELSE NULL
    END AS transaction_type,

    REGEXP_REPLACE(amount_raw,'[^0-9.]','')::NUMBER(12,2) AS amount,

    CASE
        WHEN UPPER(currency_code) LIKE '%INR%' THEN 'INR'
        WHEN UPPER(currency_code) LIKE '%USD%' THEN 'USD'
        ELSE NULL
    END AS currency,

    TRIM(REGEXP_REPLACE(merchant_name,'[^A-Za-z0-9 ]','')) AS merchant_name,

    CASE
        WHEN UPPER(status) IN ('SUCCESS','COMPLETED','S') THEN 'SUCCESS'
        WHEN UPPER(status) IN ('PENDING','PROCESSING') THEN 'PENDING'
        WHEN UPPER(status) IN ('FAILED','FAIL','F') THEN 'FAIL'
        ELSE 'FAIL'
    END AS status,

    CURRENT_TIMESTAMP() AS updated_at

FROM {{ source('bronze','transactions_raw') }}