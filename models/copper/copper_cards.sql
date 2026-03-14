WITH base AS (

SELECT
    card_id,
    customer_id,

    /* 1. Standardise card type */
    CASE
        WHEN UPPER(card_type) LIKE '%DEBIT%' THEN 'DEBIT'
        WHEN UPPER(card_type) LIKE '%CREDIT%' THEN 'CREDIT'
        ELSE NULL
    END AS card_type,

    card_number_raw AS card_number,
    expiry_date,
    cvv,
    issued_date,

    /* 5. Standardise status */
    CASE
        WHEN UPPER(status) IN ('ACTIVE','A') THEN 'ACTIVE'
        WHEN UPPER(status) IN ('INACTIVE','I','BLOCKED') THEN 'INACTIVE'
        ELSE NULL
    END AS status

FROM {{ source('bronze','cards_raw') }}

),

cvv_clean AS (

SELECT
    card_id,
    customer_id,
    card_type,
    card_number,
    expiry_date,

    /* Keep only 3 digits */
    LEFT(REGEXP_REPLACE(cvv,'[^0-9]',''),3) AS cvv,

    issued_date,
    status

FROM base

),

final AS (

SELECT
    card_id,
    customer_id,
    card_type,
    card_number,
    expiry_date,
    cvv,

    /* Handle mixed issue date formats */
    COALESCE(
        TRY_TO_DATE(issued_date,'YYYY-MM-DD'),
        TRY_TO_DATE(issued_date,'YYYY/MM/DD')
    ) AS issued_date,

    status,

    CURRENT_TIMESTAMP() AS updated_at

FROM cvv_clean

)

SELECT * FROM final