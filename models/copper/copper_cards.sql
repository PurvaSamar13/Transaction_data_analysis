{% set raw_cards = source('bronze','cards_raw')%}

WITH base AS (

    SELECT
        card_id,
        customer_id,

        /* 1. Standardise card type */
        CASE
            when upper(card_type) LIKE '%DEBIT%' THEN 'Debit'
            when upper(card_type) LIKE '%CREDIT%' THEN 'Credit'
            ELSE null
        END AS card_type,

        card_number_raw,
        expiry_date,
        cvv,
        issued_date,

        /* 5. Standardise status */
        CASE
            when upper(status) IN ('ACTIVE','A') THEN 'Active'
            when upper(status) IN ('INACTIVE','I','BLOCKED') THEN 'Inactive'
            ELSE null
        END AS status

    FROM {{ raw_cards }}
),

cvv_clean AS (

SELECT
    card_id,
    customer_id,
    card_type,
    card_number_raw,
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
        card_number_raw,
        expiry_date,
        cvv,

        /* Handle mixed issue date formats */
        COALESCE(
            TRY_TO_DATE(issued_date,'YYYY-MM-DD'),
            TRY_TO_DATE(issued_date,'YYYY/MM/DD'),
            TO_DATE('2012-12-31','YYYY-MM-DD')
        ) AS issued_date,

        status,

        CURRENT_TIMESTAMP() AS updated_at

    FROM cvv_clean

)

SELECT * FROM final