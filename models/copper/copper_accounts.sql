SELECT

    account_id,
    customer_id,

    /* Standardise account type */
    CASE
        WHEN UPPER(account_type) IN ('SAV','SA','SAVINGS') THEN 'SAVINGS'
        WHEN UPPER(account_type) IN ('CURR','CURRENT') THEN 'CURRENT'
        ELSE NULL
    END AS account_type,

    /* Standardise account status */
    CASE
        WHEN UPPER(account_status) IN ('A','ACTIVE') THEN 'ACTIVE'
        WHEN UPPER(account_status) IN ('I','INACTIVE') THEN 'INACTIVE'
        ELSE NULL
    END AS account_status,

    /* Convert opened date */
    TO_DATE(opened_date,'DD/MM/YYYY') AS opened_date,

    balance,

    /* Apply closed_date business rule */
    CASE
        WHEN UPPER(account_status) IN ('A','ACTIVE')
            THEN NULL
        WHEN UPPER(account_status) IN ('I','INACTIVE')
             AND closed_date IS NULL
            THEN DATE '2025-12-31'
        ELSE closed_date
    END AS closed_date,

    branch_code,
    updated_at

FROM {{ source('bronze','accounts_raw') }}