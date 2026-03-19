{% set raw_accounts = source('bronze','accounts_raw') %}

SELECT
    account_id,
    customer_id,

    /* Standardise account type */
    CASE
        when upper(account_type) IN ('SAV','SA','SAVINGS') THEN 'Savings'
        when upper(account_type) IN ('CURR','CURRENT') THEN 'Current'
        ELSE null
    END AS account_type,

    /* Standardise account status */
    CASE
        when upper(account_status) IN ('A','ACTIVE') THEN 'Active'
        when upper(account_status) IN ('I','INACTIVE') THEN 'Inactive'
        ELSE null
    END AS account_status,

    /* Convert opened date */
    TO_DATE(opened_date,'DD/MM/YYYY') AS opened_date,

    cast(balance as decimal(12,2)) as balance,

    /* Apply closed_date business rule */
    CASE
        when upper(account_status) IN ('A','ACTIVE')
            THEN '9999-12-31'
        when upper(account_status) IN ('I','INACTIVE')
             AND closed_date IS null
            THEN DATE '2025-12-31'
        ELSE closed_date
    END AS closed_date,

    branch_code,

    CURRENT_TIMESTAMP() as updated_at

FROM {{ raw_accounts }}