SELECT *
FROM {{ source('bronze', 'transactions_raw') }}