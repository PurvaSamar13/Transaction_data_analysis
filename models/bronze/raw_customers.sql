SELECT *
FROM {{ source('bronze', 'customers_raw') }}