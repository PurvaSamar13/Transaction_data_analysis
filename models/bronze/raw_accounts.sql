SELECT *
FROM {{ source('bronze', 'accounts_raw') }}