SELECT *
FROM {{ source('bronze', 'cards_raw') }}