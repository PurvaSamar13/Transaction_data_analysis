WITH base AS (

SELECT
    customer_id,

    /* Clean full name (remove emojis & special characters) */
    TRIM(REGEXP_REPLACE(full_name, '[^A-Za-z ]', '')) AS full_name,

    email,

    /* Remove extension starting with x */
    REGEXP_REPLACE(phone_number, 'x.*$', '') AS phone_step1,

    date_of_birth,
    pan_number,

    /* Remove leading " from address */
    REGEXP_REPLACE(address, '^"', '') AS address,

    country,

    created_at

FROM {{ source('bronze','customers_raw') }}

),

phone_digits AS (

SELECT
    customer_id,
    full_name,
    email,

    /* Remove everything except digits */
    REGEXP_REPLACE(phone_step1, '[^0-9]', '') AS phone_clean,

    date_of_birth,
    pan_number,
    address,
    country,
    created_at

FROM base

),

final_format AS (

SELECT
    customer_id,
    full_name,
    email,

    /* Keep last 10 digits and format abc-def-ghij */
    SUBSTR(phone_clean,-10,3) || '-' ||
    SUBSTR(phone_clean,-7,3) || '-' ||
    SUBSTR(phone_clean,-4,4) AS phone_number,

    date_of_birth,
    pan_number,
    address,
    country,

    /* Updated timestamp format */
    TO_VARCHAR(CURRENT_TIMESTAMP(),'DD-MM-YY HH24-MI-SS') AS updated_at

FROM phone_digits

)

SELECT * FROM final_format