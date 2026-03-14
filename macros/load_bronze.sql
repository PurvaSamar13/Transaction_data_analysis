{% macro load_bronze() %}

{% set copy_account %}
copy into TRANSACTION_ANALYTICS._1_BRONZE.account_raw
from @TRANSACTION_ANALYTICS._1_BRONZE.my_s3_stage/account_raw.csv
on_error = 'continue';
{% endset %}

{% set copy_card %}
copy into TRANSACTION_ANALYTICS._1_BRONZE.card_raw
from @TRANSACTION_ANALYTICS._1_BRONZE.my_s3_stage/card_raw.csv
on_error = 'continue';
{% endset %}

{% set copy_customer %}
copy into TRANSACTION_ANALYTICS._1_BRONZE.customer_raw
from @TRANSACTION_ANALYTICS._1_BRONZE.my_s3_stage/customer_raw.csv
on_error = 'continue';
{% endset %}

{% set copy_transaction %}
copy into TRANSACTION_ANALYTICS._1_BRONZE.transaction_raw
from @TRANSACTION_ANALYTICS._1_BRONZE.my_s3_stage/transaction_raw.csv
on_error = 'continue';
{% endset %}

{{ run_query(copy_account) }}
{{ run_query(copy_card) }}
{{ run_query(copy_customer) }}
{{ run_query(copy_transaction) }}

{% endmacro %}