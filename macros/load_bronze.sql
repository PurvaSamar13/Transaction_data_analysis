{% macro load_bronze() %}

{% set copy_accounts %}
copy into BANKING.a_bronze_table.accounts_raw
from @banking.external_stage.csv_folder/account_raw.csv
on_error = 'continue';
{% endset %}

{% set copy_cards %}
copy into BANKING.a_bronze_table.cards_raw
from @banking.external_stage.csv_folder/card_raw.csv
on_error = 'continue';
{% endset %}

{% set copy_customers %}
copy into BANKING.a_bronze_table.customers_raw
from @banking.external_stage.csv_folder/customer_raw.csv
on_error = 'continue';
{% endset %}

{% set copy_transactions %}
copy into BANKING.a_bronze_table.transactions_raw
from @banking.external_stage.csv_folder/transaction_raw.csv
on_error = 'continue';
{% endset %}

{{ run_query(copy_accounts) }}
{{ run_query(copy_cards) }}
{{ run_query(copy_customers) }}
{{ run_query(copy_transactions) }}

{% endmacro %}