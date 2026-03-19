with transactions as (
    SELECT *
    FROM {{ ref('cleaned_transactions') }}
),
accounts as (
    SELECT *
    FROM {{ ref('cleaned_accounts') }}
),
cards as (
    SELECT *
    FROM {{ ref('cleaned_cards') }}
),
customers as (
    SELECT *
    FROM {{ ref('cleaned_customers') }}
),

trans_acc as(
    select t.*, a.customer_id, a.account_type, a.account_status
    from transactions t
    left join accounts a
    on t.account_id = a.account_id
),
trans_acc_cust as(
    select ta.*, c.email
    from trans_acc ta
    left join customers c
    on ta.customer_id = c.customer_id
),

number_of_cards as(
    select customer_id, count(card_id) as total_cards
    from cards
    group by customer_id
),

trans_acc_cust_cards as(
    select tac.*, nc.total_cards 
    from trans_acc_cust tac
    left join number_of_cards nc
    on tac.customer_id = nc.customer_id
)
select *
from trans_acc_cust_cards