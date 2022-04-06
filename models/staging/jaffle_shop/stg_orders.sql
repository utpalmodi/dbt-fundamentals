with
    src as (select * from {{ source("jaffle_shop", "orders") }}),
    stg as (
        select
            id as order_id,
            user_id as customer_id,
            order_date as order_placed_at,
            status,
            status as order_status
        from src
    )
select *
from stg
