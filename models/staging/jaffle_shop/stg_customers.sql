with
    src as (select * from from {{ source("jaffle_shop", "customers") }}),
    stg as (
        select
            id as customer_id,
            first_name as customer_first_name,
            last_name as customer_last_name
        from src
    )
select *
from stg
