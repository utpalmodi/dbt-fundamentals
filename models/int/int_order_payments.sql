with
    stg as (select * from {{ ref("stg_payments") }}),
    transformed as (
        select
            order_id,
            max(created) as payment_finalized_date,
            sum(amount) as total_amount_paid
        from stg
        where status <> 'fail'
        group by 1
    )
select *
from transformed
