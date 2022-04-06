with  customers  as (select * from {{ ref('stg_customers') }})
    , orders     as ( select * from {{ ref('stg_orders') }} )
    , payments   as (select *  from {{ ref('int_order_payment') }}
    )
    ,transformed as 
    (
        select
            o.order_id                  as order_id,
            o.customer_id               as customer_id,
            o.order_placed_at           as order_placed_at,
            o.order_status              as order_status,
            p.total_amount_paid         as total_amount_paid,
            p.payment_finalized_date    as payment_finalized_date,
            c.customer_first_name       as customer_first_name,
            c.customer_last_name        as customer_last_name
        from orders o
        left join payments  p on o.order_id = p.order_id
        left join customers c on o.customer_id = c.customer_id
    )
    select * from transformed
