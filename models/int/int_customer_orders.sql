with customers   as (select * from {{ ref('stg_customers') }})
    ,orders      as (select * from {{ ref('stg_orders') }})
    ,transformed as (
      select   
            c.customer_id             as customer_id,
            min(o.order_date)         as first_order_date,
            max(o.order_date)         as most_recent_order_date,
            count(o.order_id)         as number_of_orders
        from customers c
        left join orders o on o.customer_id = c.customer_id
        group by c.customer_id
    )
    select * from transformed