-- Import CTEs
with
    -- customer_orders
    customer_orders as (
        select customer_id, first_order_date, most_recent_order_date, number_of_orders
        from {{ ref("int_customer_orders") }}
    ),

    -- paid orders
    customer_orders_paid as (select * from {{ ref("int_customer_orders_paid") }}),
    paid_orders as (
        select
            customer_id,
            order_id,
            lag(order_id) over (
                partition by customer_id order by customer_id
            ) order_id_prior,
            order_placed_at,
            order_status,
            total_amount_paid,
            lag(total_amount_paid) over (
                partition by customer_id order by customer_id
            ) total_amt_prior,
            sum(total_amount_paid) over (
                partition by customer_id
                order by order_id
                rows between unbounded preceding and current row
            ) total_amount_paid_cum,
            payment_finalized_date,
            customer_first_name,
            customer_last_name
        from customer_orders_paid
    ),
    
    -- final CTE
    final as (
        select
            -- p.*,
            p.customer_id,
            p.order_id,
            p.total_amount_paid,
            row_number() over (order by p.order_id) as transaction_seq,
            row_number() over (
                partition by customer_id order by p.order_id
            ) as customer_sales_seq,
            case
                when c.first_order_date = p.order_placed_at then 'new' else 'return'
            end as nvsr,
            p.total_amount_paid_cum as customer_lifetime_value,
            c.first_order_date as fdos
        from paid_orders p
        left join customer_orders as c using(customer_id)
    ),

select *
from final
order by order_id