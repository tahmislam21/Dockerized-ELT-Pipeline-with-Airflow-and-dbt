{{ config(materialized='table') }}

with base as (
    select
        c.customer_name,
        ROUND(sum(f.revenue- f.shipping_fee)::NUMERIC,2) as total_revenue,
        ROUND(sum(f.quantity)::NUMERIC,2) as total_quantity,
        ROUND(sum(f.unit_price * f.quantity)::NUMERIC,2) as total_sales
    from {{ ref('fact_sales') }} f
    left join {{ ref('dim_customer') }} c on f.customer_id = c.customer_id
    group by c.customer_name
)
select
    *,
    rank() over(order by total_revenue desc) as revenue_rank,
    rank() over(order by total_quantity desc) as quantity_rank,
    rank() over(order by total_sales desc) as sales_rank
from base