{{ config(materialized='table') }}

select
    EXTRACT(MONTH FROM f.order_date) as month,
    EXTRACT(QUARTER FROM f.order_date) as quarter,
    EXTRACT(YEAR FROM f.order_date) as year,
    sum(f.revenue - f.shipping_fee) as total_revenue,
    sum(f.quantity) as total_quantity,
    sum(f.unit_price * f.quantity) as total_sales
from {{ ref('fact_sales') }} f
group by 1, 2, 3
order by year, quarter, month