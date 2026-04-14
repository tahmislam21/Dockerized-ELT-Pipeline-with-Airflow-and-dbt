{{ config(materialized='table') }}

with base as (
    select
        f.order_date,
        sum(f.revenue) as revenue,
        sum(f.quantity) as quantity,
        sum(f.shipping_fee) as shipping_fee
    from {{ ref('fact_sales') }} f
    group by f.order_date
)
select
    EXTRACT(MONTH FROM order_date) as month,
    EXTRACT(QUARTER FROM order_date) as quarter,
    EXTRACT(YEAR FROM order_date) as year,
    sum(revenue) as total_revenue,
    sum(quantity) as total_quantity,
    sum(shipping_fee) as total_shipping_fee,
    sum(sum(revenue)) over (partition by EXTRACT(YEAR FROM order_date)) as ytd_revenue,
    sum(sum(revenue)) over (partition by EXTRACT(QUARTER FROM order_date)) as qtd_revenue,
    sum(sum(revenue)) over (partition by EXTRACT(MONTH FROM order_date)) as mtd_revenue
from base
group by 1,2,3
