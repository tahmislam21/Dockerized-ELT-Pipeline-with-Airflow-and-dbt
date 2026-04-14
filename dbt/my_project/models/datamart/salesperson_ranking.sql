{{ config(materialized='table') }}

select
    sp.salesperson,
    round(sum(f.revenue - f.shipping_fee)::numeric,2) as total_revenue,
    rank() over(order by sum(f.revenue - f.shipping_fee) desc) as revenue_rank,
    round(sum(f.quantity)::numeric,2) as total_quantity,
    rank() over(order by sum(f.quantity) desc) as quantity_rank,
    round(sum(f.unit_price * f.quantity)::numeric,2) as total_sales,
    rank() over(order by sum(f.unit_price * f.quantity) desc) as sales_rank
from {{ ref('fact_sales') }} f
left join {{ ref('dim_salesperson') }} sp on f.salesperson = sp.salesperson
group by sp.salesperson