{{ config(materialized='table') }}

select
    p.product_name,
    p.category,
    ROUND(sum(f.revenue -f.shipping_fee)::NUMERIC,2) as total_revenue,
    rank() over(order by sum(f.revenue - f.shipping_fee) desc) as revenue_rank,
    round(sum(f.quantity)::numeric,2) as total_quantity,
    rank() over(order by sum(f.quantity) desc) as quantity_rank,
    ROUND(sum(f.unit_price * f.quantity)::NUMERIC,2) as total_sales,
    rank() over(order by sum(f.revenue) desc) as sales_rank
from {{ ref('fact_sales') }} f
left join {{ ref('dim_product') }} p on f.product_name = p.product_name
group by p.product_name, p.category