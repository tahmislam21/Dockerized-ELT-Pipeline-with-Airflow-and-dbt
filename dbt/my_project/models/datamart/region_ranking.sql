{{ config(materialized='table') }}

select
    r.region,
    c.city,
    round(sum(f.revenue- f.shipping_fee)::numeric,2) as total_revenue,
    rank() over(order by sum(f.revenue- f.shipping_fee) desc) as region_revenue_rank,
    round(sum(f.quantity)::numeric,2) as total_quantity,
    rank() over(order by sum(f.quantity) desc) as region_quantity_rank,
    round(sum(f.unit_price * f.quantity)::numeric,2) as total_sales,
    rank() over(order by sum(f.unit_price * f.quantity) desc) as region_sales_rank
from {{ ref('fact_sales') }} f
left join {{ ref('dim_customer') }} c on f.customer_id = c.customer_id
left join {{ ref('dim_region') }} r on f.region = r.region
group by r.region, c.city