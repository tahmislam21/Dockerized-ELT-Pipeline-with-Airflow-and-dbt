{{ config(materialized='table') }}

select
    sh.shipper_name,
    sum(f.shipping_fee) as total_shipping_fee,
    avg(f.shipping_fee) as avg_shipping_fee,
    rank() over(order by sum(f.shipping_fee) desc) as expense_rank
from {{ ref('fact_sales') }} f
left join {{ ref('dim_shipper') }} sh on f.shipper_name = sh.shipper_name
group by sh.shipper_name