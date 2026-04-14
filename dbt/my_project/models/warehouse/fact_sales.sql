{{ config(materialized='table') }}

with sales as (
    select * from {{ ref('stg_sales') }}
),
customers as (
    select * from {{ ref('dim_customer') }}
),
products as (
    select * from {{ ref('dim_product') }}
),
salespersons as (
    select * from {{ ref('dim_salesperson') }}
),
shippers as (
    select * from {{ ref('dim_shipper') }}
),
payments as (
    select * from {{ ref('dim_payment') }}
),
regions as (
    select * from {{ ref('dim_region') }}
)

select
    s.order_id,
    s.order_date,
    s.shipped_date,
    c.customer_id,
    p.product_name,
    sp.salesperson,
    sh.shipper_name,
    pay.payment_type,
    r.region,
    s.quantity,
    s.unit_price,
    s.revenue,
    s.shipping_fee,
    s.revenue_bins
from sales s
left join customers c on s.customer_id = c.customer_id
left join products p on s.product_name = p.product_name
left join salespersons sp on s.salesperson = sp.salesperson
left join shippers sh on s.shipper_name = sh.shipper_name
left join payments pay on s.payment_type = pay.payment_type
left join regions r on s.region = r.region