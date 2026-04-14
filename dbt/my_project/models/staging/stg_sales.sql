-- models/staging/orders_stg.sql
{{ config(materialized='table') }}

with raw_rn as (
    select *, row_number() over (partition by order_id order by inserted_at desc) as rn
    from {{ ref('sales_raw') }}
)

select * from raw_rn where rn = 1
and unit_price is not null 
and unit_price != 'NaN'::float

