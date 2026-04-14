{{ config(materialized='table') }}

select distinct
    product_name,
    category
from {{ ref('stg_sales') }}