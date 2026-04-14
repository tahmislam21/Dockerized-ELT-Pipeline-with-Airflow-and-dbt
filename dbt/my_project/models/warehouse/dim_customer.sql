{{ config(materialized='table') }}

select distinct
    customer_id,
    customer_name,
    city,
    state,
    country_region
from {{ ref('stg_sales') }}