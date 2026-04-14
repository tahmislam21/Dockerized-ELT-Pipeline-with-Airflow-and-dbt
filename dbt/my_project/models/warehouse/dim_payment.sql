{{ config(materialized='table') }}

select distinct
    payment_type
from {{ ref('stg_sales') }}