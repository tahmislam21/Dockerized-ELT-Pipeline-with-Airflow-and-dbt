{{ config(materialized='table') }}

select distinct
    salesperson
from {{ ref('stg_sales') }}