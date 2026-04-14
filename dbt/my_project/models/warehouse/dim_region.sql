{{ config(materialized='table') }}

select distinct
    region
from {{ ref('stg_sales') }}