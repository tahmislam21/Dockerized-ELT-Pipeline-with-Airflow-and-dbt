{{ config(materialized='table') }}

select distinct
    shipper_name,
    ship_name,
    ship_address,
    ship_city,
    ship_state,
    ship_country_region
from {{ ref('stg_sales') }}