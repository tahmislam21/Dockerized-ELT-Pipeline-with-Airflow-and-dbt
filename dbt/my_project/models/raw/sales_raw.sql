{{ config(materialized='table') }}


    select * from {{ source('dev','raw_sales_data')}}

