{{ config(materialized='table') }}

with summary as (
    select
        EXTRACT(MONTH FROM f.order_date) as month,
        EXTRACT(QUARTER FROM f.order_date) as quarter,
        EXTRACT(YEAR FROM f.order_date) as year,
        ROUND(sum(f.revenue-f.shipping_fee)::NUMERIC,2) as total_revenue
    from {{ ref('fact_sales') }} f
    group by 1, 2, 3
)
select
    *,
    ROUND(lag(total_revenue) over (order by month)::NUMERIC,2) as prev_month_revenue,
    ROUND(lag(total_revenue) over (order by quarter)::NUMERIC,2) as prev_quarter_revenue,
    ROUND(lag(total_revenue) over (order by year)::NUMERIC,2) as prev_year_revenue,
    round(((total_revenue - lag(total_revenue) over (order by month)) / nullif(lag(total_revenue) over (order by month), 0))::numeric * 100, 2) as monthly_growth_pct,
    round(((total_revenue - lag(total_revenue) over (order by quarter)) / nullif(lag(total_revenue) over (order by quarter), 0))::numeric * 100, 2) as quarterly_growth_pct,
    round(((total_revenue - lag(total_revenue) over (order by year)) / nullif(lag(total_revenue) over (order by year), 0))::numeric * 100, 2) as yearly_growth_pct
from summary