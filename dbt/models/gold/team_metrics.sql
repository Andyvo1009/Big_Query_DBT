{{ config(
    materialized='view',
    schema='gold_layer'
) }}

-- 1. Aggregate leads
with leads as (
    select
        salesperson_id,
        count(*) as total_leads,
        countif(is_won = TRUE) as won_leads,
        countif(active = FALSE) as lost_leads
    from {{ ref('fact_lead') }}
    group by salesperson_id
),

-- 2. Aggregate sales
sales as (
    select
        salesperson_id,
        count(distinct id) as number_of_sales,
        sum(total_amount) as total_revenue
    from {{ ref('fact_sale_order') }}
    group by salesperson_id
),

-- 3. Salesperson dimension
sp as (
    select 
        id as salesperson_id,
        team_name as team,
    from {{ ref('dim_salesperson') }}
)

-- 4. Final join (no duplication)
select
    sp.salesperson_id,
    sp.team,
    coalesce(s.number_of_sales, 0) as number_of_sales,
    coalesce(s.total_revenue, 0) as total_revenue,
    coalesce(l.total_leads, 0) as total_leads,
    coalesce(l.won_leads, 0) as won_leads,
    coalesce(l.lost_leads, 0) as lost_leads,
    case 
        when l.total_leads > 0 
        then l.won_leads / l.total_leads
        else 0 
    end as lead_conversion_rate
from sp
left join sales s on sp.salesperson_id = s.salesperson_id
left join leads l on sp.salesperson_id = l.salesperson_id
