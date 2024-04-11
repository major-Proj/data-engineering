{{ config(
    tags=['mart']
)}}

with technical_satisfaction as (
select avg(Q6) as technical_satisfaction,name from {{ ref('feedbacks') }}
where role = 'engineer'
group by name
order by technical_satisfaction desc
),

client_requirement_satisfaction as (
select avg(Q6) as client_requirement_satisfaction,name from {{ ref('feedbacks') }}
where role = 'consultant'
group by name
order by client_requirement_satisfaction desc
)

select ts.technical_satisfaction,crs.client_requirement_satisfaction,ts.name 
from technical_satisfaction ts join client_requirement_satisfaction crs
on ts.name = crs.name
