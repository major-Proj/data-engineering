{{ config(
    tags=['mart']
) }}

with total_hours_project as (
select NAME as project_name,SUM(total) as total_hours
from {{ ref('timesheets') }}
where submitted = True and project_name != 'bench'
group by NAME
),

consolidated_feedbacks as (
select NAME as project_name, AVG(total_SUM) as AVG_rating
from {{ ref('feedbacks') }}
group by NAME
)

select thp.project_name,thp.total_hours,cf.avg_rating
from total_hours_project as thp join consolidated_feedbacks as cf
on thp.project_name=cf.project_name
order by avg_rating desc