{{ config(
    tags=['basic', 'staging']
) }}

WITH required_field AS (
    SELECT 
        ID,
        PID,    
        EMAIL,
        TO_DATE(START_PERIOD) AS START_PERIOD,
        TO_DATE(END_PERIOD) AS END_PERIOD,
        CAST(MON AS INT) AS MON,
        CAST(TUE AS INT) AS TUE,
        CAST(WED AS INT) AS WED,
        CAST(THUR AS INT) AS THUR,
        CAST(FRI AS INT) AS FRI,
        CAST(SAT AS INT) AS SAT,
        CAST(SUN AS INT) AS SUN,
        SUBMITTED
    FROM {{ source('timely','TIMESHEETS') }}
    WHERE VISIBLE = true AND PID != ''
),

total_calculation AS (
    SELECT *,
           MON + TUE + WED + THUR + FRI + SAT + SUN AS total
    FROM required_field
)

SELECT rf.*,
       p.name
FROM total_calculation rf
JOIN {{ source('timely', 'PROJECTS') }} p
ON rf.PID = p.PID

