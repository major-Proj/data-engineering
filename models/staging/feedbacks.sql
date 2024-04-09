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
        ROLE,
        CAST(Q1 AS INT) AS Q1,
        CAST(Q2 AS INT) AS Q2,
        CAST(Q3 AS INT) AS Q3,
        CAST(Q4 AS INT) AS Q4,
        CAST(Q5 AS INT) AS Q5,
        CAST(Q6 AS INT) AS Q6,
        CAST(Q7 AS INT) AS Q7,
        CAST(Q8 AS INT) AS Q8,
        SUM(Q1 + Q2 + Q3 + Q4 + Q5 + Q6 + Q7 + Q8) AS total_sum,
        SUM(Q1 + Q2 + Q3 + Q4 + Q5 + Q6 + Q7 + Q8)/8 AS total_avg
    FROM {{ source('timely','FEEDBACKS') }}
    WHERE PID != ''

    GROUP BY 
    ID,PID,EMAIL,START_PERIOD,END_PERIOD,ROLE,Q1,Q2,Q3,Q4,Q5,Q6,Q7,Q8
)

SELECT 
    rf.*,
    p.name
FROM 
    required_field rf
JOIN 
    {{ source('timely', 'PROJECTS') }} p
ON 
    rf.PID = p.PID
