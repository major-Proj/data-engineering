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
    FROM {{ source('timely','FEEDBACK_HISTORIES') }}
    WHERE PID != ''
)

SELECT rf.*,
       p.name
FROM required_field rf
JOIN {{ source('timely', 'PROJECTS') }} p
ON rf.PID = p.PID