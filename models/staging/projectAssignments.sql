{{ config(
    tags=['basic', 'staging']
) }}

WITH required_field AS (
    SELECT 
        ID,
        PID,    
        EMAIL,
        TO_DATE(ALLOCATION_START) AS START_PERIOD,
        TO_DATE(ALLOCATION_END) AS END_PERIOD,
        DATEDIFF('day', TO_DATE(START_PERIOD), TO_DATE(END_PERIOD)) AS duration_length
    FROM {{ source('timely','PROJECTASSIGNMENTS') }}
    WHERE PID != ''
)

SELECT rf.*,
       p.name
FROM required_field rf
JOIN {{ source('timely', 'PROJECTS') }} p
ON rf.PID = p.PID