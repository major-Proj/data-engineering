{{ config(
    tags=['basic', 'staging']
) }}

WITH required_field AS (
    SELECT 
        ID,
        PID,    
        NAME,
        CLIENT_NAME,
        TO_DATE(START_PERIOD) AS START_PERIOD,
        TO_DATE(END_PERIOD) AS END_PERIOD,
        DATEDIFF('day', TO_DATE(START_PERIOD), TO_DATE(END_PERIOD)) AS duration_length
    FROM {{ source('timely','PROJECTS') }}
    WHERE PID != ''
)

SELECT * FROM required_field
