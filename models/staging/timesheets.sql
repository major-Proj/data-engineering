{{ config(
    tags=['basic', 'staging']
) }}

WITH required_field AS (
    SELECT 
        ID,
        PID,    
        EMAIL,
        TO_DATE(START_PERIOD) AS START_DATE,
        TO_DATE(END_PERIOD) AS END_DATE,
        CAST(MON AS INT) AS MON,
        CAST(TUE AS INT) AS TUE,
        CAST(WED AS INT) AS WED,
        CAST(THUR AS INT) AS THUR,
        CAST(FRI AS INT) AS FRI,
        CAST(SAT AS INT) AS SAT,
        CAST(SUN AS INT) AS SUN,
        SUBMITTED
    FROM {{ source('timely','TIMESHEETS') }}
    WHERE PID != ''
)

SELECT 
    rf.EMAIL,
    rf.START_DATE,
    rf.END_DATE,
    SUM(rf.MON) AS MON,
    SUM(rf.TUE) AS TUE,
    SUM(rf.WED) AS WED,
    SUM(rf.THUR) AS THUR,
    SUM(rf.FRI) AS FRI,
    SUM(rf.SAT) AS SAT,
    SUM(rf.SUN) AS SUN,
    SUM(rf.MON + rf.TUE + rf.WED + rf.THUR + rf.FRI + rf.SAT + rf.SUN) AS total,
    p.name
FROM required_field rf
JOIN {{ source('timely', 'PROJECTS') }} p
ON rf.PID = p.PID
GROUP BY rf.EMAIL, rf.START_DATE, rf.END_DATE, p.name
