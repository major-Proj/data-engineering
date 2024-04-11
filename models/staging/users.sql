{{
    config(
        tags=['basic', 'staging']
    )
}}

WITH 
required_field AS (
    SELECT 
    FIRST_NAME,
    EMAIL,
    LAST_NAME,
    ROLE
    FROM {{ source('timely','USERS') }}
)

select * from required_field