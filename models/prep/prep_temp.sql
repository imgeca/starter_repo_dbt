WITH temp_daily AS (
    SELECT * 
    FROM {{ref('staging_weather')}}
),
add_weekday AS (
    SELECT *,
        date_part('day',date) AS weekday,
        date_part('dow',date) AS day_num
    FROM temp_daily
)
SELECT *
FROM add_weekday