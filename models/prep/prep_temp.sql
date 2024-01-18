WITH temp_daily AS (
    SELECT * 
    FROM {{ref('staging_weather')}}
),
add_weekday AS (
    SELECT *,
        date_part('day',date) AS weekday,
        date_part('dow',date) AS day_num,
        date_part('month', date) as month,
        date_part('year', date) as year,
        date_part('week', date) as week
    FROM temp_daily
)
SELECT *
FROM add_weekday