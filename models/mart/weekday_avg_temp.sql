WITH temp_daily AS (
    SELECT *
    FROM {{ref('staging_weather')}}
),
weekday_avg_temp AS (
    SELECT
        weekday,
        AVG(avgtemp_c) AS avg_weekday_temp
    FROM temp_daily
    WHERE weekday IS NOT NULL -- Ensure that weekday information is available
    GROUP BY weekday
)
SELECT *
FROM weekday_avg_temp;
