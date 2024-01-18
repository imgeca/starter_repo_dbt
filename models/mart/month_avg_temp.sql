WITH temp_daily AS (
    SELECT *
    FROM {{ref('prep_temp')}}
),
monthly_avg_temp AS (
    SELECT
        EXTRACT(MONTH FROM date) AS month,
        AVG(avgtemp_c) AS avg_monthly_temp
    FROM temp_daily
    GROUP BY month
)
SELECT *
FROM monthly_avg_temp;


WITH total_avg_month AS(
    SELECT 
    city, country, year, lat, lon,
    AVG(avgtemp_c) AS avg_temp_month,
    MAX(maxtemp_c) AS max_temp_month,
    MIN(mintemp_c) AS min_temp_month
    FROM {{ref("prep_temp")}}
    GROUP BY city, country, year, month, lat, lon
)
SELECT * FROM total_avg_month