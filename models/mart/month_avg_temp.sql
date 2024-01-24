

WITH total_avg_month AS (
    SELECT 
        city, 
        country, 
        year, 
        month,  -- Include 'month' here
        lat, 
        lon,
        AVG(avgtemp_c) AS avg_temp_month,
        MAX(maxtemp_c) AS max_temp_month,
        MIN(mintemp_c) AS min_temp_month
    FROM {{ref("prep_temp")}}
    GROUP BY city, country, year, month, lat, lon
)
SELECT * FROM total_avg_month;
