WITH total_avg_month AS (
    SELECT 
        city,
        region,
        country,
        lat,
        lon,
        year,             -- Add year
        month,            -- Add month
        AVG(avgtemp_c) AS avg_temp_month,
        MAX(maxtemp_c) AS max_temp_month,
        MIN(mintemp_c) AS min_temp_month,
        AVG(maxwind_kph) AS avg_maxwind_month,
        AVG(totalprecip_mm) AS avg_totalprecip_month,
        AVG(avghumidity) AS avg_humidity_month,
        AVG(uv_index) AS avg_uv_index_month,
        AVG(avgvis_km) AS avg_visibility_month,
        SUM(totalsnow_cm) AS total_snow_month,
        AVG(CASE WHEN will_it_rain = 1 THEN 1 ELSE 0 END) AS avg_rain_probability,
        AVG(chance_of_rain) AS avg_chance_of_rain
    FROM {{ ref("prep_temp") }}
    GROUP BY city, region, country, year, month, lat, lon
)
SELECT * FROM total_avg_month;

