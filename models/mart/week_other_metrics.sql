WITH total_avg_week AS (
    SELECT 
        city,
        region,
        country,
        lat,
        lon,
        DATE_TRUNC('week', date) AS week_start,  -- Adjusting to a weekly grouping
        AVG(avgtemp_c) AS avg_temp_week,
        MAX(maxtemp_c) AS max_temp_week,
        MIN(mintemp_c) AS min_temp_week,
        AVG(maxwind_kph) AS avg_maxwind_week,
        AVG(totalprecip_mm) AS avg_totalprecip_week,
        AVG(avghumidity) AS avg_humidity_week,
        AVG(uv_index) AS avg_uv_index_week,
        AVG(avgvis_km) AS avg_visibility_week,
        SUM(totalsnow_cm) AS total_snow_week,
        AVG(CASE WHEN will_it_rain = 1 THEN 1 ELSE 0 END) AS avg_rain_probability,
        AVG(chance_of_rain) AS avg_chance_of_rain
    FROM {{ ref("prep_temp") }}
    GROUP BY city, region, country, lat, lon, DATE_TRUNC('week', date_field)
)
SELECT * FROM total_avg_week
