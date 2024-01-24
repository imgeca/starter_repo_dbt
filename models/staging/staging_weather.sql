WITH temperature_daily AS (
    SELECT 
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'date')::VARCHAR)::date AS date,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'maxtemp_c')::VARCHAR)::FLOAT AS maxtemp_c,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'mintemp_c')::VARCHAR)::FLOAT AS mintemp_c,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'avgtemp_c')::VARCHAR)::FLOAT AS avgtemp_c,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'maxwind_kph')::VARCHAR)::FLOAT AS maxwind_kph,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'totalprecip_mm')::VARCHAR)::FLOAT AS totalprecip_mm,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'avghumidity')::VARCHAR)::FLOAT AS avghumidity,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'uv')::VARCHAR)::FLOAT AS uv_index,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'avgvis_km')::VARCHAR)::FLOAT AS avgvis_km,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'totalsnow_cm')::VARCHAR)::FLOAT AS totalsnow_cm,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'daily_will_it_rain')::VARCHAR)::INT AS will_it_rain,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'daily_chance_of_rain')::VARCHAR)::INT AS chance_of_rain,
        (extracted_data -> 'location' -> 'name')::VARCHAR AS city,
        (extracted_data -> 'location' -> 'region')::VARCHAR AS region,
        (extracted_data -> 'location' -> 'country')::VARCHAR AS country,
        ((extracted_data -> 'location' -> 'lat')::VARCHAR)::NUMERIC AS lat, 
        ((extracted_data -> 'location' -> 'lon')::VARCHAR)::NUMERIC AS lon
    FROM {{source("staging", "raw_temp")}})
SELECT * 
FROM temperature_daily
