WITH CombinedData AS (
    SELECT 
        uav.country,
        uav."game app",
        uav.createdat,  
        ai.cpi,
        (CAST(SUM(uav.install) OVER (PARTITION BY uav.country, uav."game app" ORDER BY uav.createdat) AS REAL) * 1000) / 
           ROW_NUMBER() OVER (PARTITION BY uav.country, uav."game app" ORDER BY uav.createdat) AS ipm,
        DATE(uav.createdat) AS view_date  
    FROM 
        user_ad_views uav
    JOIN 
        advertisers_info ai ON uav.country = ai.country AND uav.advertiser = ai.advertiser
), RankedData AS ( 
  SELECT
        country,
        "game app",
        cpi,
        ipm,
        createdat,
        view_date,
        ROW_NUMBER() OVER (PARTITION BY country, "game app", view_date ORDER BY createdat) as rn
    FROM CombinedData
), SlidingWindowData AS (
    SELECT 
        r1.country,
        r1."game app",
        r1.view_date, 
        EXTRACT(HOUR FROM r1.createdat) AS view_hour,
        AVG(r2.cpi) AS avg_cpi,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY r2.cpi) AS median_cpi,
        AVG(r2.ipm) AS avg_ipm,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY r2.ipm) AS median_ipm
    FROM 
        RankedData r1
    INNER JOIN RankedData r2 
        ON r1.country = r2.country
        AND r1."game app" = r2."game app"
        AND r1.view_date = r2.view_date 
        AND r2.createdat BETWEEN r1.createdat - INTERVAL '23 hour' AND r1.createdat 
    GROUP BY 
        r1.country,
        r1."game app",
        r1.view_date,
        EXTRACT(HOUR FROM r1.createdat)
)
SELECT 
    country,
    "game app",
    view_date,
    view_hour,  
    avg_cpi,
    median_cpi,
    avg_ipm,
    median_ipm
FROM 
    SlidingWindowData
ORDER BY 
    country, 
    "game app", 
    view_date, 
    view_hour;