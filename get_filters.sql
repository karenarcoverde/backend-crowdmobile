SELECT jsonb_agg(results) 
FROM (
    SELECT jsonb_build_object('column_name', 'TEST_CARRIER_A', 'unique_values', 
        (SELECT jsonb_agg(DISTINCT "TEST_CARRIER_A") 
         FROM android_extracts WHERE "TEST_CARRIER_A" IS NOT NULL AND "TEST_CARRIER_A" != '' AND "TEST_CARRIER_A" != ' ')) AS results 
    UNION ALL 
    SELECT jsonb_build_object('column_name', 'BRAND', 'unique_values', 
        (SELECT jsonb_agg(DISTINCT "BRAND") 
         FROM android_extracts WHERE "BRAND" IS NOT NULL AND "BRAND" != '' AND "BRAND" != ' '))
    UNION ALL 
    SELECT jsonb_build_object('column_name', 'DEVICE', 'unique_values', 
        (SELECT jsonb_agg(DISTINCT "DEVICE") 
         FROM android_extracts WHERE "DEVICE" IS NOT NULL AND "DEVICE" != '' AND "DEVICE" != ' '))
    UNION ALL 
    SELECT jsonb_build_object('column_name', 'HARDWARE', 'unique_values', 
        (SELECT jsonb_agg(DISTINCT "HARDWARE") 
         FROM android_extracts WHERE "HARDWARE" IS NOT NULL AND "HARDWARE" != '' AND "HARDWARE" != ' '))
    UNION ALL 
    SELECT jsonb_build_object('column_name', 'MODEL', 'unique_values', 
        (SELECT jsonb_agg(DISTINCT "MODEL") 
         FROM android_extracts WHERE "MODEL" IS NOT NULL AND "MODEL" != '' AND "MODEL" != ' '))
) AS combined_results
