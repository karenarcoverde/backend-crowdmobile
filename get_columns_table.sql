SELECT jsonb_build_object('table_name', 'android_extracts','columns_name', array_agg(column_name)) 
FROM information_schema.columns 
WHERE table_name = 'android_extracts'