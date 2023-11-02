SELECT 
    table_name,
    jsonb_build_object(
        'table_name', table_name,
        'columns_name', array_agg(column_name)
    ) as table_info
FROM 
    information_schema.columns 
WHERE 
    table_schema = 'public'
GROUP BY 
    table_name