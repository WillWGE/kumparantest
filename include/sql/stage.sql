truncate table public.{{ params.source_table }}

;COPY INTO public.{{ params.source_table }}
FROM 's3://kumparan/article/{{ params.source_table }}.csv'
FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',');



