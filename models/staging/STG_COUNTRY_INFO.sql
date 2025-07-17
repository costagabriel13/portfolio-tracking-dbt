{{ config(materialized='ephemeral') }}

WITH

src_data as (

    SELECT
        country_name                                AS COUNTRY_NAME,            --TEXT
        country_code_2_letter                       AS COUNTRY_CODE_2_LETTER,   --TEXT
        country_code_3_letter                       AS COUNTRY_CODE_3_LETTER,   --TEXT
        country_code_numeric                        AS COUNTRY_CODE_NUMERIC,    --TEXT
        iso_3166_2                                  AS ISO_3166_2,              --TEXT
        COALESCE(region    , 'Missing')             AS REGION_NAME,             --TEXT
        COALESCE(sub_region, 'Missing')             AS SUB_REGION_NAME,         --TEXT
        COALESCE(intermediate_region, 'Missing')    AS INTERMEDIATE_REGION_NAME,--TEXT
        COALESCE(region_code    , -1)               AS REGION_CODE,             --NUMERIC
        COALESCE(sub_region_code, -1)               AS SUB_REGION_CODE,         --NUMERIC
        COALESCE(intermediate_region_code, -1)      AS INTERMEDIATE_REGION_CODE,--NUMERIC
        LOAD_TS                                     AS LOAD_TS,                 --TIMESTAMP_NTZ
        'SEED.COUNTRY_INFO'                         AS RECORD_SOURCE
    FROM {{ source('seeds', 'COUNTRY_INFO') }}
),

default_record as (
  SELECT
    'Missing'           AS COUNTRY_NAME,            
    '-1'                AS COUNTRY_CODE_2_LETTER,   
    '-1'                AS COUNTRY_CODE_3_LETTER,   
    '-1'                AS COUNTRY_CODE_NUMERIC,    
    '-1'                AS ISO_3166_2,              
    'Missing'           AS REGION_NAME,             
    'Missing'           AS SUB_REGION_NAME,         
    'Missing'           AS INTERMEDIATE_REGION_NAME,
    -1                  AS REGION_CODE,             
    -1                  AS SUB_REGION_CODE,         
    -1                  AS INTERMEDIATE_REGION_CODE,
    '2020-01-01'        AS LOAD_TS,                 
    'System.DefaultKey' AS RECORD_SOURCE
),

with_default_record as(
    SELECT * FROM src_data
    UNION ALL
    SELECT * FROM default_record
),

hashed as (
    SELECT
        CONCAT_WS('|', COUNTRY_CODE_3_LETTER) AS COUNTRY_HKEY,
        CONCAT_WS('|', COUNTRY_NAME,
            COUNTRY_CODE_2_LETTER, COUNTRY_CODE_3_LETTER, 
            COUNTRY_CODE_NUMERIC, ISO_3166_2, REGION_NAME,
            SUB_REGION_NAME, INTERMEDIATE_REGION_NAME,
            REGION_CODE, SUB_REGION_CODE, INTERMEDIATE_REGION_CODE
        ) AS COUNTRY_HDIFF,
        * EXCLUDE LOAD_TS,
        LOAD_TS AS LOAD_TS_UTC
    FROM with_default_record
)

SELECT * FROM hashed