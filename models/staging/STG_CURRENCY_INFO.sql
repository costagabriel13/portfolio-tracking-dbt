{{ config(materialized='ephemeral') }}

WITH

src_data as (

    SELECT
        AlphabeticCode                  AS ALPHABETIC_CODE,     --TEXT
        NumericCode                     AS NUMERIC_CODE,        --NUMBER
        COALESCE(DecimalDigits, -1)     AS DECIMAL_DIGITS,      --NUMBER
        CurrencyName                    AS CURRENCY_NAME,       --TEXT
        COALESCE(Locations, 'Missing')  AS LOCATIONS,           --TEXT
        LOAD_TS                         AS LOAD_TS,             --TIMESTAMP_NTZ
        'SEED.CURRENCY_INFO'            AS RECORD_SOURCE
    FROM {{ source('seeds', 'CURRENCY_INFO') }}
),

default_record as (
  SELECT
    '-1'                AS ALPHABETIC_CODE,
    -1                  AS NUMERIC_CODE,   
    -1                  AS DECIMAL_DIGITS, 
    'Missing'           AS CURRENCY_NAME,  
    'Missing'           AS LOCATIONS,      
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
        CONCAT_WS('|', ALPHABETIC_CODE) AS CURRENCY_HKEY,
        CONCAT_WS('|', ALPHABETIC_CODE,
            NUMERIC_CODE,   
            DECIMAL_DIGITS, 
            CURRENCY_NAME,  
            LOCATIONS     
        ) AS CURRENCY_HDIFF,
        * EXCLUDE LOAD_TS,
        LOAD_TS AS LOAD_TS_UTC
    FROM with_default_record
)

SELECT * FROM hashed