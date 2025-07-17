{{ config(materialized='ephemeral') }}

WITH

src_data as (

    SELECT
        Name                            AS EXCHANGE_NAME,       --TEXT
        ID                              AS EXCHANGE_CODE,       --TEXT
        Country                         AS COUNTRY_NAME,        --TEXT
        City                            AS CITY_NAME,           --TEXT
        Zone                            AS ZONE_CODE,           --TEXT
        Delta                           AS DELTA_NUMBER,        --NUMBER
        COALESCE(DST_period, 'Missing') AS DST_PERIOD,          --TEXT
        Open                            AS OPEN_HOUR,           --TEXT
        Close                           AS CLOSE_HOUR,          --TEXT
        Lunch                           AS LUNCH_HOUR,          --TEXT
        Open_UTC                        AS OPEN_UTC_HOUR,       --TEXT
        Close_UTC                       AS CLOSE_UTC_HOUR,      --TEXT
        Lunch_UTC                       AS LUNCH_UTC_HOUR,      --TEXT
        LOAD_TS                         AS LOAD_TS,             --TIMESTAMP_NTZ
        'SEED.EXCHANGE_INFO'            AS RECORD_SOURCE
    FROM {{ source('seeds', 'EXCHANGE_INFO') }}
),

default_record as (
  SELECT
    'Missing'           AS EXCHANGE_NAME,       --TEXT
    '-1'                AS EXCHANGE_CODE,       --TEXT
    'Missing'           AS COUNTRY_NAME,        --TEXT
    'Missing'           AS CITY_NAME,           --TEXT
    '-1'                AS ZONE_CODE,           --TEXT
    -1                  AS DELTA_NUMBER,        --NUMBER
    'Missing'           AS DST_PERIOD,          --TEXT
    'Missing'           AS OPEN_HOUR,           --TEXT
    'Missing'           AS CLOSE_HOUR,          --TEXT
    'Missing'           AS LUNCH_HOUR,          --TEXT
    'Missing'           AS OPEN_UTC_HOUR,       --TEXT
    'Missing'           AS CLOSE_UTC_HOUR,      --TEXT
    'Missing'           AS LUNCH_UTC_HOUR,      --TEXT
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
        CONCAT_WS('|', EXCHANGE_CODE) AS EXCHANGE_HKEY,
        CONCAT_WS('|', EXCHANGE_NAME, 
            EXCHANGE_CODE, 
            COUNTRY_NAME,  
            CITY_NAME,     
            ZONE_CODE,     
            DELTA_NUMBER,  
            DST_PERIOD,    
            OPEN_HOUR,     
            CLOSE_HOUR,    
            LUNCH_HOUR,    
            OPEN_UTC_HOUR, 
            CLOSE_UTC_HOUR,
            LUNCH_UTC_HOUR
        ) AS EXCHANGE_HDIFF,
        * EXCLUDE LOAD_TS,
        LOAD_TS AS LOAD_TS_UTC
    FROM with_default_record
)

SELECT * FROM hashed