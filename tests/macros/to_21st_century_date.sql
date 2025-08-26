WITH

test_data AS (
    SELECT '0021-09-23'::date AS src_date,
            '2021-09-23':: AS expected_date
    UNION
    SELECT '1021-09-24', '1021-09-24'

  UNION

  SELECT '2021-09-25', '2021-09-25'

  UNION

  SELECT '-0021-09-26', '1979-09-26'
)

SELECT
    {{to_21st_century_date('src_date')}} AS ok_date,
    expected_date,
    ok_date = expected_date AS matchin
FROM test_data

WHERE NOT matching