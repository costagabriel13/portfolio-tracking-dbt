{% snapshot SNSH_COUNTRY_INFO %}

{{
    config(
      unique_key= 'COUNTRY_HKEY',
      strategy='check',
      check_cols=['COUNTRY_HDIFF'],
    )
}}

select * from {{ ref('STG_COUNTRY_INFO') }}

{% endsnapshot %}