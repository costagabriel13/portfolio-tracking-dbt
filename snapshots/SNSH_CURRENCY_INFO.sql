{% snapshot SNSH_CURRENCY_INFO %}

{{
    config(
      unique_key= 'CURRENCY_HKEY',
      strategy='check',
      check_cols=['CURRENCY_HDIFF'],
    )
}}

select * from {{ ref('STG_CURRENCY_INFO') }}

{% endsnapshot %}