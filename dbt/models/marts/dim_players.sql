{{
    config(
        materialized="table",
        table_type="iceberg",
        format="parquet",
    )
}}

select 1 as x
