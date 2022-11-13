
{{config(materialized='ephemeral')}}

select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."CUSTOMER"
