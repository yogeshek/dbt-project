
{{config(materialized='view')}}

select * from {{ref('rwt_report_view')}}
where counts<>0
