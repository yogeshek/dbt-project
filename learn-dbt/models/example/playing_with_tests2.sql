
{{config(materialized='view')}}

SELECT SUM(c_acctbal) AS sum_c_acctbal FROM {{ref('playing_with_tests')}}
group by c_mktsegment HAVING SUM(c_acctbal) >= 100000000
