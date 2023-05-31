with tbl_stg as (select * from {{ ref("stg_test_4") }}) select * from tbl_stg
