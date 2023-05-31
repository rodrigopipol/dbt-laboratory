with stg_test_3 as (
    select
         orden
        ,id
        ,SUM(amount) as monto
    from {{ ref('stg_test_3') }}
    group by 1, 2
)

select
    orden
    ,id
    ,monto
from stg_test_3