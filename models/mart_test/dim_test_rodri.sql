with test_1 as (
    select 
        CAST(id AS int) as id
        ,id as id_str
    from {{ ref('stg_test_1') }}
),   test_2 as (
    select 
        CAST(id AS int) as id
        ,id as id_str
    from {{ ref('stg_test_2') }}
), stg_dim as (
    select id, id_str
    from test_1
    union all
    select id, id_str
    from test_2
), stg_fct as (
    select
        id
        ,sum(monto) as monto_tot
    from {{ ref('fct_test_rodri') }}
    group by 1
)

select 
     d.id
    ,d.id_str
    ,f.monto_tot
from stg_dim as d
inner join stg_fct as f
    on d.id_str = f.id



