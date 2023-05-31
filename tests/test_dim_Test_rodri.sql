with src_dim as (
    select *
    from {{ ref('dim_test_rodri') }}
)
select *
from src_dim
where id = 50