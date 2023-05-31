select *
from {{ source('test', 'test_table') }}