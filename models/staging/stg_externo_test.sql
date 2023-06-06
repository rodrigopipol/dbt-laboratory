select *
from {{ source('test', 'src_external_test') }}