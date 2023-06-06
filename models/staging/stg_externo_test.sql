select *
from {{ source('test', 'cl000_gcs_auditsa_cl_santiago_test_dbt') }}