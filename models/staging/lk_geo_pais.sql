select *
from {{ source('dims', 'lk_geo_pais') }}