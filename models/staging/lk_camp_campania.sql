select *
from {{ source('dims', 'lk_camp_campania') }}