select
    cast(date as datetime) as date,
    plant,
    part_number,
    quantity
from {{source("vehicles", "plant_data")}}