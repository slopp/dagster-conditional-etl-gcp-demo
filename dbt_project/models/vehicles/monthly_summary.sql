{{ config(materialized='table') }}

select 
     {{ date_trunc("month", "date") }} as month,
     plant,
     part_number,
     sum(quantity) as quantity
from {{ref("plant_data_cleaned")}}
group by 1,2,3
