with source as (
    select 1 as id
    union all
    select 2 as id
    union all
    select 3 as id
)

, second_model as (
    select *
    from {{ ref('my_second_dbt_model') }}
)

, joined as (
    select
        second_model.*
        , source.id as source_id
    from second_model
    inner join source
        on second_model.id = source.id
)

select * 
from joined
