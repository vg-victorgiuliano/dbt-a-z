with pagar as(
    select * from {{ ref('stg_pagar') }}
),
receber as(
    select * from {{ ref('stg_receber') }}
),

united as (
    select * 
        , null as id_fornecedor
        , null as pagar_moeda_int
        , null as pagar_sol_int
        , null as pagar_hab_int
        , null as pagar_sh_int
    from receber
    union all
    select *
        , null as id_cliente
        , null as receber_moeda_int
        , null as receber_sol_int
        , null as receber_hab_int
        , null as receber_sh_int
        from pagar
)

select *
from united