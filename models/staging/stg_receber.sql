with receber as(
    select * from {{ source('CASHIN_CASHOUT', 'receber_completa') }}
)
select 
    cast(kunnr as string) as id_cliente
    , cast(REPLACE(REPLACE(dmbtr, '.', ''), ',', '.')  as numeric) as receber_moeda_int
    , cast(REPLACE(REPLACE(dmsol, '.', ''), ',', '.')  as numeric) as receber_sol_int
    , cast(REPLACE(REPLACE(dmhab, '.', ''), ',', '.')  as numeric) as receber_hab_int
    , cast(REPLACE(REPLACE(dmshb, '.', ''), ',', '.') as numeric) as receber_sh_int
    , cast(netdt as date) as data_vencimento
    , cast(PARSE_DATE('%m/%d/%Y', augdt) as date) as data_compensacao
   -- , cast()
    , cast(augbl as string) as doc_compensacao
    , cast(awtyp as string) as doc_origem
    , cast(lcurr as string) as moeda
    , cast(gjahr as integer) as ano
    , cast(saknr as string) as conta_razao
    , cast(umskz as string) as razao_especial
from receber