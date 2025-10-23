with receber as(
    select *
    from {{ ref('stg_receber') }}
)
select 
--    CASE 
--        WHEN data_vencimento <= '2018-12-31' THEN '2018-'
--        WHEN '2019-01-01' <= data_vencimento AND data_vencimento <= '2019-12-31' THEN '2019'
--        WHEN '2020-01-01' <= data_vencimento AND data_vencimento <= '2020-12-31' THEN '2020'
--        WHEN '2021-01-01' <= data_vencimento AND data_vencimento <= '2021-12-31' THEN '2021'
--        WHEN '2022-01-01' <= data_vencimento AND data_vencimento <= '2022-12-31' THEN '2022'
--        WHEN '2023-01-01' <= data_vencimento AND data_vencimento <= '2023-12-31' THEN '2023'
--        WHEN data_vencimento >= '2024-01-01' THEN '2024+'
--    END AS ano
    count(*) as id_count
    , sum(vlr_moeda_int) as valor_moeda_int
    , sum(vlr_sol_int) as valor_sol
    , sum(vlr_hab_int) as valor_hab
    , sum(vlr_sh_int) as valor_sh_int
    , sum(vlr_sol_int) - sum(vlr_hab_int) as valor_sh_teste
    , min(data_vencimento) as menor_data
    , max(data_vencimento) as maior_data
    , min(conta_razao) as min_conta
    , max(conta_razao) as max_conta
    , min(razao_especial) as min_razaoespecial
    , max(razao_especial) as max_razaoespecial
from receber
where data_compensacao is null and data_vencimento between '2012-01-01' and '2030-12-31'
--group by ano
--ORDER BY ano