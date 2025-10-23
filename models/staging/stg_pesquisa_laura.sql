

with 
    pesquisa as (
        select * from {{ source('dbt_victorg', 'pesquisa_laura') }}
    )

    , final as (
        select 
        ROW_NUMBER() over(order by Carimbo_de_data_hora) AS row_num
        , Carimbo_de_data_hora
        , Consentimento__participante
        , regiao_do_pais
        , quantos_residentes_por_ano
        , via_SUS_ou_via_saude_suplementar_setor_privado
        , idade_servico
        , linhas_de_psicoterapia__teorico
        , case 
            when linhas_de_psicoterapia__teorico like '%Psicoterapia de orientação analítica%' then true
            when linhas_de_psicoterapia__teorico  not like '%Psicoterapia de orientação analítica%' then false
        end as flg_orientacao_analitica_teorico
        , case 
            when linhas_de_psicoterapia__teorico like '%Psicoterapias cognitivo-comportamentais%' then true
            when linhas_de_psicoterapia__teorico  not like '%Psicoterapias cognitivo-comportamentais%' then false
        end as flg_cognitivo_comportamental_teorico
        , case 
            when linhas_de_psicoterapia__teorico like '%Gestalt%' then true
            when linhas_de_psicoterapia__teorico  not like '%Gestalt%' then false
        end as flg_gestault_teorico
        , case 
            when linhas_de_psicoterapia__teorico like '%Psicoterapias de família e grupos%' then true
            when linhas_de_psicoterapia__teorico  not like '%Psicoterapias de família e grupos%' then false
        end as flg_familias_e_grupos_teorico
        , case 
            when linhas_de_psicoterapia__teorico like '%Entrevista motivacional%' then true
            when linhas_de_psicoterapia__teorico  not like '%Entrevista motivacional%' then false
        end as flg_entrevista_motivacional_teorico
        , case 
            when linhas_de_psicoterapia__teorico like '%Intervenção breve%' then true
            when linhas_de_psicoterapia__teorico  not like '%Intervenção breve%' then false
        end as flg_intervencao_breve_teorico
        , case 
            when linhas_de_psicoterapia__teorico like '%Psicodrama%' then true
            when linhas_de_psicoterapia__teorico  not like '%Psicodrama%' then false
        end as flg_psicodrama_teorico
        , linha_predominante_teorico
        , linhas_de_psicoterapia_pratico
        , case 
            when linhas_de_psicoterapia_pratico like '%Psicoterapia de orientação analítica%' then true
            when linhas_de_psicoterapia_pratico  not like '%Psicoterapia de orientação analítica%' then false
        end as flg_orientacao_analitica_pratico
        , case 
            when linhas_de_psicoterapia_pratico like '%Psicoterapias cognitivo-comportamentais%' then true
            when linhas_de_psicoterapia_pratico  not like '%Psicoterapias cognitivo-comportamentais%' then false
        end as flg_cognitivo_comportamental_pratico
        , case 
            when linhas_de_psicoterapia_pratico like '%Gestalt%' then true
            when linhas_de_psicoterapia_pratico  not like '%Gestalt%' then false
        end as flg_gestault_pratico
        , case 
            when linhas_de_psicoterapia_pratico like '%Psicoterapias de família e grupos%' then true
            when linhas_de_psicoterapia_pratico  not like '%Psicoterapias de família e grupos%' then false
        end as flg_familias_e_grupos_pratico
        , case 
            when linhas_de_psicoterapia_pratico like '%Entrevista motivacional%' then true
            when linhas_de_psicoterapia_pratico  not like '%Entrevista motivacional%' then false
        end as flg_entrevista_motivacional_pratico
        , case 
            when linhas_de_psicoterapia_pratico like '%Intervenção breve%' then true
            when linhas_de_psicoterapia_pratico  not like '%Intervenção breve%' then false
        end as flg_intervencao_breve_pratico
        , case 
            when linhas_de_psicoterapia_pratico like '%Psicodrama%' then true
            when linhas_de_psicoterapia_pratico  not like '%Psicodrama%' then false
        end as flg_psicodrama_pratico
        , linha_predominante_pratico
        , segundo_ano_288_horas_previstas_treinamento_em_psicoterapia
        , terceiro_ano_288_horas_previstas_treinamento_em_psicoterapia
        , R4_de_psicoterapia
    from pesquisa
    )

select * 
from final
order by Carimbo_de_data_hora
