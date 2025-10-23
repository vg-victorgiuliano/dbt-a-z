with pagar as(
    select * from {{ source('CASHIN_CASHOUT', 'pagar_completa') }}
),

final as(
    select
        cast(bukrs as string) as company_key
        , cast(fiscper as string) as exercicio
        , cast(belnr as string) as n_documento
        , cast(buzei as string) as n_item
        , cast(uposz as string) as n_subitem
        , cast(statusps as string) as status_fi
        , cast(lifnr as string) as chave_fornecedor
        , cast(kkber as string) as area_controlefiscal
        , cast(maber as string) as area_advertencia
	    , cast(koart as string) as tipo_conta
        , cast(umskz as string) as codigo_razaoespecial
        , cast(blart as string) as tipo_documento
        , cast(bschl as string) as chave_lancamento
	    , cast(fiscvar as string) as variante_execicio
--        , cast(PARSE_DATE('%m/%d/%Y', bldat) as date) as data_documento
--        , cast(PARSE_DATE('%m/%d/%Y', budat) as date) as data_lancamento
--        , cast(PARSE_DATE('%m/%d/%Y', cpudt) as date) as data_entrada
--        , cast(PARSE_DATE('%m/%d/%Y', augdt) as date) as data_compensacao
--        , cast(PARSE_DATE('%m/%d/%Y', madat) as date) as data_ultimaadvertencia
--        , cast(PARSE_DATE('%m/%d/%Y', netdt) as date) as data_vencimentoliquido
--        , cast(PARSE_DATE('%m/%d/%Y', sk1dt) as date) as data_vencimentodesconto1
--        , cast(PARSE_DATE('%m/%d/%Y', sk2dt) as date) as data_vencimentodesconto2
--        , cast(PARSE_DATE('%m/%d/%Y', zfbdt) as date) as data_base_calculovencimento
	    , cast(zbd1t as integer) as dias_desconto1
	    , cast(zbd2t as integer) as dias_desconto2
	    , cast(zbd3t as integer) as prazo_condicaoliquida
	    , cast(zbd1p as numeric) as taxa_desconto1
	    , cast(zbd2p as numeric) as taxa_desconto2
	    , cast(land1 as string) as chave_pais
	    , cast(zlsch as string) as forma_pagamento
	    , cast(zterm as string) as chave_condicaopagamento
	    , cast(zlspr as string) as chave_bloqueiopagamento
	    , cast(rstgr as string) as motivo_diferencapagamento
	    , cast(mansp as string) as bloqueio_advertencia
	    , cast(mschl as string) as chave_advertencia
	    , cast(manst as string) as nivel_advertencia
	    , cast(lcurr as string) as chave_minterna
	    , cast(replace(regexp_replace(dmsol, r'[^0-9.,]', ''), ',', '.') as numeric) as montante_debito_minterna
	    , cast(replace(regexp_replace(dmhab, r'[^0-9.,]', ''), ',', '.') as numeric) as montante_credito_minterna 
	    , cast(replace(regexp_replace(dmshb, r'[^0-9.,]', ''), ',', '.') as numeric) as montante_total_minterna
	    , cast(replace(regexp_replace(sknto, r'[^0-9.,]', ''), ',', '.') as numeric) as montante_desconto_minterna
	    , cast(waers as string) as chave_mdoc
	    , cast(replace(regexp_replace(wrsol, r'[^0-9.,]', ''), ',', '.') as numeric) as montante_debito_mdoc
	    , cast(replace(regexp_replace(wrhab, r'[^0-9.,]', ''), ',', '.') as numeric) as montante_credito_mdoc 
	    , cast(replace(regexp_replace(wrshb, r'[^0-9.,]', ''), ',', '.') as numeric) as montante_total_mdoc
	    , cast(replace(regexp_replace(skfbt, r'[^0-9.,]', ''), ',', '.') as numeric) as montante_comdireitoadesconto_mdoc
	    , cast(replace(regexp_replace(wskto, r'[^0-9.,]', ''), ',', '.') as numeric) as montante_desconto_mdoc
	    , cast(ktopl as string) as plano_contas
	    , cast(hkont as string) as conta_razao_contabilidade
	    , cast(saknr as string) as n_conta_razao
	    , cast(filkd as string) as n_conta_filial
	    , cast(augbl as string) as n_documento_compensacao
	    , cast(xblnr as string) as n_documento_referencia
	    , cast(rebzg as string) as n_documento_fatura
	    , cast(rebzj as integer) as exercicio_fatura
	    , cast(rebzz as string) as n_item_fatura
	    , cast(vbeln as string) as n_documento_venda
	    , cast(xref1 as string) as chave_referencia_parceiro1
	    , cast(xref2 as string) as chave_referencia_parceiro2
		, cast(left(xref2, 2) as string) as cultura
		, cast(right(xref2, 2) as string) as safra
		, cast(xref3 as string) as chave_referencia_parceiro3
	    , cast(sgtxt as string) as texto_item
	    , cast(xnegp as string) as codigo_lancamento_negativo
	    , cast(xarch as string) as codigo_documento_arquivado
	    , cast(umsks as string) as classe_razaoespecial
	    , cast(updmod as string) as procedimento_delta_bw
	    , cast(zuonr as string) as n_atribuicao
	    , cast(awtyp as string) as operacao_referencia
	    , cast(awkey as string) as chave_referencia
	    , cast(bstat as string) as status_documento
		, cast(replace(regexp_replace(dmbtr, r'[^0-9.,]', ''), ',', '.') as numeric) as montante_mint1
		, cast(replace(regexp_replace(dmbe2, r'[^0-9.,]', ''), ',', '.') as numeric) as montante_mint2
		, cast(replace(regexp_replace(dmbe3, r'[^0-9.,]', ''), ',', '.') as numeric) as montante_mint3
	    , cast(gjahr as integer) as ano_exercicio
	    , cast(hwae2 as string) as chave_mint2
	    , cast(hwae3 as string) as chave_mint3
	    , cast(monat as string) as mes_exercicio
	    , cast(projk as string) as elemento_pep
	    , cast(shkzg as string) as codigo_debitocredito
	    , cast(replace(regexp_replace(wrbtr, r'[^0-9.,]', ''), ',', '.') as numeric) as montante_mdoc1
        , cast(zzgsber as string) as divisao
        , cast(zzwerks as string) as centro
        , cast(zzstblg as string) as n_docestorno
        , cast(zzsegment as string) as segmento
        , cast(zzprctr as string) as centro_lucro
        , cast(zzkursf as string) as taxa_cambio
        , cast(odq_changemode as string) as modo_modificacao
        , cast(odq_entitycntr as string) as entidade
    from pagar
)

select * from final


