{{
    config(
        materialized='incremental',
        pre_hook="DELETE FROM {{ this }} WHERE id_tie_aud_creacion = CAST(FORMAT_DATE('%Y%m%d', CURRENT_DATE()) AS INT64) ;",        
    )
}}

{% call set_sql_header(config) %}
    DECLARE fecha_actual_int INT64 DEFAULT CAST(FORMAT_DATE('%Y%m%d', CURRENT_DATE()) AS INT64);
    DECLARE sk_valor_no_aplica INT64 DEFAULT FARM_FINGERPRINT('0');
{% endcall %}

with tmp_ods_adquisicion as (
    SELECT
		 id_cli_cliente
        ,id_tie_fecha_valor
        ,id_tie_fecha_valor_utc
        ,desc_tie_fecha_tiempo
        ,desc_tie_fecha_tiempo_utc
        ,id_ga_vista
        ,web_property_id
        ,profile_id
        ,account_id
        ,ga_countryisocode
        ,ga_sourcemedium
        ,ga_campaign
        ,ga_adcontent
        ,fc_transacciones_cant
        ,fc_ingreso_producto_monto
        ,fc_producto_agregado_carrito_cant
        ,fc_transacciones_monto
        ,fc_producto_cant
        ,fc_vista_detalle_producto_cant
        ,desc_pagina_destino
        ,desc_ruta_pag_destino
        ,fc_visualizaciones_pag_cant
        ,fc_sesiones_cant
        ,fc_rebotes_cant
        ,fc_duracion_sesion_seg
        ,fc_ingresos_monto
        ,fc_signup_cant
        ,fc_signup_inicio_cant
        ,fc_deposito_cant
        ,fc_deposito_inicio_cant
        ,fc_login_cant
        ,fc_login_inicio_cant
        ,id_tie_aud_creacion
	FROM {{ ref('ods_ga_adquisicion') }}
    {% if is_incremental() %}
    WHERE id_tie_aud_creacion = fecha_actual_int
    {% endif %}
), lk_ga_fuente_medio_2 as (
    SELECT DISTINCT 
         id_cli_cliente
        ,desc_ga_fuente_medio
        ,first_value(id_ga_fuente_medio) OVER (PARTITION BY id_cli_cliente, desc_ga_fuente_medio ORDER BY desc_tie_fecha_desde ASC) AS id_ga_fuente_medio
    FROM {{ ref('lk_ga_fuente_medio') }}
)
select
    vw_adq.id_tie_fecha_valor,
	IFNULL(id_tie_fecha_valor_utc,0) id_tie_fecha_valor_utc,
	vw_adq.id_cli_cliente, 
	vw_adq.id_ga_vista, 
	IFNULL(pais.id_geo_pais, sk_valor_no_aplica) AS id_geo_pais, 
	IFNULL(pd.id_ga_pagina_destino, sk_valor_no_aplica) AS id_ga_pagina_destino,
	vw_adq.desc_ruta_pag_destino, 
	COALESCE(fm.id_ga_fuente_medio, fm2.id_ga_fuente_medio , sk_valor_no_aplica) id_ga_fuente_medio, 
	CASE WHEN UPPER(vw_adq.ga_sourcemedium) = 'GOOGLE / CPC' AND vw_adq.ga_campaign = '(not set)' THEN 1658707429783083819 ELSE IFNULL(camp.id_camp_campania, sk_valor_no_aplica) END AS id_camp_campania,
	IFNULL(anu.id_ga_contenido_anuncio , sk_valor_no_aplica) AS id_ga_contenido_anuncio, 
	sk_valor_no_aplica AS id_ga_canal, -- por ver (fuente medio y campania)
	vw_adq.desc_tie_fecha_tiempo, 
	IFNULL(desc_tie_fecha_tiempo_utc, NULL) desc_tie_fecha_tiempo_utc,			
	vw_adq.fc_ingreso_producto_monto, 
	vw_adq.fc_producto_agregado_carrito_cant, 
	vw_adq.fc_transacciones_monto,
	vw_adq.fc_producto_cant, 
	vw_adq.fc_vista_detalle_producto_cant,	
	vw_adq.fc_sesiones_cant,  
	vw_adq.fc_visualizaciones_pag_cant, 
	vw_adq.fc_rebotes_cant, 
	vw_adq.fc_duracion_sesion_seg,
	vw_adq.fc_transacciones_cant, 
	vw_adq.fc_ingresos_monto,
	fecha_actual_int AS id_tie_aud_creacion, 
	fecha_actual_int AS id_tie_aud_actualizacion,
	vw_adq.fc_signup_cant AS fc_signup_cant,
	vw_adq.fc_signup_inicio_cant AS fc_signup_inicio_cant,
	vw_adq.fc_deposito_cant AS fc_deposito_cant,
	vw_adq.fc_deposito_inicio_cant AS fc_deposito_inicio_cant,
	vw_adq.fc_login_cant AS fc_login_cant,
	vw_adq.fc_login_inicio_cant AS fc_login_inicio_cant 
from tmp_ods_adquisicion as vw_adq
LEFT JOIN {{ ref('lk_camp_campania') }} camp 
	ON camp.desc_camp_campania = vw_adq.ga_campaign 
	AND camp.id_cli_cliente = vw_adq.id_cli_cliente
	AND (camp. desc_camp_origen = 'GA' OR camp. desc_camp_origen = 'GA4')
LEFT JOIN {{ ref('lk_geo_pais') }} pais 
	ON pais.desc_cod_geo_pais = vw_adq.ga_countryisocode 
LEFT JOIN {{ ref('lk_ga_fuente_medio') }} fm 
	ON fm.desc_ga_fuente_medio = vw_adq.ga_sourcemedium 
	AND fm.id_cli_cliente = vw_adq.id_cli_cliente 
	AND vw_adq.id_tie_fecha_valor >= fm.desc_tie_fecha_desde AND vw_adq.id_tie_fecha_valor < fm.desc_tie_fecha_hasta
LEFT JOIN lk_ga_fuente_medio_2 fm2
    ON fm2.desc_ga_fuente_medio = vw_adq.ga_sourcemedium
    AND fm2.id_cli_cliente = vw_adq.id_cli_cliente
LEFT JOIN {{ ref('lk_ga_pagina_destino') }} pd 
	ON pd.desc_ga_pagina_destino = vw_adq.desc_pagina_destino
	AND pd.id_cli_cliente = vw_adq.id_cli_cliente 
LEFT JOIN {{ ref('lk_ga_contenido_anuncio') }} anu 
	ON anu.desc_ga_contenido_anuncio = vw_adq.ga_adcontent
	AND anu.id_cli_cliente = vw_adq.id_cli_cliente