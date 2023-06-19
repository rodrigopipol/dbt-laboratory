select
     id_cli_cliente
	,id_tie_fecha_valor
	,CAST(NULL AS INT64)     AS id_tie_fecha_valor_utc
	,CAST(NULL AS DATETIME)  AS desc_tie_fecha_tiempo
	,CAST(NULL AS TIMESTAMP) AS desc_tie_fecha_tiempo_utc
	,id_ga_vista
	,web_property_id
	,profile_id
	,'No Aplica' AS account_id
	,ga_countryisocode
	,ga_sourcemedium
	,ga_campaign
	,'No Aplica' AS ga_adcontent
	,CAST(NULL AS INT64) AS fc_transacciones_cant
	,CAST(NULL AS INT64) AS fc_ingreso_producto_monto
	,CAST(NULL AS INT64) AS fc_producto_agregado_carrito_cant
	,CAST(NULL AS INT64) AS fc_transacciones_monto
	,CAST(NULL AS INT64) AS fc_producto_cant
	,CAST(NULL AS INT64) AS fc_vista_detalle_producto_cant
	,desc_pagina_destino
	,desc_ruta_pag_destino
	,CAST(NULL AS INT64) AS fc_visualizaciones_pag_cant
	,fc_sesiones_cant
	,fc_rebotes_cant
	,fc_duracion_sesion_seg				
	,CAST(NULL AS INT64) AS fc_ingresos_monto
	,CAST(NULL AS INT64) AS fc_signup_cant
	,CAST(NULL AS INT64) AS fc_signup_inicio_cant
	,CAST(NULL AS INT64) AS fc_deposito_cant
	,CAST(NULL AS INT64) AS fc_deposito_inicio_cant
	,CAST(NULL AS INT64) AS fc_login_cant
	,CAST(NULL AS INT64) AS fc_login_inicio_cant
from {{ source('staging', 'vw_ga4_adquisition_agr_fecha') }}