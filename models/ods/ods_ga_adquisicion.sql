{{
    config(
        materialized='incremental',
    )
}}

{% call set_sql_header(config) %}
    DECLARE fecha_actual_int INT64 DEFAULT CAST(FORMAT_DATE('%Y%m%d', CURRENT_DATE()) AS INT64);
{% endcall %}

with source_union as (
    SELECT *
    FROM {{ ref('stg_vw_ga_adquistion') }} AS s_ga
    {% if is_incremental() %}
    WHERE EXISTS (
        WITH tmp_ods_ga AS (
            SELECT
                 id_cli_cliente
                ,id_ga_vista
                ,MAX(id_tie_fecha_valor) AS max_fecha_valor
            FROM {{ this }}
            GROUP BY 1, 2
        )
        SELECT 1
        FROM tmp_ods_ga AS tmp_ga
        WHERE
                s_ga.id_tie_fecha_valor > tmp_ga.max_fecha_valor
            AND s_ga.id_cli_cliente     = tmp_ga.id_cli_cliente
            AND s_ga.id_ga_vista        = tmp_ga.id_ga_vista
    )
    {% endif %}
    UNION ALL
    SELECT *
    FROM {{ ref('stg_vw_ga4_adquisition_agr_fecha') }} AS s_ga4
    {% if is_incremental() %}
    WHERE EXISTS (
        WITH tmp_ods_ga4 AS (
            SELECT
                 id_cli_cliente
                ,id_ga_vista
                ,MAX(id_tie_fecha_valor) AS max_fecha_valor
            FROM {{ this }}
            GROUP BY 1, 2
        )
        SELECT 1
        FROM tmp_ods_ga4 AS tmp_ga4
        WHERE
                s_ga4.id_tie_fecha_valor > tmp_ga4.max_fecha_valor
            AND s_ga4.id_cli_cliente     = tmp_ga4.id_cli_cliente
            AND s_ga4.id_ga_vista        = tmp_ga4.id_ga_vista
    )
    {% endif %}
)
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
    ,fecha_actual_int AS id_tie_aud_creacion
FROM source_union
