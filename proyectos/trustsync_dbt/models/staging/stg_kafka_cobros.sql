-- models/staging/stg_kafka_cobros.sql
-- Transforma eventos crudos de Kafka al formato del pipeline TrustSync

{{ config(materialized='view') }}

with kafka_raw as (
    select * from {{ ref('eventos_procesados') }}
),

clasificados as (
    select
        id_recibo,
        numsituarecib,
        motivo,
        cobrador,
        timestamp,

        -- Clasificación de eventos igual que el consumer Python
        case
            when motivo = 'PA' and cobrador = 'ci' then 'BLOQUEADO'
            when motivo = 'CB'                     then 'COBRO'
            when motivo = 'DB'                     then 'DEBITO'
            when motivo = 'PB'                     then 'PAGO_BASE'
            else 'DESCONOCIDO'
        end as estado_evento,

        -- Flag de riesgo para TrustSync
        case
            when motivo = 'PA' and cobrador = 'ci' then 1
            else 0
        end as flag_riesgo

    from kafka_raw
)

select * from clasificados