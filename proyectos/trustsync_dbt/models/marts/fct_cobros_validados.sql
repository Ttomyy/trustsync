{{ config(
    materialized='table'
) }}

-- Referenciando directamente a seeds para entorno de desarrollo/pruebas locales
with movimientos_recib as (

    select
        idrecibo as id_recibo,
        numsituarecib as num_situa_recib,
        motivo,
        cobrador
    from {{ ref('movimientos_recib') }}

),

cobros_r as (

    select
        idrecibo as id_recibo,
        numsituarecib_cb as num_situa_recib_cb,
        cobrador
    from {{ ref('cobros_r') }}

),

movimientos_ordenados as (

    select
        id_recibo,
        num_situa_recib,
        motivo,
        cobrador,
        max(case when motivo = 'DB' then num_situa_recib else null end) 
            over (
                partition by id_recibo 
                order by num_situa_recib
                rows between unbounded preceding and current row
            ) as num_situa_recib_db_previo
    from movimientos_recib

),

movimientos_cb_validados as (

    select
        m.id_recibo,
        m.num_situa_recib,
        m.num_situa_recib_db_previo,
        count(case when sub.motivo = 'PB' then 1 end) as count_pb_in_range,
        count(case when sub.motivo = 'PA' and sub.cobrador <> 'ci' then 1 end) as count_pa_blocker_in_range
    from movimientos_ordenados as m
    left join movimientos_recib as sub
        on m.id_recibo = sub.id_recibo
        and sub.num_situa_recib > m.num_situa_recib_db_previo
        and sub.num_situa_recib < m.num_situa_recib
    where m.motivo = 'CB'
      -- Protección explícita para evitar producto cartesiano o joins abiertos si no hay DB previo
      and m.num_situa_recib_db_previo is not null
    group by
        m.id_recibo,
        m.num_situa_recib,
        m.num_situa_recib_db_previo

),

final as (

    select
        c.id_recibo,
        c.num_situa_recib_cb,
        c.cobrador,
        case
            when v.num_situa_recib_db_previo is not null
             and v.count_pb_in_range > 0
             and v.count_pa_blocker_in_range = 0
            then 1
            else 0
        end as indicador_marca
    from cobros_r as c
    -- LEFT JOIN para preservar todos los cobros, asignando 0 a los que no tienen DB previo
    left join movimientos_cb_validados as v
        on c.id_recibo = v.id_recibo 
       and c.num_situa_recib_cb = v.num_situa_recib

)

select 
    id_recibo,
    num_situa_recib_cb,
    cobrador,
    coalesce(indicador_marca, 0) as indicador_marca
from final