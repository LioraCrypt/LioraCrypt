-- stg_binance__klines
-- Une ligne par bougie journalière clôturée, par paire.
-- Source : raw.klines_raw (une ligne par paire et par exécution, payload = texte JSON
-- d'un tableau de bougies Binance à 12 champs, lus par position).
-- Règles : typage, dédoublonnage sur (symbol, open_time), filtre des bougies non clôturées.

with source as (

    select
        run_id,
        symbol,
        kline_interval,
        requested_at,
        loaded_at,
        payload
    from {{ source('binance', 'klines_raw') }}

),

-- 1er niveau : le texte payload devient un tableau JSON, UNNEST crée une ligne par bougie.
bougies as (

    select
        source.run_id,
        source.symbol,
        source.kline_interval,
        source.requested_at,
        source.loaded_at,
        bougie
    from source
    cross join unnest(json_query_array(source.payload)) as bougie

),

-- 2e niveau : chaque bougie est un tableau [open_time, open, high, low, close, volume,
-- close_time, quote_asset_volume, number_of_trades, taker_buy_base_volume,
-- taker_buy_quote_volume, ignore]. JSON_VALUE enlève les guillemets, CAST type la valeur.
-- Binance envoie les prix/volumes en texte : NUMERIC garde les 8 décimales sans arrondi.
-- Le champ "ignore" (toujours "0") n'est pas repris.
typees as (

    select
        symbol,
        kline_interval,
        timestamp_millis(cast(json_value(bougie, '$[0]') as int64))  as open_time,
        timestamp_millis(cast(json_value(bougie, '$[6]') as int64))  as close_time,
        cast(json_value(bougie, '$[1]') as numeric)                  as open_price,
        cast(json_value(bougie, '$[2]') as numeric)                  as high_price,
        cast(json_value(bougie, '$[3]') as numeric)                  as low_price,
        cast(json_value(bougie, '$[4]') as numeric)                  as close_price,
        cast(json_value(bougie, '$[5]') as numeric)                  as base_volume,
        cast(json_value(bougie, '$[7]') as numeric)                  as quote_volume,
        cast(json_value(bougie, '$[8]') as int64)                    as number_of_trades,
        cast(json_value(bougie, '$[9]') as numeric)                  as taker_buy_base_volume,
        cast(json_value(bougie, '$[10]') as numeric)                 as taker_buy_quote_volume,
        run_id,
        requested_at,
        loaded_at
    from bougies

)

select *
from typees
-- Bougies clôturées seulement : Binance renvoie aussi la bougie du jour en cours.
-- On compare à requested_at (moment de l'appel) et non à l'heure actuelle : une bougie
-- en cours à l'appel a des valeurs partielles, même si elle est clôturée aujourd'hui.
where close_time < requested_at
-- Dédoublonnage : une bougie peut revenir dans plusieurs exécutions ; on garde la plus récente.
qualify row_number() over (
    partition by symbol, open_time
    order by requested_at desc, loaded_at desc
) = 1
