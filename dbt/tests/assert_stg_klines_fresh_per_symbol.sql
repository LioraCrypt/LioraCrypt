-- Fraîcheur par paire : chaque paire attendue doit avoir une bougie clôturée récente.
-- Renvoie une ligne par paire absente ou périmée.
-- Seuil provisoire (variable freshness_max_hours, défaut 48 h) : seuil définitif à fixer.
-- En avertissement tant que l'ingestion quotidienne n'est pas automatisée.
{{ config(severity = 'warn') }}

with attendues as (
    select symbol
    from unnest(['BTCUSDT', 'BTCEUR', 'ETHUSDT', 'ETHEUR', 'SOLUSDT', 'SOLEUR']) as symbol
),

derniere as (
    select symbol, max(close_time) as derniere_cloture
    from {{ ref('stg_binance__klines') }}
    group by symbol
)

select
    attendues.symbol,
    derniere.derniere_cloture
from attendues
left join derniere using (symbol)
where derniere.derniere_cloture is null
   or derniere.derniere_cloture
      < timestamp_sub(current_timestamp(), interval {{ var('freshness_max_hours', 48) }} hour)
