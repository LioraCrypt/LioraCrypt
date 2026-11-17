-- Cohérence des prix : high est le plus haut, low le plus bas, prix et volumes positifs.
-- Le test échoue s'il renvoie des lignes.
select *
from {{ ref('stg_binance__klines') }}
where high_price < greatest(open_price, close_price, low_price)
   or low_price  > least(open_price, close_price, high_price)
   or low_price <= 0
   or base_volume < 0
   or close_time <= open_time
