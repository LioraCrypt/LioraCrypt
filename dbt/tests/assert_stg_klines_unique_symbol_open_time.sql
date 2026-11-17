-- Unicité de (symbol, open_time) dans le staging : le test échoue s'il renvoie des lignes.
select
    symbol,
    open_time,
    count(*) as nb
from {{ ref('stg_binance__klines') }}
group by symbol, open_time
having count(*) > 1
