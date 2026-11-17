-- Unicité de (symbol, trade_date) dans la table de faits. Échoue s'il renvoie des lignes.
select symbol, trade_date, count(*) as nb
from {{ ref('fct_daily_klines') }}
group by symbol, trade_date
having count(*) > 1
