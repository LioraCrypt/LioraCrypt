-- Les corrélations sont entre -1 et 1, et chaque devise a ses 3 couples par fenêtre
-- (BTC-ETH, BTC-SOL, ETH-SOL). Échoue s'il renvoie des lignes.
select 'hors_bornes' as probleme, quote_asset, window_days, symbol_a, symbol_b
from {{ ref('fct_return_correlations') }}
where correlation < -1.0000001 or correlation > 1.0000001

union all

select 'couples_manquants', quote_asset, window_days, cast(count(*) as string), cast(null as string)
from {{ ref('fct_return_correlations') }}
group by quote_asset, window_days
having count(*) != 3
