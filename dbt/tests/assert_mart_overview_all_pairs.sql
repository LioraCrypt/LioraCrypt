-- La synthèse contient exactement les 6 paires, chacune avec une dernière bougie.
-- Échoue s'il renvoie des lignes.
select 'nb_paires' as probleme, cast(count(*) as string) as detail
from {{ ref('mart_pair_overview') }}
having count(*) != 6

union all

select 'paire_sans_bougie', symbol
from {{ ref('mart_pair_overview') }}
where last_trade_date is null
