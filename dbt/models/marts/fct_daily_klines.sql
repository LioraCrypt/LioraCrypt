-- fct_daily_klines (table de faits)
-- Une ligne par paire et par jour : prix, volumes, rendements et indicateurs.
-- Sert l'historique d'une paire (API /pairs/{symbol}/history) et les autres modèles du mart.
--
-- Choix :
-- - Fenêtres en JOURS CALENDAIRES (RANGE sur unix_date), pas en nombre de lignes :
--   un jour manquant ne décale pas la fenêtre.
-- - Rendement calculé seulement si la veille existe (sinon NULL, pas de rendement sur 2 jours).
-- - Volatilité = écart-type des rendements log sur 30 jours, annualisée par racine(365)
--   (la crypto cote tous les jours). Les nb_jours_* permettent de savoir si la fenêtre est complète.
-- - Indicateurs descriptifs uniquement : aucune prédiction, aucun conseil.

{{ config(cluster_by = ['symbol']) }}

with klines as (

    select
        symbol,
        date(open_time) as trade_date,
        open_time,
        close_time,
        open_price,
        high_price,
        low_price,
        close_price,
        base_volume,
        quote_volume,
        number_of_trades,
        requested_at,
        loaded_at
    from {{ ref('stg_binance__klines') }}

),

veille as (

    select
        *,
        lag(trade_date)  over (partition by symbol order by trade_date) as prev_trade_date,
        lag(close_price) over (partition by symbol order by trade_date) as prev_close_price
    from klines

),

rendements as (

    select
        * except (prev_trade_date, prev_close_price),
        case when date_diff(trade_date, prev_trade_date, day) = 1
             then safe_divide(close_price - prev_close_price, prev_close_price)
        end as daily_return,
        case when date_diff(trade_date, prev_trade_date, day) = 1
             then ln(safe_divide(cast(close_price as float64), cast(prev_close_price as float64)))
        end as log_return
    from veille

)

select
    symbol,
    trade_date,
    open_time,
    close_time,
    open_price,
    high_price,
    low_price,
    close_price,
    base_volume,
    quote_volume,
    number_of_trades,
    daily_return,
    log_return,

    avg(close_price) over w7  as ma_7d,
    avg(close_price) over w30 as ma_30d,
    count(*)         over w7  as nb_jours_ma_7d,
    count(*)         over w30 as nb_jours_ma_30d,

    stddev_samp(log_return) over w30 * sqrt(365) as volatility_30d_annualized,
    count(log_return)       over w30             as nb_rendements_30d,

    requested_at,
    loaded_at
from rendements
window
    w7  as (partition by symbol order by unix_date(trade_date) range between 6 preceding and current row),
    w30 as (partition by symbol order by unix_date(trade_date) range between 29 preceding and current row)
