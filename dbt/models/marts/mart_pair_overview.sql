-- mart_pair_overview
-- Une ligne par paire : dernière bougie clôturée et indicateurs récents (API /overview).
-- Contient la date de la dernière bougie et la date de chargement (décision du 2026-11-17) :
-- l'API s'en sert pour masquer une paire périmée et ne pas appeler le LLM pour elle.
-- is_fresh utilise un seuil PROVISOIRE (variable freshness_max_hours, 48 h par défaut).
-- Calculé au moment du dbt build : l'API doit aussi recalculer avec l'heure de la requête.

with derniere as (

    select *
    from {{ ref('fct_daily_klines') }}
    qualify row_number() over (partition by symbol order by trade_date desc) = 1

),

-- Clôture 7 et 30 jours calendaires avant la dernière bougie (NULL si le jour manque).
historique as (

    select
        derniere.symbol,
        j7.close_price  as close_price_7d_ago,
        j30.close_price as close_price_30d_ago
    from derniere
    left join {{ ref('fct_daily_klines') }} as j7
        on  j7.symbol = derniere.symbol
        and j7.trade_date = date_sub(derniere.trade_date, interval 7 day)
    left join {{ ref('fct_daily_klines') }} as j30
        on  j30.symbol = derniere.symbol
        and j30.trade_date = date_sub(derniere.trade_date, interval 30 day)

)

select
    pairs.symbol,
    pairs.base_asset,
    pairs.quote_asset,
    pairs.base_name,

    derniere.trade_date                                   as last_trade_date,
    derniere.close_time                                   as last_close_time,
    derniere.close_price                                  as last_close_price,
    derniere.daily_return                                 as return_1d,
    safe_divide(derniere.close_price - historique.close_price_7d_ago,
                historique.close_price_7d_ago)            as return_7d,
    safe_divide(derniere.close_price - historique.close_price_30d_ago,
                historique.close_price_30d_ago)           as return_30d,
    derniere.ma_7d,
    derniere.ma_30d,
    derniere.volatility_30d_annualized,
    derniere.nb_rendements_30d,

    derniere.requested_at                                 as last_requested_at,
    derniere.loaded_at                                    as last_loaded_at,
    timestamp_diff(current_timestamp(), derniere.close_time, hour) as hours_since_last_close,
    derniere.close_time >= timestamp_sub(
        current_timestamp(), interval {{ var('freshness_max_hours', 48) }} hour
    )                                                     as is_fresh,
    current_timestamp()                                   as computed_at
from {{ ref('dim_pairs') }} as pairs
left join derniere   using (symbol)
left join historique using (symbol)
