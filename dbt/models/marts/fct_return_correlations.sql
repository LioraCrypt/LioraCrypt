-- fct_return_correlations
-- Corrélation de Pearson des RENDEMENTS LOG journaliers (jamais des prix : deux prix qui
-- montent tous les deux sont "corrélés" sans que leurs variations le soient), entre paires
-- de la MÊME devise de cotation (USDT avec USDT, EUR avec EUR), sur 30, 90 et 365 jours
-- calendaires se terminant à la dernière date commune. API /correlations.
-- Une ligne par couple (symbol_a < symbol_b), devise et fenêtre. nb_jours = jours communs utilisés.

{{ config(materialized = 'table') }}

with rendements as (

    select
        klines.symbol,
        pairs.quote_asset,
        klines.trade_date,
        klines.log_return
    from {{ ref('fct_daily_klines') }} as klines
    inner join {{ ref('dim_pairs') }} as pairs using (symbol)
    where klines.log_return is not null

),

fenetres as (
    select window_days from unnest([30, 90, 365]) as window_days
),

couples as (

    select
        a.quote_asset,
        a.symbol      as symbol_a,
        b.symbol      as symbol_b,
        a.trade_date,
        a.log_return  as log_return_a,
        b.log_return  as log_return_b
    from rendements as a
    inner join rendements as b
        on  a.quote_asset = b.quote_asset
        and a.trade_date  = b.trade_date
        and a.symbol      < b.symbol

),

fin_par_couple as (

    select quote_asset, symbol_a, symbol_b, max(trade_date) as end_date
    from couples
    group by quote_asset, symbol_a, symbol_b

)

select
    couples.quote_asset,
    couples.symbol_a,
    couples.symbol_b,
    fenetres.window_days,
    date_sub(fin.end_date, interval fenetres.window_days - 1 day) as start_date,
    fin.end_date,
    corr(couples.log_return_a, couples.log_return_b)              as correlation,
    count(*)                                                      as nb_jours
from couples
inner join fin_par_couple as fin
    using (quote_asset, symbol_a, symbol_b)
cross join fenetres
where couples.trade_date > date_sub(fin.end_date, interval fenetres.window_days day)
group by
    couples.quote_asset, couples.symbol_a, couples.symbol_b,
    fenetres.window_days, fin.end_date
