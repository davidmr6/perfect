{{
  config(
    materialized = 'incremental',
    schema = 'DWH',
    partition_by = {
      "field": "date_utc",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["player_id"],
    incremental_strategy = "insert_overwrite",
    partition_filter = "date_utc BETWEEN {{ var('start_date') }} AND {{ var('end_date') }}",
    tags = ['daily']
  )
}}

WITH daily_first_balance AS (
  SELECT
    player_id,
    date_utc,
    FIRST_VALUE(balance_before) OVER (
      PARTITION BY player_id, date_utc
      ORDER BY timestamp_utc ASC
    ) AS balance_day_start
  FROM {{ source('game_events', 'events') }}
  WHERE 1=1
    {% if is_incremental() %}
      AND date_utc BETWEEN {{ var('start_date') }} AND {{ var('end_date') }}
    {% endif %}
),

daily_last_balance AS (
  SELECT
    player_id,
    date_utc,
    FIRST_VALUE(balance_before) OVER (
      PARTITION BY player_id, date_utc
      ORDER BY timestamp_utc DESC
    ) AS balance_day_end
  FROM {{ source('game_events', 'events') }}
  WHERE 1=1
    {% if is_incremental() %}
      AND date_utc BETWEEN {{ var('start_date') }} AND {{ var('end_date') }}
    {% endif %}
),

daily_matches AS (
  SELECT
    player_id,
    date_utc,
    COUNT(DISTINCT room_id) AS matches_played,
    SUM(play_duration) AS total_matches_duration,
    COUNT(DISTINCT CASE WHEN position = 0 THEN room_id END) AS matches_won_reward,
    MAX(score) AS max_score,
    AVG(score) AS avg_score,
    MIN(position) AS max_position,
    AVG(position) AS avg_position
  FROM {{ source('game_events', 'events') }}
  WHERE 1=1
    AND event_name IN ('tournamentFinished', 'tournamentRoomClosed')
    {% if is_incremental() %}
      AND date_utc BETWEEN {{ var('start_date') }} AND {{ var('end_date') }}
    {% endif %}
  GROUP BY 1, 2
),

daily_claims AS (
  SELECT
    player_id,
    date_utc,
    COUNT(DISTINCT room_id) AS matches_claimed,
    SUM(coins_claimed) AS coins_source_tournaments
  FROM {{ source('game_events', 'events') }}
  WHERE 1=1
    AND event_name = 'tournamentRewardClaimed'
    {% if is_incremental() %}
      AND date_utc BETWEEN {{ var('start_date') }} AND {{ var('end_date') }}
    {% endif %}
  GROUP BY 1, 2
),

daily_joins AS (
  SELECT
    player_id,
    date_utc,
    SUM(coins_spent) AS coins_sink_tournaments
  FROM {{ source('game_events', 'events') }}
  WHERE 1=1
    AND event_name = 'tournamentJoined'
    {% if is_incremental() %}
      AND date_utc BETWEEN {{ var('start_date') }} AND {{ var('end_date') }}
    {% endif %}
  GROUP BY 1, 2
),

daily_purchases AS (
  SELECT
    player_id,
    date_utc,
    SUM(coins_claimed) AS coins_source_purchases,
    SUM(price_usd) AS revenue
  FROM {{ source('game_events', 'events') }}
  WHERE 1=1
    AND event_name = 'purchase'
    {% if is_incremental() %}
      AND date_utc BETWEEN {{ var('start_date') }} AND {{ var('end_date') }}
    {% endif %}
  GROUP BY 1, 2
),

player_match_results AS (
  SELECT
    player_id,
    date_utc,
    room_id,
    timestamp_utc,
    CASE WHEN position = 0 THEN 1 ELSE 0 END AS is_win
  FROM {{ source('game_events', 'events') }}
  WHERE 1=1
    AND event_name = 'tournamentRoomClosed'
    {% if is_incremental() %}
      AND date_utc BETWEEN {{ var('start_date') }} AND {{ var('end_date') }}
    {% endif %}
),

match_streaks AS (
  SELECT
    player_id,
    date_utc,
    timestamp_utc,
    is_win,
    CASE
      WHEN is_win = 1 AND LAG(is_win) OVER (PARTITION BY player_id, date_utc ORDER BY timestamp_utc) = 0 THEN 1
      WHEN is_win = 1 AND LAG(is_win) OVER (PARTITION BY player_id, date_utc ORDER BY timestamp_utc) IS NULL THEN 1
      ELSE 0
    END AS streak_start
  FROM player_match_results
),

match_streaks_grouped AS (
  SELECT
    player_id,
    date_utc,
    timestamp_utc,
    is_win,
    SUM(streak_start) OVER (PARTITION BY player_id, date_utc ORDER BY timestamp_utc) AS streak_group
  FROM match_streaks
),

streak_lengths AS (
  SELECT
    player_id,
    date_utc,
    streak_group,
    is_win,
    COUNT(*) AS streak_length
  FROM match_streaks_grouped
  GROUP BY 1, 2, 3, 4
),

daily_streaks AS (
  SELECT
    player_id,
    date_utc,
    MAX(CASE WHEN is_win = 1 THEN streak_length ELSE 0 END) AS max_reward_won_streak,
    MAX(CASE WHEN is_win = 0 THEN streak_length ELSE 0 END) AS max_losing_streak
  FROM streak_lengths
  GROUP BY 1, 2
)

SELECT
  dfb.player_id,
  dfb.date_utc,
  MAX(dfb.balance_day_start) AS balance_day_start,
  MAX(dlb.balance_day_end) AS balance_day_end,
  COALESCE(SUM(dm.matches_played), 0) AS matches_played,
  COALESCE(SUM(dm.total_matches_duration), 0) AS total_matches_duration,
  COALESCE(SUM(dm.matches_won_reward), 0) AS matches_won_reward,
  COALESCE(SUM(dc.matches_claimed), 0) AS matches_claimed,
  COALESCE(SUM(dj.coins_sink_tournaments), 0) AS coins_sink_tournaments,
  COALESCE(SUM(dc.coins_source_tournaments), 0) AS coins_source_tournaments,
  COALESCE(MAX(dm.max_score), 0) AS max_score,
  COALESCE(AVG(dm.avg_score), 0) AS avg_score,
  COALESCE(MAX(dm.max_position), 0) AS max_position,
  COALESCE(AVG(dm.avg_position), 0) AS avg_position,
  COALESCE(MAX(ds.max_reward_won_streak), 0) AS max_reward_won_streak,
  COALESCE(MAX(ds.max_losing_streak), 0) AS max_losing_streak,
  COALESCE(SUM(dp.revenue), 0) AS revenue,
  COALESCE(SUM(dp.coins_source_purchases), 0) AS coins_source_purchases
FROM daily_first_balance dfb
LEFT JOIN daily_last_balance dlb
  ON dfb.player_id = dlb.player_id AND dfb.date_utc = dlb.date_utc
LEFT JOIN daily_matches dm
  ON dfb.player_id = dm.player_id AND dfb.date_utc = dm.date_utc
LEFT JOIN daily_claims dc
  ON dfb.player_id = dc.player_id AND dfb.date_utc = dc.date_utc
LEFT JOIN daily_joins dj
  ON dfb.player_id = dj.player_id AND dfb.date_utc = dj.date_utc
LEFT JOIN daily_purchases dp
  ON dfb.player_id = dp.player_id AND dfb.date_utc = dp.date_utc
LEFT JOIN daily_streaks ds
  ON dfb.player_id = ds.player_id AND dfb.date_utc = ds.date_utc
GROUP BY dfb.player_id, dfb.date_utc