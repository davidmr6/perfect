{{
  config(
    materialized = 'incremental',
    schema = 'DWH',
    partition_by = {
      "field": "date_utc",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["player_id", "room_id"],
    incremental_strategy = "insert_overwrite",
    partition_filter = "date_utc BETWEEN {{ var('start_date') }} AND {{ var('end_date') }} and timestamp_utc between {{ var('start_time') }} AND {{ var('end_time') }}",
    tags = ['hourly']
  )
}}

WITH tournament_joins AS (
  SELECT
    timestamp_utc,
    date_utc,
    player_id,
    balance_before,
    tournament_id,
    room_id,
    entry_fee,
    coins_spent,
    players_capacity
  FROM {{ source('game_events', 'events') }}
  WHERE
    event_name = 'tournamentJoined'
    {% if is_incremental() %}
      AND date_utc BETWEEN {{ var('start_date') }} AND {{ var('end_date') }} and timestamp_utc between {{ var('start_time') }} AND {{ var('end_time') }}
    {% endif %}
),

tournament_finishes AS (
  SELECT
    timestamp_utc,
    date_utc,
    player_id,
    tournament_id,
    room_id,
    play_duration,
    score
  FROM {{ source('game_events', 'events') }}
  WHERE
    event_name = 'tournamentFinished'
    {% if is_incremental() %}
      AND date_utc BETWEEN {{ var('start_date') }} AND {{ var('end_date') }} and timestamp_utc between {{ var('start_time') }} AND {{ var('end_time') }}
    {% endif %}
),

room_closes AS (
  SELECT
    timestamp_utc,
    date_utc,
    player_id,
    tournament_id,
    room_id,
    score,
    position,
    reward
  FROM {{ source('game_events', 'events') }}
  WHERE
    event_name = 'tournamentRoomClosed'
    {% if is_incremental() %}
      AND date_utc BETWEEN {{ var('start_date') }} AND {{ var('end_date') }} and timestamp_utc between {{ var('start_time') }} AND {{ var('end_time') }}
    {% endif %}
),

reward_claims AS (
  SELECT
    timestamp_utc,
    date_utc,
    player_id,
    tournament_id,
    room_id,
    score,
    position,
    reward,
    coins_claimed
  FROM {{ source('game_events', 'events') }}
  WHERE
    event_name = 'tournamentRewardClaimed'
    {% if is_incremental() %}
      AND date_utc BETWEEN {{ var('start_date') }} AND {{ var('end_date') }} and timestamp_utc between {{ var('start_time') }} AND {{ var('end_time') }}
    {% endif %}
)

SELECT DISTINCT
  tj.date_utc,
  tj.player_id,
  MIN(tj.timestamp_utc) AS joined_time,
  MAX(tf.timestamp_utc) AS submit_time,
  MAX(rc.timestamp_utc) AS room_close_time,
  MAX(COALESCE(tf.play_duration, 0)) AS play_duration,
  MIN(tj.balance_before) AS balance_before,
  MAX(CASE
    WHEN cl.timestamp_utc IS NOT NULL THEN tj.balance_before - tj.entry_fee + COALESCE(cl.coins_claimed, 0)
    ELSE NULL
  END) AS balance_after_claim,
  tj.tournament_id,
  tj.room_id,
  MIN(tj.entry_fee) AS entry_fee,
  MIN(tj.players_capacity) AS players_capacity,
  (SELECT COUNT(DISTINCT e.player_id)
   FROM {{ source('game_events', 'events') }} e
   WHERE e.room_id = tj.room_id
   AND e.event_name = 'tournamentJoined'
   AND e.date_utc = tj.date_utc
   AND e.timestamp_utc between {{ var('start_time') }} AND {{ var('end_time') }}
  ) AS actual_players_in_room,
  MAX(COALESCE(tf.score, rc.score, cl.score)) AS score,
  MAX(rc.position) AS position,
  MAX(rc.reward) AS reward,
  MAX(CASE WHEN cl.timestamp_utc IS NOT NULL THEN 1 ELSE 0 END) AS did_claim_reward,
  MAX(cl.timestamp_utc) AS claim_time
FROM tournament_joins tj
LEFT JOIN tournament_finishes tf
  ON tj.player_id = tf.player_id
  AND tj.room_id = tf.room_id
  AND tj.date_utc = tf.date_utc
LEFT JOIN room_closes rc
  ON tj.player_id = rc.player_id
  AND tj.room_id = rc.room_id
  AND tj.date_utc = rc.date_utc
LEFT JOIN reward_claims cl
  ON tj.player_id = cl.player_id
  AND tj.room_id = cl.room_id
  AND tj.date_utc = cl.date_utc
GROUP BY
  tj.date_utc,
  tj.player_id,
  tj.tournament_id,
  tj.room_id