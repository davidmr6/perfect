{{
  config(
    materialized = 'incremental',
    schema = 'DWH',
    cluster_by = ["room_id"],
    unique_key = "room_id",
    incremental_strategy = 'merge',
    merge_update_columns = ['tournament_id', 'players_capacity', 'entry_fee', 'room_open_time',
                          'room_closing_time', 'actual_players', 'total_coins_sink',
                          'total_coins_rewards', 'avg_play_duration', 'room_open_duration',
                          'is_closed', 'is_full'],
    partition_filter = "date_utc between DATE({{ var('start_date') }}) AND DATE({{ var('end_date') }}) and timestamp_utc between {{ var('start_time') }} AND {{ var('end_time') }}",
    tags = ['hourly']
  )
}}

WITH room_opens AS (
  SELECT
    date_utc,
    MIN(timestamp_utc) AS room_open_time,
    tournament_id,
    room_id,
    entry_fee,
    players_capacity
  FROM {{ source('game_events', 'events') }}
  WHERE
    event_name = 'tournamentJoined'
    {% if is_incremental() %}
      AND date_utc between {{ var('start_date') }} AND {{ var('end_date') }} and timestamp_utc between {{ var('start_time') }} AND {{ var('end_time') }}
    {% endif %}
  GROUP BY date_utc, tournament_id, room_id, entry_fee, players_capacity
),

room_closes AS (
  SELECT
    room_id,
    MAX(timestamp_utc) AS room_closing_time
  FROM {{ source('game_events', 'events') }}
  WHERE
    event_name = 'tournamentRoomClosed'
    {% if is_incremental() %}
      AND date_utc between {{ var('start_date') }} AND {{ var('end_date') }} and timestamp_utc between {{ var('start_time') }} AND {{ var('end_time') }}
    {% endif %}
  GROUP BY room_id
),

room_players AS (
  SELECT
    room_id,
    COUNT(DISTINCT player_id) AS actual_players,
    SUM(coins_spent) AS total_coins_sink
  FROM {{ source('game_events', 'events') }}
  WHERE
    event_name = 'tournamentJoined'
    {% if is_incremental() %}
      AND date_utc between {{ var('start_date') }} AND {{ var('end_date') }} and timestamp_utc between {{ var('start_time') }} AND {{ var('end_time') }}
    {% endif %}
  GROUP BY room_id
),

room_rewards AS (
  SELECT
    room_id,
    SUM(coins_claimed) AS total_coins_rewards
  FROM {{ source('game_events', 'events') }}
  WHERE
    event_name = 'tournamentRewardClaimed'
    {% if is_incremental() %}
      AND date_utc between {{ var('start_date') }} AND {{ var('end_date') }} and timestamp_utc between {{ var('start_time') }} AND {{ var('end_time') }}
    {% endif %}
  GROUP BY room_id
),

room_durations AS (
  SELECT
    room_id,
    AVG(play_duration) AS avg_play_duration
  FROM {{ source('game_events', 'events') }}
  WHERE
    event_name = 'tournamentFinished'
    {% if is_incremental() %}
      AND date_utc between {{ var('start_date') }} AND {{ var('end_date') }} and timestamp_utc between {{ var('start_time') }} AND {{ var('end_time') }}
    {% endif %}
  GROUP BY room_id
)

SELECT
  ro.room_id,
  max(ro.tournament_id) AS tournament_id,
  max(ro.players_capacity) as players_capacity,
  max(ro.entry_fee) as entry_fee,
  max(ro.room_open_time) as room_open_time,
  max(rc.room_closing_time) as room_closing_time,
  COALESCE(sum(rp.actual_players), 0) AS actual_players,
  COALESCE(sum(rp.total_coins_sink), 0) AS total_coins_sink,
  COALESCE(sum(rr.total_coins_rewards), 0) AS total_coins_rewards,
  COALESCE(sum(rd.avg_play_duration), 0) AS avg_play_duration,
  max(TIMESTAMP_DIFF(rc.room_closing_time, ro.room_open_time, SECOND) / 60) AS room_open_duration,
  max(CASE WHEN rc.room_closing_time IS NOT NULL THEN 1 ELSE 0 END) AS is_closed,
  max(CASE WHEN COALESCE(rp.actual_players, 0) >= ro.players_capacity THEN 1 ELSE 0 END) AS is_full,
FROM room_opens ro
LEFT JOIN room_closes rc
  ON ro.room_id = rc.room_id
LEFT JOIN room_players rp
  ON ro.room_id = rp.room_id
LEFT JOIN room_rewards rr
  ON ro.room_id = rr.room_id
LEFT JOIN room_durations rd
  ON ro.room_id = rd.room_id
group by ro.room_id