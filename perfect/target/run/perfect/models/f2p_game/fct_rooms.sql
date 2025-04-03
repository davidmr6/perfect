-- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into `playperfect-455514`.`PLAY_DWH`.`fct_rooms` as DBT_INTERNAL_DEST
        using (

WITH room_opens AS (
  SELECT
    date_utc,
    MIN(timestamp_utc) AS room_open_time,
    tournament_id,
    room_id,
    entry_fee,
    players_capacity
  FROM `playperfect-455514`.`row_data`.`events`
  WHERE
    event_name = 'tournamentJoined'
    
      AND date_utc between date_sub(current_date(), interval 1 day) AND current_date() and timestamp_utc between DATETIME_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR), HOUR) AND DATETIME_TRUNC(CURRENT_TIMESTAMP(), HOUR)
    
  GROUP BY date_utc, tournament_id, room_id, entry_fee, players_capacity
),

room_closes AS (
  SELECT
    room_id,
    MAX(timestamp_utc) AS room_closing_time
  FROM `playperfect-455514`.`row_data`.`events`
  WHERE
    event_name = 'tournamentRoomClosed'
    
      AND date_utc between date_sub(current_date(), interval 1 day) AND current_date() and timestamp_utc between DATETIME_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR), HOUR) AND DATETIME_TRUNC(CURRENT_TIMESTAMP(), HOUR)
    
  GROUP BY room_id
),

room_players AS (
  SELECT
    room_id,
    COUNT(DISTINCT player_id) AS actual_players,
    SUM(coins_spent) AS total_coins_sink
  FROM `playperfect-455514`.`row_data`.`events`
  WHERE
    event_name = 'tournamentJoined'
    
      AND date_utc between date_sub(current_date(), interval 1 day) AND current_date() and timestamp_utc between DATETIME_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR), HOUR) AND DATETIME_TRUNC(CURRENT_TIMESTAMP(), HOUR)
    
  GROUP BY room_id
),

room_rewards AS (
  SELECT
    room_id,
    SUM(coins_claimed) AS total_coins_rewards
  FROM `playperfect-455514`.`row_data`.`events`
  WHERE
    event_name = 'tournamentRewardClaimed'
    
      AND date_utc between date_sub(current_date(), interval 1 day) AND current_date() and timestamp_utc between DATETIME_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR), HOUR) AND DATETIME_TRUNC(CURRENT_TIMESTAMP(), HOUR)
    
  GROUP BY room_id
),

room_durations AS (
  SELECT
    room_id,
    AVG(play_duration) AS avg_play_duration
  FROM `playperfect-455514`.`row_data`.`events`
  WHERE
    event_name = 'tournamentFinished'
    
      AND date_utc between date_sub(current_date(), interval 1 day) AND current_date() and timestamp_utc between DATETIME_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR), HOUR) AND DATETIME_TRUNC(CURRENT_TIMESTAMP(), HOUR)
    
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
        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.room_id = DBT_INTERNAL_DEST.room_id))

    
    when matched then update set
        tournament_id = DBT_INTERNAL_SOURCE.tournament_id,players_capacity = DBT_INTERNAL_SOURCE.players_capacity,entry_fee = DBT_INTERNAL_SOURCE.entry_fee,room_open_time = DBT_INTERNAL_SOURCE.room_open_time,room_closing_time = DBT_INTERNAL_SOURCE.room_closing_time,actual_players = DBT_INTERNAL_SOURCE.actual_players,total_coins_sink = DBT_INTERNAL_SOURCE.total_coins_sink,total_coins_rewards = DBT_INTERNAL_SOURCE.total_coins_rewards,avg_play_duration = DBT_INTERNAL_SOURCE.avg_play_duration,room_open_duration = DBT_INTERNAL_SOURCE.room_open_duration,is_closed = DBT_INTERNAL_SOURCE.is_closed,is_full = DBT_INTERNAL_SOURCE.is_full
    

    when not matched then insert
        (`room_id`, `tournament_id`, `players_capacity`, `entry_fee`, `room_open_time`, `room_closing_time`, `actual_players`, `total_coins_sink`, `total_coins_rewards`, `avg_play_duration`, `room_open_duration`, `is_closed`, `is_full`)
    values
        (`room_id`, `tournament_id`, `players_capacity`, `entry_fee`, `room_open_time`, `room_closing_time`, `actual_players`, `total_coins_sink`, `total_coins_rewards`, `avg_play_duration`, `room_open_duration`, `is_closed`, `is_full`)


    