WITH cur AS (
  SELECT max(no) AS cur_epoch
  FROM epoch
),
win AS (
  SELECT
    e.no AS epoch_no,
    e.start_time,
    e.end_time
  FROM epoch e
  JOIN cur ON true
  WHERE e.no BETWEEN (cur.cur_epoch - %(window_epochs)s) AND (cur.cur_epoch - 1)
  ORDER BY e.no
)
SELECT
  (SELECT cur_epoch FROM cur) AS chain_current_epoch,
  (SELECT max(epoch_no) FROM win) AS last_completed_epoch,
  (SELECT min(start_time) FROM win) AS window_start,
  (SELECT max(end_time) FROM win) AS window_end,
  (SELECT max(end_time) FROM win) AS next_run_time;

