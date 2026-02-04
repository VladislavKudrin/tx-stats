SELECT
  o.payment_cred,
  COUNT(DISTINCT t.id) AS tx_count
FROM tx_out o
JOIN tx t    ON t.id = o.tx_id
JOIN block b ON b.id = t.block_id
WHERE o.payment_cred = ANY(%(payment_creds)s)
  AND b.time >= %(window_start)s
  AND b.time <  %(window_end)s
GROUP BY o.payment_cred;
