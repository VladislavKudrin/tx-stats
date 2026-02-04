SELECT
  tm.tx_id,
  msg.msg_text AS raw_msg
FROM tx_metadata tm
JOIN tx t    ON t.id = tm.tx_id
JOIN block b ON b.id = t.block_id
JOIN LATERAL (
  SELECT tm.json->>'msg' AS msg_text
  WHERE jsonb_typeof(tm.json->'msg') = 'string'

  UNION ALL

  SELECT x AS msg_text
  FROM jsonb_array_elements_text(tm.json->'msg') AS x
  WHERE jsonb_typeof(tm.json->'msg') = 'array'
) AS msg ON TRUE
WHERE tm.key = 674
  AND b.time >= %(start_time)s
  AND b.time <  %(end_time)s
  AND msg.msg_text IS NOT NULL;
