CREATE TABLE IF NOT EXISTS data_lineage (
  from_id String,
  to_id String,
  created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree()
ORDER BY (from_id, to_id) COMMENT 'data lineage';