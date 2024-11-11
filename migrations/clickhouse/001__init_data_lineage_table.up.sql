CREATE TABLE IF NOT EXISTS data_lineage (
  to_table LowCardinality(String),
  to_id String,
  from_table LowCardinality(String),
  from_ids Array(String),
  to_metric_keys Array(String),
  created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (to_table, to_id)
SETTINGS index_granularity = 8192,
         min_bytes_for_wide_part = 0,
         write_final_mark = 0

INDEX idx_metric_keys to_metric_keys TYPE set(0) GRANULARITY 1

COMMENT 'data lineage';
