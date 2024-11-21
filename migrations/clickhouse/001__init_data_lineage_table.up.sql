CREATE TABLE IF NOT EXISTS data_lineage (
  to_table LowCardinality(String),
  -- LowCardinality 类型用于节省存储空间
  to_id String,
  to_facets Array(String),
  from_table LowCardinality(String),
  -- LowCardinality 类型用于节省存储空间
  from_ids Array(String),
  created_at DateTime DEFAULT now(),
  -- 为常用的查询条件组合创建复合跳数索引
  INDEX idx_main (to_table, to_id, from_table) TYPE minmax GRANULARITY 4,
  -- facets 索引
  INDEX idx_facet_keys to_facets TYPE
  set(0) GRANULARITY 1,
    -- from_ids 索引
    INDEX idx_from_ids from_ids TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = MergeTree() -- 使用 MergeTree 引擎，支持分区、排序和索引
PARTITION BY to_table -- 按 to_table 分区,可以提高查询性能
ORDER BY (to_table, to_id, from_table) -- 按 to_table, to_id, from_table 排序
  SETTINGS index_granularity = 8192,
  -- 索引粒度设置,影响查询性能和存储空间的平衡
  min_bytes_for_wide_part = 0,
  -- 设置为0可以强制使用 Wide 格式存储,提高查询性能
  write_final_mark = 0 -- 禁用写入最终标记,可以略微提升写入性能
;