-- 查询场景：
-- 1. 查询某条数据的某个指标的特定表的上游数据，大部分场景使用
-- SELECT from_table, from_ids FROM data_lineage WHERE to_id = '123' AND to_table = 'table_name' AND has(facets, 'metric_key_1') AND from_table = 'table_name_1';
-- 1.1 查询某条数据的某个指标的特定表的上游数据，join 出 from 表信息，表都带 data_id 字段
-- SELECT from_table, from_id, f.* FROM data_lineage dl 
-- ARRAY JOIN from_ids AS from_id
-- LEFT JOIN from_table f ON f.data_id = from_id
-- WHERE dl.to_id = '123' AND dl.to_table = 'table_name' AND has(dl.facets, 'metric_key_1') AND dl.from_table = 'table_name_1';
-- 2. 查询某条数据的某个指标的所有表的上游数据，较少使用
-- SELECT from_table, from_ids FROM data_lineage WHERE to_id = '123' AND to_table = 'table_name' AND has(facets, 'metric_key_1');
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
-- SOME TEST TABLES
CREATE TABLE IF NOT EXISTS ods_jira_issues (
  project_id String,
  issue_id String,
  issue_code String,
  issue_type String,
  issue_status String,
  data_id String PRIMARY KEY,
  data_created_at DateTime
) ENGINE = MergeTree();
CREATE TABLE IF NOT EXISTS ods_git_commits (
  project_id String,
  commit_id String,
  commit_message String,
  data_id String PRIMARY KEY,
  data_created_at DateTime
) ENGINE = MergeTree();
CREATE TABLE IF NOT EXISTS dwd_jira_issues (
  project_id String,
  issue_id String,
  issue_code String,
  issue_type String,
  issue_status String,
  issue_created_at DateTime,
  `metrics.keys` Array(String),
  `metrics.values` Array(String),
  data_id String PRIMARY KEY,
  data_created_at DateTime
) ENGINE = MergeTree();
CREATE TABLE IF NOT EXISTS dwd_git_commits (
  project_id String,
  commit_id String,
  commit_message String,
  commit_at DateTime,
  `metrics.keys` Array(String),
  `metrics.values` Array(String),
  data_id String PRIMARY KEY,
  data_created_at DateTime
) ENGINE = MergeTree();
CREATE TABLE IF NOT EXISTS dwm_bugs (
  metrics_date Date,
  project_id String,
  issue_id String,
  `metrics.keys` Array(String),
  `metrics.values` Array(String),
  data_id String PRIMARY KEY,
  data_created_at DateTime
) ENGINE = MergeTree();
CREATE TABLE IF NOT EXISTS dws_projects (
  metrics_date Date,
  project_id String,
  `metrics.keys` Array(String),
  `metrics.values` Array(String),
  data_id String PRIMARY KEY,
  data_created_at DateTime
) ENGINE = MergeTree();