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