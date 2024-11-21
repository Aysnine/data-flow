import { createClient } from "@clickhouse/client";

const db = createClient({
  url: "http://localhost:8123",
  username: "default",
  password: "",
  database: "default",
});

async function clean() {
  await db.query({
    query: "TRUNCATE TABLE ods_jira_issues",
  });
  await db.query({
    query: "TRUNCATE TABLE ods_git_commits",
  });
  await db.query({
    query: "TRUNCATE TABLE dwd_jira_issues",
  });
  await db.query({
    query: "TRUNCATE TABLE dwd_git_commits",
  });
  await db.query({
    query: "TRUNCATE TABLE dwm_bugs",
  });
  await db.query({
    query: "TRUNCATE TABLE dws_projects",
  });
  await db.query({
    query: "TRUNCATE TABLE data_lineage",
  });
  console.log("Clean done");
}
clean();
