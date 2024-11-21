import { createClient } from "@clickhouse/client";
import pl from "nodejs-polars";
import { v4 as uuidv4 } from "uuid";
import dayjs from "dayjs";

const SIMULATION_DATE_START_DATE = "2024-01-01";
const SIMULATION_DATE_END_DATE = "2024-01-02";
const DAILY_JIRA_ISSUE_DATA_COUNT = 10;
const PROJECT_ID_RANGE = [1, 10];
const ISSUE_ID_RANGE = [1, 99];
const ISSUE_TYPE_RANGE = ["bug", "task", "story"];
const ISSUE_STATUS_RANGE = ["open", "in-progress", "resolved", "closed"];
const DAILY_GIT_COMMIT_COUNT = 10;
const COMMIT_MESSAGE_TEMPLATES = ["fix: 修复bug #", "feat: 添加新功能 #", "docs: 更新文档 #", "refactor: 重构代码 #", "test: 添加测试用例 #"];
const METRIC_KEYS = ["total_days", "dev_days", "ba_days", "qa_days", "over_days"];
const GIT_METRIC_KEYS = ["code_lines_added", "code_lines_deleted", "files_changed", "review_hours", "discussion_count"];
const DWM_BUG_METRIC_KEYS = ["bug_days"];
const DWS_PROJECT_METRIC_KEYS = ["commit_count_in_1m", "issue_added_count_in_1m", "commit_count_in_3m", "issue_added_count_in_3m", "bug_avg_days_in_1m"];

const db = createClient({
  url: "http://localhost:8123",
  username: "default",
  password: "",
  database: "default",
});

async function make_ods_jira_issues(date: string) {
  const generateRandomRange = (min: number, max: number) => Math.floor(Math.random() * (max - min)) + min;

  const generateSeries = (name: string, count: number, generator: () => any) => {
    return pl.Series(name, Array(count).fill(0).map(generator));
  };

  const issueTypes = generateSeries("issue_type", DAILY_JIRA_ISSUE_DATA_COUNT, () => ISSUE_TYPE_RANGE[generateRandomRange(0, ISSUE_TYPE_RANGE.length)]);

  const issueIds = generateSeries("issue_id", DAILY_JIRA_ISSUE_DATA_COUNT, () => generateRandomRange(ISSUE_ID_RANGE[0], ISSUE_ID_RANGE[1]));

  const df = pl.DataFrame({
    data_id: pl.Series(
      "data_id",
      Array(DAILY_JIRA_ISSUE_DATA_COUNT)
        .fill(0)
        .map(() => uuidv4())
    ),
    data_created_at: pl.Series("data_created_at", Array(DAILY_JIRA_ISSUE_DATA_COUNT).fill(Math.floor(new Date(date).getTime() / 1000))),
    project_id: generateSeries("project_id", DAILY_JIRA_ISSUE_DATA_COUNT, () => generateRandomRange(PROJECT_ID_RANGE[0], PROJECT_ID_RANGE[1])),
    issue_id: issueIds,
    issue_code: pl.Series(
      "issue_code",
      Array(DAILY_JIRA_ISSUE_DATA_COUNT)
        .fill(0)
        .map((_, i) => `${issueTypes.get(i).toUpperCase()}-${issueIds.get(i)}`)
    ),
    issue_type: issueTypes,
    issue_status: generateSeries("issue_status", DAILY_JIRA_ISSUE_DATA_COUNT, () => ISSUE_STATUS_RANGE[generateRandomRange(0, ISSUE_STATUS_RANGE.length)]),
  });

  await db.insert({
    table: "ods_jira_issues",
    values: df.toRecords(),
    format: "JSONEachRow",
  });
}

async function make_ods_git_commits(date: string) {
  const generateRandomRange = (min: number, max: number) => Math.floor(Math.random() * (max - min)) + min;
  const generateRandomString = () => Math.random().toString(36).substring(2, 15);

  const generateSeries = (name: string, count: number, generator: () => any) => {
    return pl.Series(name, Array(count).fill(0).map(generator));
  };

  const issueIds = generateSeries("issue_id", DAILY_GIT_COMMIT_COUNT, () => generateRandomRange(ISSUE_ID_RANGE[0], ISSUE_ID_RANGE[1]));

  const commitMessages = pl.Series(
    "commit_message",
    Array(DAILY_GIT_COMMIT_COUNT)
      .fill(0)
      .map(() => {
        const template = COMMIT_MESSAGE_TEMPLATES[generateRandomRange(0, COMMIT_MESSAGE_TEMPLATES.length)];
        return template + generateRandomRange(ISSUE_ID_RANGE[0], ISSUE_ID_RANGE[1]);
      })
  );

  const df = pl.DataFrame({
    data_id: pl.Series(
      "data_id",
      Array(DAILY_GIT_COMMIT_COUNT)
        .fill(0)
        .map(() => uuidv4())
    ),
    data_created_at: pl.Series("data_created_at", Array(DAILY_GIT_COMMIT_COUNT).fill(Math.floor(new Date(date).getTime() / 1000))),
    project_id: generateSeries("project_id", DAILY_GIT_COMMIT_COUNT, () => generateRandomRange(PROJECT_ID_RANGE[0], PROJECT_ID_RANGE[1])),
    commit_id: generateSeries("commit_id", DAILY_GIT_COMMIT_COUNT, () => generateRandomString()),
    commit_message: commitMessages,
  });

  await db.insert({
    table: "ods_git_commits",
    values: df.toRecords(),
    format: "JSONEachRow",
  });
}

async function make_dwd_jira_issues(date: string) {
  const odsData = await db.query({
    query: `SELECT * FROM ods_jira_issues WHERE toDate(data_created_at) = '${date}'`,
    format: "JSONEachRow",
  });

  const records = await odsData.json();

  const generateMetrics = () => {
    const total_days = +(Math.random() * 20).toFixed(2); // 0-20天
    const dev_days = +(Math.random() * total_days * 0.5).toFixed(2); // 开发天数
    const ba_days = +(Math.random() * total_days * 0.2).toFixed(2); // BA分析天数
    const qa_days = +(Math.random() * total_days * 0.3).toFixed(2); // 测试天数
    const over_days = +Math.max(0, total_days - (dev_days + ba_days + qa_days)).toFixed(2); // 超出天数

    return {
      keys: METRIC_KEYS,
      values: [total_days.toString(), dev_days.toString(), ba_days.toString(), qa_days.toString(), over_days.toString()],
    };
  };

  const dwdRecords = records.map((record: any) => {
    const metrics = generateMetrics();
    return {
      ...record,
      issue_created_at: record.data_created_at,
      "metrics.keys": metrics.keys,
      "metrics.values": metrics.values,
    };
  });

  await db.insert({
    table: "dwd_jira_issues",
    values: dwdRecords,
    format: "JSONEachRow",
  });

  const lineageRecords = dwdRecords.map((record: any) => ({
    from_table: "ods_jira_issues",
    from_ids: [record.data_id],
    to_table: "dwd_jira_issues",
    to_id: record.data_id,
    to_to_facets: METRIC_KEYS,
    created_at: Math.floor(new Date(date).getTime() / 1000),
  }));

  await db.insert({
    table: "data_lineage",
    values: lineageRecords,
    format: "JSONEachRow",
  });
}

async function make_dwd_git_commits(date: string) {
  const odsData = await db.query({
    query: `SELECT * FROM ods_git_commits WHERE toDate(data_created_at) = '${date}'`,
    format: "JSONEachRow",
  });

  const records = await odsData.json();

  const generateMetrics = () => {
    const lines_added = Math.floor(Math.random() * 500); // 0-500行新增
    const lines_deleted = Math.floor(Math.random() * 200); // 0-200行删除
    const files_changed = Math.floor(Math.random() * 10) + 1; // 1-10个文件改动
    const review_hours = +(Math.random() * 8).toFixed(2); // 0-8小时代码审查
    const discussion_count = Math.floor(Math.random() * 15); // 0-15条讨论

    return {
      keys: GIT_METRIC_KEYS,
      values: [lines_added.toString(), lines_deleted.toString(), files_changed.toString(), review_hours.toString(), discussion_count.toString()],
    };
  };

  const dwdRecords = records.map((record: any) => {
    const metrics = generateMetrics();
    return {
      ...record,
      commit_at: record.data_created_at,
      "metrics.keys": metrics.keys,
      "metrics.values": metrics.values,
    };
  });

  await db.insert({
    table: "dwd_git_commits",
    values: dwdRecords,
    format: "JSONEachRow",
  });

  const lineageRecords = dwdRecords.map((record: any) => ({
    from_table: "ods_git_commits",
    from_ids: [record.data_id],
    to_table: "dwd_git_commits",
    to_id: record.data_id,
    to_facets: GIT_METRIC_KEYS,
    created_at: Math.floor(new Date(date).getTime() / 1000),
  }));

  await db.insert({
    table: "data_lineage",
    values: lineageRecords,
    format: "JSONEachRow",
  });
}

async function make_dwm_bugs(date: string) {
  const dwdData = await db.query({
    query: `SELECT * FROM dwd_jira_issues WHERE toDate(data_created_at) = '${date}' AND issue_type = 'bug'`,
    format: "JSONEachRow",
  });

  const records = await dwdData.json();

  const dwmRecords = records.map((record: any) => {
    const bugDays = +(parseFloat(record.total_days) * (0.8 + Math.random() * 0.4)).toFixed(2); // 80%-120%的total_days

    return {
      metrics_date: date,
      project_id: record.project_id,
      issue_id: record.issue_id,
      "metrics.keys": DWM_BUG_METRIC_KEYS,
      "metrics.values": [bugDays.toString()],
      data_id: uuidv4(),
      data_created_at: Math.floor(new Date(date).getTime() / 1000),
    };
  });

  if (dwmRecords.length > 0) {
    await db.insert({
      table: "dwm_bugs",
      values: dwmRecords,
      format: "JSONEachRow",
    });
    const lineageRecords = dwmRecords.map((dwmRecord: any) => {
      const sourceRecord = records.find((r: any) => r.issue_id === dwmRecord.issue_id) as { data_id: string };
      return {
        from_table: "dwd_jira_issues",
        from_ids: [sourceRecord.data_id],
        to_table: "dwm_bugs",
        to_id: dwmRecord.data_id,
        to_facets: DWM_BUG_METRIC_KEYS,
        created_at: Math.floor(new Date(date).getTime() / 1000),
      };
    });

    await db.insert({
      table: "data_lineage",
      values: lineageRecords,
      format: "JSONEachRow",
    });
  }
}

async function make_dws_projects(date: string): Promise<
  {
    from_table: string;
    from_ids: string[];
    to_table: string;
    to_id: string;
    to_facets: string[];
    created_at: number;
  }[]
> {
  // 获取原始数据，同时获取data_id
  const [gitData, issueData, bugData] = await Promise.all([
    db
      .query({
        query: `
        WITH latest_commits AS (
          SELECT project_id, commit_id, commit_at, data_id, ROW_NUMBER() OVER (PARTITION BY commit_id ORDER BY data_created_at DESC) as rn
          FROM dwd_git_commits
          WHERE toDate(commit_at) >= addMonths(toDate('${date}'), -3)
        )
        SELECT project_id, commit_id, commit_at as date, data_id 
        FROM latest_commits
        WHERE rn = 1
      `,
        format: "JSONEachRow",
      })
      .then((res) => res.json()),

    db
      .query({
        query: `
        WITH latest_issues AS (
          SELECT project_id, issue_created_at, data_id, ROW_NUMBER() OVER (PARTITION BY issue_id ORDER BY data_created_at DESC) as rn
          FROM dwd_jira_issues
          WHERE toDate(issue_created_at) >= addMonths(toDate('${date}'), -3)
        )
        SELECT project_id, issue_created_at as date, data_id
        FROM latest_issues
        WHERE rn = 1
      `,
        format: "JSONEachRow",
      })
      .then((res) => res.json()),

    db
      .query({
        query: `
        WITH bug_metrics AS (
          SELECT 
            project_id,
            metrics_date as date,
            data_id,
            metrics.keys,
            metrics.values
          FROM dwm_bugs
          WHERE metrics_date >= addMonths(toDate('${date}'), -1)
        )
        SELECT
          project_id,
          date,
          data_id,
          arrayElement(metrics.values, indexOf(metrics.keys, 'bug_days')) as bug_days
        FROM bug_metrics
      `,
        format: "JSONEachRow",
      })
      .then((res) => res.json()),
  ]);

  const projectIds = new Set([...gitData.map((r: any) => r.project_id), ...issueData.map((r: any) => r.project_id), ...bugData.map((r: any) => r.project_id)]);
  const dwsRecords = Array.from(projectIds).map((projectId) => {
    // Git提交统计
    const projectGitData = gitData.filter((r: any) => r.project_id === projectId);
    const gitData1m = projectGitData.filter((r: any) => dayjs(r.date).format("YYYY-MM-DD") === dayjs(date).format("YYYY-MM-DD"));
    const gitData3m = projectGitData.filter((r: any) => dayjs(r.date).format("YYYY-MM-DD") === dayjs(date).format("YYYY-MM-DD"));
    const commit_count_1m = gitData1m.length;
    const commit_count_3m = gitData3m.length;

    // Issue统计
    const projectIssueData = issueData.filter((r: any) => r.project_id === projectId);
    const issueData1m = projectIssueData.filter((r: any) => dayjs(r.date).format("YYYY-MM-DD") === dayjs(date).format("YYYY-MM-DD"));
    const issueData3m = projectIssueData.filter((r: any) => dayjs(r.date).format("YYYY-MM-DD") === dayjs(date).format("YYYY-MM-DD"));
    const issue_count_1m = issueData1m.length;
    const issue_count_3m = issueData3m.length;

    // Bug统计
    const projectBugData = bugData.filter((r: any) => r.project_id === projectId);
    const bug_days = projectBugData.length > 0 ? projectBugData.map((r: any) => parseFloat(r.bug_days)) : [0];
    const bug_avg_days = (bug_days.reduce((a: number, b: number) => a + b, 0) / bug_days.length).toFixed(2);

    return {
      metrics_date: date,
      project_id: projectId,
      "metrics.keys": DWS_PROJECT_METRIC_KEYS,
      "metrics.values": [commit_count_1m.toString(), issue_count_1m.toString(), commit_count_3m.toString(), issue_count_3m.toString(), bug_avg_days],
      data_id: uuidv4(),
      data_created_at: dayjs(date).unix(),
      // 保存源数据ID用于生成血缘关系
      _source: {
        gitData1m: gitData1m.map((r: any) => r.data_id),
        gitData3m: gitData3m.map((r: any) => r.data_id),
        issueData1m: issueData1m.map((r: any) => r.data_id),
        issueData3m: issueData3m.map((r: any) => r.data_id),
        bugData: projectBugData.map((r: any) => r.data_id),
      },
    };
  });

  if (dwsRecords.length > 0) {
    // 插入数据到 dws 表
    await db.insert({
      table: "dws_projects",
      values: dwsRecords.map(({ _source, ...record }) => record),
      format: "JSONEachRow",
    });

    // 生成更详细的数据血缘关系
    const lineageRecords = dwsRecords.flatMap((dwsRecord: any) => {
      return [
        {
          from_table: "dwd_git_commits",
          from_ids: dwsRecord._source.gitData1m,
          to_table: "dws_projects",
          to_id: dwsRecord.data_id,
          to_facets: ["commit_count_in_1m"],
          created_at: dayjs(date).unix(),
        },
        {
          from_table: "dwd_git_commits",
          from_ids: dwsRecord._source.gitData3m,
          to_table: "dws_projects",
          to_id: dwsRecord.data_id,
          to_facets: ["commit_count_in_3m"],
          created_at: dayjs(date).unix(),
        },
        {
          from_table: "dwd_jira_issues",
          from_ids: dwsRecord._source.issueData1m,
          to_table: "dws_projects",
          to_id: dwsRecord.data_id,
          to_facets: ["issue_added_count_in_1m"],
          created_at: dayjs(date).unix(),
        },
        {
          from_table: "dwd_jira_issues",
          from_ids: dwsRecord._source.issueData3m,
          to_table: "dws_projects",
          to_id: dwsRecord.data_id,
          to_facets: ["issue_added_count_in_3m"],
          created_at: dayjs(date).unix(),
        },
        {
          from_table: "dwm_bugs",
          from_ids: dwsRecord._source.bugData,
          to_table: "dws_projects",
          to_id: dwsRecord.data_id,
          to_facets: ["bug_avg_days_in_1m"],
          created_at: dayjs(date).unix(),
        },
      ];
    });

    await db.insert({
      table: "data_lineage",
      values: lineageRecords,
      format: "JSONEachRow",
    });

    return lineageRecords;
  }

  return [];
}

async function get_data_lineage(root_record: { to_id: string; to_table: string; to_facets: string[]; from_table: string; from_ids: string[]; created_at: number }) {
  // 递归查询数据血缘关系
  const lineageRecords = [root_record];
  const visited = new Set<string>();

  async function getLineage(record: typeof root_record) {
    const key = `${record.from_table}_${record.from_ids.join(",")}`;
    if (visited.has(key)) {
      return;
    }
    visited.add(key);

    const childRecords = await db
      .query({
        query: `
        SELECT * FROM data_lineage 
        WHERE to_table = '${record.from_table}' 
        AND to_id IN (${record.from_ids.map((id) => `'${id}'`).join(",")})
      `,
        format: "JSONEachRow",
      })
      .then((res) => res.json());

    if (childRecords.length > 0) {
      lineageRecords.push(...(childRecords as (typeof root_record)[]));

      // 递归查询每条记录
      await Promise.all(childRecords.map((r) => getLineage(r as typeof root_record)));
    }
  }

  await getLineage(root_record);
  return lineageRecords;
}

async function main() {
  await Promise.all([make_ods_jira_issues(SIMULATION_DATE_START_DATE), make_ods_git_commits(SIMULATION_DATE_START_DATE)]);

  await Promise.all([make_dwd_jira_issues(SIMULATION_DATE_START_DATE), make_dwd_git_commits(SIMULATION_DATE_START_DATE)]);

  await make_dwm_bugs(SIMULATION_DATE_START_DATE);
  const topRecords = await make_dws_projects(SIMULATION_DATE_START_DATE);

  // for (const record of topRecords) {
  //   await get_data_lineage(record);
  // }

  console.log("Generate data done");
}

main().catch(console.error);