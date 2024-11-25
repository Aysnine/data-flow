import { createRoot } from "react-dom/client";
import { CartesianGrid, Legend, Line, Tooltip, XAxis, YAxis } from "recharts";
import { LineChart } from "recharts";

const rawBenchmarkResults = JSON.parse(process.env.BENCHMARK_RESULTS!) as {
  args: Record<string, string>;
  timings: {
    date: string;
    duration_ms: number;
    table_stats: {
      database: string;
      table: string;
      total_bytes: string;
      total_rows: string;
      part_count: string;
    }[];
  }[];
};

const benchmarkResults = {
  args: rawBenchmarkResults.args,
  timings: rawBenchmarkResults.timings.map((t) => ({
    ...t,
    table_stats: t.table_stats.map((stat) => ({
      ...stat,
      total_bytes: parseInt(stat.total_bytes),
      total_rows: parseInt(stat.total_rows),
      part_count: parseInt(stat.part_count),
    })),
  })),
};

console.log(benchmarkResults);

const data = benchmarkResults.timings.map((t) => ({
  date: t.date,
  duration_ms: t.duration_ms,
  ...t.table_stats.reduce(
    (acc, stat) => ({
      ...acc,
      [`${stat.table}_bytes`]: stat.total_bytes,
      [`${stat.table}_rows`]: stat.total_rows,
      [`${stat.table}_part_count`]: stat.part_count,
    }),
    {}
  ),
}));

function App() {
  return (
    <div style={{ padding: 20 }}>
      <div style={{ margin: "auto", width: 1024 }}>
        <LineChart width={1024} height={620} data={data} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="date" />
          <YAxis yAxisId="left" />
          <YAxis yAxisId="right" orientation="right" />
          <Tooltip />
          <Legend />
          <Line yAxisId="right" type="monotone" dataKey="duration_ms" stroke="#8884d8" dot={false} />
          {benchmarkResults.timings[0].table_stats.map((stat) => (
            <Line
              yAxisId="left" 
              key={stat.table}
              type="monotone"
              dataKey={`${stat.table}_rows`}
              stroke={stat.table === "data_lineage" ? "#8884d8" : "#82ca9d"}
              dot={false}
            />
          ))}
        </LineChart>
        <div>
          <h3>Benchmark Args</h3>
          <pre>{JSON.stringify(benchmarkResults.args, null, 2)}</pre>
        </div>
      </div>
    </div>
  );
}

const container = document.getElementById("root");
const root = createRoot(container!);

root.render(<App />);
