# data-lineage-bench

## Requirements

- Bun v1.1.25 or higher (https://bun.sh/docs/installation)

## Data Lineage Benchmark

```bash
docker compose up clickhouse -d
docker compose up migrate-clickhouse
```

## Generate Report

```bash
cd scripts/data-lineage-bench
bun install # only run once
bun run remake
```

This project was created using `bun init` in bun v1.1.25. [Bun](https://bun.sh) is a fast all-in-one JavaScript runtime.
