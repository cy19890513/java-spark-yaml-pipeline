# java-spark-yaml-pipeline

A config-driven ETL framework on Apache Spark: declare data pipelines in YAML, run them anywhere Spark runs.

```yaml
name: user-events-daily

sources:
  - name: raw_events
    type: jdbc
    options:
      url: jdbc:postgresql://db:5432/app
      dbtable: events

transforms:
  - name: cleaned
    type: sql
    input: raw_events
    sql: "SELECT user_id, event_type, ts FROM __input__ WHERE ts >= current_date() - 1"

quality:
  - name: no_null_users
    input: cleaned
    type: not_null
    columns: [user_id]
    on_failure: fail

sinks:
  - name: gold_events
    type: parquet
    input: cleaned
    path: ./data/output/events
    mode: overwrite
    partitionBy: [dt]
```

Run it:

```bash
java -jar etl-cli/target/etl-cli-0.1.0-SNAPSHOT.jar pipeline.yaml
```

Options:

```bash
java -jar etl-cli/target/etl-cli-0.1.0-SNAPSHOT.jar pipeline.yaml \
  --master spark://cluster:7077 \
  --conf spark.executor.memory=4g
```

`--master` defaults to `local[*]`; `--conf key=value` can be repeated for
any Spark setting. Exit codes: 0 success, 1 bad arguments or unreadable
YAML, 2 validation failure, 3 fail-fast quality abort, 4 other errors.

Try the bundled sample from the repository root:

```bash
java -jar etl-cli/target/etl-cli-0.1.0-SNAPSHOT.jar examples/sample-pipeline.yaml
```

It reads `examples/data/users.csv`, keeps the adult users, checks that no
id is null, and writes Parquet to `examples/output/adult_users`.

## Architecture

```
+---------------------------+
| 1. Definition: pipeline   |
|    YAML written by user   |
+-------------+-------------+
              | Jackson parsing + validation
+-------------v-------------+
| 2. Engine: DAG scheduling |
|    topological sort,      |
|    SparkSession lifecycle |
+---+----------+------------+
    |          |
+---v---+  +---v-----+  +--------v-+
|Source |  |Transform|  | Quality  |
|plugins|  | plugins |  | plugins  |
+---+---+  +---+-----+  +----+-----+
    |          |             |
+---v----------v-------------v----+
| 3. Sinks + observability       |
|    metrics, lineage, alerting  |
+--------------------------------+
```

Design principles:

1. **Plugin registry** — every source, transform, quality check, and sink is a plugin
   registered by name. Adding a new connector never touches the engine.
2. **Uniform stage contract** — each stage consumes named DataFrames and produces
   named DataFrames; the engine only schedules them in DAG order.
3. **Quality gates** — checks run after transforms with `warn` or `fail` severity;
   `fail` stops the pipeline before bad data reaches the sink.
4. **Definition / execution split** — YAML describes *what* to do; the engine
   decides *how*. Swap Spark versions or environments without touching pipelines.

## Modules

| Module            | Contents                                                                 |
|-------------------|--------------------------------------------------------------------------|
| `etl-api`         | Plugin interfaces (`SourceStage`, `TransformStage`, `QualityCheck`, `SinkStage`), `StageRegistry`, pipeline definition model |
| `etl-core`        | YAML parsing and validation, DAG topological sort, `SparkSessionManager`, `PipelineRunner` |
| `etl-connectors`  | Source/sink plugins: CSV, Parquet, JDBC                                   |
| `etl-quality`     | Quality check plugins: `not_null`, `unique`, `freshness` (`warn`/`fail`)  |
| `etl-cli`         | `main` entry point: `java -jar etl-cli.jar pipeline.yaml`                 |

## Build

Requirements: Java 17, Maven 3.8+.

```bash
mvn -q -DskipTests package
```

Run the sample pipeline (after the CLI module is complete):

```bash
java -jar etl-cli/target/etl-cli-0.1.0-SNAPSHOT.jar etl-cli/src/main/resources/sample-pipeline.yaml
```

## Roadmap

- [x] PR 1: Maven multi-module skeleton
- [ ] PR 2: `etl-api` plugin interfaces and registry
- [ ] PR 3: YAML parsing, validation, unit tests
- [ ] PR 4: DAG engine and pipeline runner
- [ ] PR 5: CSV / Parquet / JDBC connectors
- [ ] PR 6: Data quality checks with fail-fast
- [ ] PR 7: CLI entry point and end-to-end sample
