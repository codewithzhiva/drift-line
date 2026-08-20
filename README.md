# DriftLine

Data quality, statistical drift, and downstream impact in one FastAPI control plane. DriftLine connects versioned profiles and contracts to OpenLineage-style job dependencies.

## Quick start

```bash
docker compose up --build
```

- API/OpenAPI: <http://localhost:8002/docs>
- Incident dashboard: <http://localhost:8086>

## Implemented analysis

- Required-column, minimum-row, and maximum-null-rate contracts.
- Population Stability Index and Jensen–Shannon divergence for binned distributions.
- Two-sample KS statistic for numeric samples.
- Idempotent lineage events and dataset-to-job-to-output impact reporting.
- Incident snapshot streaming over SSE.

Metrics are evidence, not automatic truth: every incident records its baseline, threshold, value, column, and observed consumers. v0.1 uses SQLite WAL and synchronous reference analysis; the API boundary supports moving heavy analysis to workers later.

See [architecture](docs/architecture.md) and [benchmark policy](docs/benchmarks.md). Apache-2.0 licensed.
