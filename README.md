# Sky Workflow

**SKYCOIN4444 standalone product #9** — a deterministic persisted workflow coordinator implemented in Python/FastAPI with SQLite/WAL.

The repository name is historical. The shipped implementation is Python, not Ruby, and the documentation follows the code rather than the old label.

## Implemented capability

- persisted workflow instances with immutable per-instance step definitions;
- ordered sequential steps with per-step timeout, retry delay and max-attempt policy;
- idempotent workflow starts;
- idempotent step-transition operations;
- deterministic current-step and attempt tracking;
- success transitions to the next step and terminal success after the final step;
- failure transitions through retry backoff and terminal failure when budget is exhausted;
- timeout reconciliation that consumes the same retry budget;
- workflow cancellation;
- append-only transition history with operation keys;
- bounded JSON definitions, inputs, results and history payloads;
- optional constant-time bearer protection for workflow APIs;
- `/healthz`, `/readyz`, and workflow-state `/metrics` endpoints;
- non-root container with persistent `/data` volume;
- tests covering start idempotency, successful progression, transition replay, retry/failure, timeout and cancellation;
- CI compile, Ruff, pytest, dependency audit, Docker build and non-root-image gates.

## Run locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
export WORKFLOW_DB_PATH=./data/workflows.db
uvicorn src.main:app --port 8080
```

Start a workflow:

```bash
curl -X POST http://localhost:8080/api/v1/workflows \
  -H 'content-type: application/json' \
  -d '{
    "workflowType":"order.fulfillment",
    "idempotencyKey":"order-42",
    "input":{"orderId":"42"},
    "steps":[
      {"stepId":"reserve","maxAttempts":3,"retryDelaySeconds":5,"timeoutSeconds":60},
      {"stepId":"ship","maxAttempts":2,"retryDelaySeconds":10,"timeoutSeconds":300}
    ]
  }'
```

Report a completed current step using a unique `operationKey`:

```bash
curl -X POST http://localhost:8080/api/v1/workflows/<id>/advance \
  -H 'content-type: application/json' \
  -d '{"stepId":"reserve","outcome":"success","operationKey":"reserve-op-1","result":{"reservation":"r-1"}}'
```

## Verify

```bash
python -m compileall -q src tests
ruff check src tests
pytest -q
pip-audit -r requirements.txt
docker build -t sky-workflow .
```

## Architecture

```text
Application / Worker
       │
       ▼
Sky Workflow
  ├─ immutable step plan
  ├─ current step + attempt
  ├─ timeout / retry policy
  ├─ idempotent transition keys
  ├─ append-only history
  └─ SQLite/WAL persistence
       │
       └─ future Sky Queue dispatch adapter
```

## Deliberate boundaries

See [`SECURITY.md`](SECURITY.md), [`PRODUCT.md`](PRODUCT.md), and [`MASTER_PLAN.md`](MASTER_PLAN.md).

Sky Workflow coordinates state; it does **not** execute arbitrary user code. It is not represented as Temporal, Cadence, Step Functions, Airflow or a distributed exactly-once workflow engine. Scheduling/cron, distributed worker dispatch, compensation/Saga DSLs, HA replication and a visual workflow builder remain separate future integrations.

## License

See [`LICENSE`](LICENSE).
