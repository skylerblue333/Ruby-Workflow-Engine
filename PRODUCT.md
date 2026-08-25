# Sky Workflow — Product Definition

Sky Workflow began as standalone product #9 and is extended in **Wave 2 slot #133 / Lane 01** as the shared workflow state-machine and integration boundary.

## Product role

A small durable state coordinator for ordered multi-step business workflows. It records immutable step plans, current progress, bounded retry/timeout policy, idempotent transition operations and an append-only transition history.

Wave 2 adds a stable **data-minimized integration event** in `src/integration.py`. The event carries only schema version, workflow ID/type, lifecycle status, current step ID, current attempt and update timestamp. It intentionally excludes workflow input/output payloads and does not itself publish to a broker.

## SKYCOIN4444 integration

The event envelope is suitable for downstream SkyObservability, SkyStatus, SkyClassroom and other domain consumers that need workflow lifecycle metadata without duplicating workflow state or receiving full business payloads. Delivery/retry/broker semantics belong to SkyQueue/Webhook/Integration products rather than this helper.

## Explicit non-claims

It is not a distributed workflow cloud, arbitrary-code executor, cron scheduler, DAG engine, Temporal/Cadence replacement, HA consensus service or exactly-once distributed execution system. The integration event helper is not a live event bus, webhook delivery system, security enforcement point, or proof of production deployment.

## Productization gate

Exact-head CI must pass compile, Ruff, pytest, dependency audit, Docker build and non-root image checks. Completion is declared only after merge and default-branch read-back.
