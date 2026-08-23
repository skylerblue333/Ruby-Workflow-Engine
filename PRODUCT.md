# Sky Workflow — Product Definition

Sky Workflow is product #9 in the SKYCOIN4444 standalone-product roadmap.

## Product role

A small durable state coordinator for ordered multi-step business workflows. It records immutable step plans, current progress, bounded retry/timeout policy, idempotent transition operations and an append-only transition history.

## Explicit non-claims

It is not a distributed workflow cloud, arbitrary-code executor, cron scheduler, DAG engine, Temporal/Cadence replacement, HA consensus service or exactly-once distributed execution system.

## Productization gate

Exact-head CI must pass compile, Ruff, pytest, dependency audit, Docker build and non-root image checks. Completion is declared only after merge and default-branch read-back.

## Future integration

Sky Queue becomes the durable worker-dispatch layer; the CSharp Cron Manager becomes a schedule-trigger adapter; Sky Event Ledger can receive business-domain events without duplicating workflow state storage.
