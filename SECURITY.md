# Security Model — Sky Workflow

## Implemented controls

- Workflow and step identifiers use bounded restricted character sets.
- Definitions, inputs, results and history payloads are JSON-validated and capped at 64 KiB each.
- Workflow start idempotency prevents duplicate instances from ordinary client retries.
- Step transitions require a per-workflow operation key and replay safely without applying the same operation twice.
- Current-step matching prevents callers from advancing an unexpected step.
- Retry and timeout handling share a bounded attempt budget, preventing infinite failure loops.
- SQLite uses WAL, `synchronous=FULL`, busy timeout and explicit write transactions for state transitions.
- Optional coarse bearer authentication uses constant-time comparison.
- The container runs as non-root and keeps mutable state in `/data`.

## Trust boundaries

This coordinator records application-supplied workflow data. Do not place plaintext secrets in workflow input/results unless deployment policy explicitly protects them. Use Sky Secret Vault for secret material and pass references where possible.

The bearer gate is service-level protection, not per-user authorization. Tenant/user authorization belongs at Sky Gateway/Sky Identity or another policy layer.

## Execution boundary

Sky Workflow never evaluates or executes arbitrary source code or shell commands. External workers perform actual business operations and report deterministic transitions.

## Single-node boundary

The SQLite store is a single-node persistence design. No cross-node consensus, global exactly-once execution, multi-region HA or disaster-recovery guarantee is claimed.

## Reporting

Use private security reporting when available. Do not disclose workflow databases, bearer tokens, secret-bearing payloads or customer data in public issues.
