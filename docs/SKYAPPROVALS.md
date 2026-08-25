# SkyApprovals (#131)

Status: **engineering beta / deterministic approval-policy core**.

SkyApprovals is a bounded domain module layered beside the existing Sky Workflow service. It models approval requests, eligible approvers, quorum, individual approve/reject decisions, cancellation, and terminal status without treating an approval vote as authorization to execute an external action.

## Implemented rules

- safe bounded request, subject, and approver identifiers
- 1–20 unique eligible approvers
- quorum must be achievable by the eligible set
- each eligible actor may decide once
- rejection is terminal
- approval becomes terminal only when quorum is reached
- cancellation applies only while pending
- immutable request values and deterministic decision ordering
- `sky.approvals.workflow.v1` summary contract for a workflow consumer

## SKYCOIN4444 integration

`workflow_contract()` exposes only the request identifier, subject, status, quorum, approval count, and decision count. A SkyWorkflow/SkyTasks consumer can use that contract as input to its own transition policy without importing approval-module internals.

## Security and truth boundary

This module does **not** authenticate approvers, prove identity, evaluate RBAC/ABAC, persist decisions, produce electronic signatures, execute workflow steps, authorize payments, satisfy legal-signature requirements, or certify regulatory approval. Callers must establish actor identity and authorization before accepting a decision and must use durable storage/audit controls for consequential workflows.
