# OFM Order Saga Service

## Purpose

The Order Saga Service orchestrates the order lifecycle across gig, order, payment, chat, file, review, and user contexts. It owns saga state and workflow decisions, not the entities managed by those services. Status: active.

## Flow and boundaries

The gateway starts an order through the saga API. The saga persists the current step, publishes commands through NATS, consumes per-step results, and advances or compensates the workflow. Delivery acceptance, revisions, disputes, completion, chat creation, payout, and review eligibility are orchestration decisions.

Saga state is authoritative in PostgreSQL. Outbox and processed-event records make command publication and result handling idempotent. Kafka/CDC projection events are separate from NATS workflow commands.

## Configuration

.env.example groups are DB_*, MIGRATIONS_*, gRPC listener/client targets, NATS subjects and durable consumers, Kafka/CDC settings, timeouts/retries, and observability. Database values select saga state; NATS values select workflow commands/results; timeout values define how long steps may remain pending.

## Local development

    cp .env.example .env
    just run
    go test ./...

## Build and operations

Dockerfile builds ofm/order-saga-service:<tag>. Helm deploys the saga and ofm-infra supplies dependencies. Diagnose saga steps, NATS results, outbox records, Kafka projection events, retries, and payment settlement state.

