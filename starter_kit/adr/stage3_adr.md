# Architecture Decision Record: Stage 3 Streaming Extension

**File:** `adr/stage3_adr.md`  
**Author:** tendanimukhithi  
**Date:** 2026-04-19  
**Status:** Final

---

## Context

Stage 3 introduced a near-real-time requirement on top of the existing batch pipeline: process pre-staged micro-batch JSONL files from `/data/stream/`, in filename order, and produce two additional Delta outputs under `/data/output/stream_gold/`:
1. `current_balances` (one row per account, upsert semantics), and  
2. `recent_transactions` (last 50 transactions per account).

The SLA requirement is that `updated_at` should be within 5 minutes of event time for full credit. The pipeline still had to keep Stage 2 batch behavior intact: Bronze/Silver/Gold outputs, DQ logic driven by `config/dq_rules.yaml`, and `dq_report.json` generation.

Coming into Stage 3, Stage 1 and Stage 2 were implemented in modular files: `pipeline/ingest.py`, `pipeline/transform.py`, `pipeline/provision.py`, with orchestration in `pipeline/run_all.py`. Stage 2 already established key patterns used in Stage 3: config path resolution, Spark session bootstrap, Delta writes, and deterministic transformations.

---

## Decision 1: How did the existing Stage 1 architecture facilitate or hinder the streaming extension?

The architecture helped in three important ways. First, the stage separation (`ingest.py`, `transform.py`, `provision.py`) meant Stage 3 could be added as a new module (`pipeline/stream_ingest.py`) instead of rewriting existing batch logic. Second, the config-driven approach and path resolution functions from Stage 1/2 made it straightforward to add stream inputs and stream output paths without hardcoding environment-specific locations. Third, using Delta as the common storage layer in earlier stages made stream table maintenance natural, because Stage 3 also needed Delta outputs and stateful table updates.

The architecture also created friction. Spark session construction was duplicated in multiple modules, so Stage 3 required repeating runtime hardening logic (driver host binding, local hostname behavior, compression settings, local jar handling) instead of reusing a shared runtime utility. `run_all.py` is a linear entrypoint, so adding stream behavior there is simple but less flexible than a mode-based launcher. Another friction point was that batch and stream schemas were still defined inline in modules, which made stream-table introduction more manual than it needed to be.

Estimated code survival: roughly 85–90% of Stage 1/2 code survived untouched. Stage 3 was mostly additive: new `stream_ingest.py` plus a small call-site change in `run_all.py`.

---

## Decision 2: What design decisions in Stage 1 would I change in hindsight?

In hindsight, I would introduce a shared runtime/core layer in Stage 1 with a single Spark bootstrap utility and shared schema/config helpers. Right now, `_build_spark_session` and path resolution patterns are repeated across `ingest.py`, `transform.py`, `provision.py`, and now `stream_ingest.py`. This repetition increased Stage 3 implementation risk because every runtime fix had to be applied in several places.

I would also separate table schema definitions into a dedicated module, for example `pipeline/schemas.py`, with explicit builders for batch and stream output schemas. In Stage 3, stream table schema creation was coded directly in `stream_ingest.py`. That works, but it reduces consistency and makes future schema evolution harder to reason about.

Finally, I would redesign `run_all.py` early to support explicit execution modes (`batch`, `stream`, `all`) and lifecycle behavior. The current linear orchestration is valid for this challenge, but mode-aware orchestration would have made Stage 3 integration cleaner and easier to test in isolation.

---

## Decision 3: How would I approach this differently with full Stage 1–3 visibility?

With full visibility from Day 1, I would design a small shared pipeline framework with two adapters: batch source adapter and stream source adapter, both producing normalized event frames into common transformation primitives. I would keep one shared Spark bootstrap utility and one shared config object, including batch paths, stream paths, polling parameters, and runtime safety defaults.

For state management, I would define explicit stream state contracts up front: `current_balances` as account-level state and `recent_transactions` as bounded event history (top-N by account). I would still use Delta tables as the state backend because they fit challenge constraints (no external infrastructure, no queue, local container execution) and support deterministic local testing.

For entrypoints, I would expose `pipeline/run_all.py` as orchestrator plus mode options, so CI and local test flows can run `batch` or `stream` independently when needed. I would also centralize validation helpers (row count checks, schema checks, table readability checks) to avoid drift between development and harness expectations. Overall, the big change would be shifting from stage-specific scripts to reusable dataflow components with stage-specific wiring.
