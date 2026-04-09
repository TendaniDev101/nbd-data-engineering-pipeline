"""
Stage 3 — Streaming extension: Process micro-batch JSONL files from /data/stream/.

This module is only used at Stage 3. You do not need to implement it for
Stage 1 or Stage 2.

Input paths:
  /data/stream/            — Directory of micro-batch JSONL files, arriving
                             during pipeline execution. The scoring system
                             drops new files here while your container runs.

Output paths (your pipeline must create these directories):
  /data/output/stream_gold/current_balances/    — 4 fields; upsert table
  /data/output/stream_gold/recent_transactions/ — 7 fields; last 50 per account

Requirements:
  - Poll /data/stream/ periodically for new files (the scoring system will
    deliver micro-batches over the course of the run).
  - Process each new file and merge results into the stream_gold tables.
  - current_balances: maintain one row per account_id (upsert/merge).
  - recent_transactions: maintain the 50 most recent transactions per account.
  - SLA: updated_at must be within 300 seconds of the source event timestamp
    for full credit; 300–600 seconds receives partial credit.
  - Write all stream_gold output as Delta Parquet tables.
  - The stream_ingest loop must terminate on its own when no new files have
    arrived for a reasonable quiesce period. The container has a 30-minute
    hard timeout — do not run indefinitely.

See output_schema_spec.md §5 and §6 for the full field-by-field specification
of current_balances and recent_transactions.

See docker_interface_contract.md §3 for the /data/stream/ mount details.
"""


def run_stream_ingestion():
    # TODO: Implement Stage 3 streaming ingestion.
    #
    # Suggested approach:
    #   1. Scan /data/stream/ for unprocessed JSONL files.
    #   2. For each new file, parse events and update stream_gold tables via
    #      Delta MERGE (upsert) to maintain current state.
    #   3. Evict records beyond the 50-row-per-account window in
    #      recent_transactions after each merge.
    #   4. Track which files have been processed (e.g. a processed_files.txt
    #      in /tmp) so you don't re-process on each poll cycle.
    #   5. Quiesce after N seconds of no new files arriving, then return.
    pass
