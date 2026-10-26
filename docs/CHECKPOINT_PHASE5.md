# Phase 5 checkpoint — October 26, 2026

This checkpoint preserves the streaming and query-hardening work completed since
the September 30 opt-in Kafka commit. Kafka remains opt-in, direct ingestion
remains the default, and Phase 6 has not started.

Verified work includes four-partition trace routing, bounded coalesced writes,
replay/offset safeguards, delivery-status API/UI, exact database-side detector
statistics, bounded representative evidence and explicit uncertainty, diagnostic
failure categories, and reproducible paired benchmarks. The historical Candidate
3 correctness suite passed 13 real Kafka/ClickHouse tests. Query revision 3 passed
seven of eight diagnostic performance gates; scheduled query drops still failed.
These results do not establish full promotion acceptance.

The trace-ID index is an optional experiment. Eight query integration tests passed
on October 12, including index replay/cutoff parity. Its subsequent candidate smoke
startup failed because Docker could not mount its migration beneath a read-only
directory. No query-revision-4 candidate workload or full promotion campaign ran.
The cutover procedure and harness remain proposals and have not been executed.

Checkpoint verification on October 26 reran the Go internal/command race tests,
all 30 frontend tests, the frontend production build, and all 60 benchmark Python
tests successfully. Historical
real-dependency results are preserved, not represented as freshly rerun tests.

See [query progress](QUERY_HARDENING_PHASE5.md),
[streaming verification](VERIFICATION_STREAM_HARDENING.md), and the
[evidence archive instructions](../benchmarks/archives/README.md). Raw observations
and failed attempts are preserved without promoting Kafka or weakening any gate.
