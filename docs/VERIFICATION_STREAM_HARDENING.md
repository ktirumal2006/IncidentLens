# Phase 5 streaming hardening verification

Final status, October 9, 2026: **candidate 3 is not promoted; tuning has stopped.**
The [bounded promotion decision](PHASE5_PROMOTION_DECISION.md) records the complete
matched diagnostic: five numerical gates passed, while visibility and query
error/drop gates failed. Every admitted identity in this pair recovered. The
full 16-case campaign and default/cutover rehearsal were not started under the
agreed stop rule. The direct path remains the default; no Phase 6 work began.
The [final pair audit](evidence/stream-hardening/candidate3-final-pair-audit.json)
verifies raw results and shared controls. The
[query classification](evidence/stream-hardening/candidate3-decision-query-classification.json)
separates detector-limit evidence from deadlines and unclassified errors without
removing any failure from acceptance.

Earlier audit, October 9, 2026 at 16:57 UTC: candidate 3's correctness suite
(13 real-dependency race tests) and paired smoke passed. Its diagnostic pair
failed during candidate startup, before candidate load: Compose waited 240 s
without Kafka becoming healthy. Cleanup and direct-stack restoration succeeded.
No benchmark driver is currently running. Candidate 2's completed diagnostic
pair failed four performance gates; candidate 1 also failed its completed
high-load pair. No candidate has completed the required full 16-case acceptance
campaign. Kafka default promotion remains pending; Phase 6 has not begun.
The October 9 audit of retained logs found Kafka reported `Kafka Server started`
at 00:46:54 UTC, about 22 seconds after container start, while the recorded
health probes repeatedly exceeded their five-second timeout. This narrows the
latest failure to readiness observation; startup logs alone do not prove every
broker operation was healthy. See the [health state](evidence/stream-hardening/progress-audit-kafka-state-20261009.json)
and [retained broker logs](evidence/stream-hardening/candidate3-startup-kafka-logs.txt).

The decision and promotion gates are in [ADR 0005](adr/0005-hardened-streaming-default.md)
and the [declared workload](../benchmarks/WORKLOAD_STREAM_HARDENING.md). The frozen
single-partition baseline has its own [source and image manifest](../benchmarks/baselines/phase5-streaming-20261003/manifest.json).
Both candidates use four fixed trace-ID partitions, bounded batches, coalesced
commits, and an independently sampled delivery-status API/UI. Candidate 1 used
two synchronous write lanes; candidate 2 combines each existing bounded cohort
into one synchronous write, with unchanged resource limits.

## Candidate 1 correctness evidence

The [initial real-dependency suite](evidence/stream-hardening/acceptance1-streaming-tests.txt)
ran against isolated Kafka and ClickHouse with the candidate images. Eleven
top-level tests passed. The broker-outage test hit a Compose dependency-start
timeout while completed topic and migration jobs were restarted. Its original
failure is retained. The test helper now restarts only named existing services;
both affected broker recovery tests [passed on rerun](evidence/stream-hardening/acceptance1-broker-recovery-retest.txt).

Together these runs verify:

- Four-partition routing, cross-record batches and fresh-group replay, including
  committed offsets and physical replay with duplicate-safe `FINAL` reads.
- Crash after write/before commit, same-group restart, retained broker restart,
  retryable broker outage, and duplicate-safe query/detector behavior.
- A second consumer requesting rebalance while a write is stalled; no commit
  covers the unwritten cohort. A partial two-lane write also replays safely.
- Poison records, retained-offset gaps, oversized decompression and invalid
  requests fail without advancing past unsuccessful work. Compressed valid
  batches continue making ordered progress.

[Go race checks](evidence/stream-hardening/unit-race-20261006.txt) cover the unit
suite; real-dependency tests in that command are gated off and are supported by
the separate runs above. The [52 Python checks](evidence/stream-hardening/python-tests-20261006.txt)
cover benchmark accounting and the exact paired acceptance calculations.

The [real API/browser acceptance](evidence/stream-hardening/status-browser2/result.json)
primed 288 spans and offered another 3,600 while ClickHouse was paused. Both
finite identity ledgers show every admitted span stored. The UI displayed rising
backlog and pending age, storage retry, drain and a completed processing delay.
Pausing the worker produced unavailable status; resuming it restored status.
The stale-rendering assertion separately freezes a real response in the browser;
it is not represented as a real broker outage. [Raw observations](evidence/stream-hardening/status-browser2/status-observations.jsonl)
and screenshots are retained with the result.

The earlier [browser attempt](evidence/stream-hardening/status-browser1/result.json)
passed the real stall/recovery assertions but failed the browser-only stale
check because the interception pattern missed the URL's trailing question mark.
That test pattern and its verification were corrected before the passing rerun.

The first candidate startup also exposed an invalid franz-go status-client
configuration: its read-response limit was below the default fetch budget.
Both limits are now bounded consistently and a constructor regression covers
this configuration. The [startup failure](evidence/stream-hardening/acceptance1-worker-startup-failure.txt)
is retained. The status sampler and worker are joined before their clients close.

## Benchmark evidence and remaining gates

Candidate 2's [real Kafka/ClickHouse suite](evidence/stream-hardening/acceptance2-streaming-tests.txt)
passed all 12 top-level tests with the race detector on October 8 UTC (154.057 s),
with no skips. This includes replay after an ambiguous partial batch write,
write-before-commit crash, stalled-write rebalance, four-partition routing,
duplicate-safe replay/detector checks, bounded decompression, poison records,
retained-offset gaps, and broker outage/restart. Its
[unit/race checks](evidence/stream-hardening/unit-race-candidate2.txt) and
[59 Python checks](evidence/stream-hardening/python-tests-candidate2-final.txt)
also passed. The [source manifest and archive](evidence/stream-hardening/candidate2-source-archive.json)
freeze the implementation and harness for the next paired comparison.
The [candidate 2 paired smoke](../benchmarks/results/20261008-hardening-candidate2-smoke/campaign.json)
completed with 888 admitted and stored spans in each variant, no evidence gaps,
and preserved experiment volumes. Its
[8,000-span/s diagnostic pair](../benchmarks/results/20261008-hardening-candidate2-probe/campaign.json)
completed. This probe cannot establish full campaign acceptance.
Its [exact paired gates](evidence/stream-hardening/candidate2-probe-gates.json)
passed admission (446,784 baseline versus 828,414 candidate measured spans),
ACK p95/p99 (1,096,579,000/1,225,502,000 versus 701,432,000/900,582,000 ns), and
completed write throughput (910.4 versus 3,487.2 rows/s). Both variants recovered
every admitted identity: 498,240 baseline and 909,246 candidate full-run spans.
However, candidate drain upper bound 89,935,234,000 ns exceeded half the baseline
166,901,936,000 ns. Censoring remained 381/425 versus baseline 231/231.
Non-404 query failures were 198/432 versus 166/467, and scheduling drops were
44/480 versus 13/480; both increases exceeded their gates. Candidate 2 is not
eligible for promotion, and its full 16-case campaign has not been run.
The slower baseline observed in this pair is kept as measured; no earlier
campaign result is substituted for it.
The [exact metric record](evidence/stream-hardening/candidate2-probe-metrics.json)
retains both variants' admission/storage counts, ACK tails, write/commit
throughput and timings, peak lag/drain bounds, visibility observations and every
query route's status/scheduling results. Candidate 3 is now declared to raise
only the cohort row target to 2,048. Its
[real Kafka/ClickHouse race suite](evidence/stream-hardening/acceptance3-verification.json)
passed all 13 top-level tests with no skips on October 9 UTC, including a new
2,304-row target-boundary/replay test. The
[post-load SELECT diagnostic query](evidence/stream-hardening/acceptance3-query-diagnostic-check.json)
was verified separately. Its [source archive](evidence/stream-hardening/candidate3-source-archive.json)
is frozen. The [paired smoke](../benchmarks/results/20261009-hardening-candidate3-smoke/campaign.json)
passed with 888 admitted/stored spans and valid query diagnostics in each
variant. The [fresh diagnostic high-load comparison](../benchmarks/results/20261009-hardening-candidate3-probe/campaign.json)
completed its baseline, then failed at candidate startup: the 240-second Compose
deadline expired while Kafka was waiting for health. Candidate telemetry load
never began. This is startup failure evidence, not a candidate throughput result.
The driver stopped the owned project and restored the direct stack successfully.
No performance improvement or default promotion is established for candidate 3.
The [incident-query correlation](evidence/stream-hardening/candidate2-query-limit-correlation.json)
shows candidate successes before 100,000 completed write rows and only errors
after that point, consistent with the unchanged detector input limit. Individual
503 causes were not recorded and can also include database/concurrency failures.
Faster persistence may reach this limit sooner; batching alone must not be
assumed to solve that query gate. Candidate 3 adds bounded post-load SELECT log
diagnostics to both variants without changing query caps or measured traffic.

The [paired smoke campaign](../benchmarks/results/20261006-hardening-smoke1/campaign.json)
completed for both variants. Each admitted and stored all 888 setup/load spans;
operation-recorder trailers, lag coverage and raw identity accounting validated.
This establishes instrumentation, not a throughput gain.

The [candidate 1 campaign](../benchmarks/results/20261006-hardening-candidate1/campaign.json)
stopped after the first pair because a nonempty topic still had uninitialized
consumer offsets in the first load sample. Both cases stored all 260766 admitted
identities. The unknown lag sample is retained; it is not rewritten as zero.
The workload amendment specifies a common setup-commit barrier before repeating
the campaign, with unchanged application images and numerical acceptance gates.
The [amended paired smoke](../benchmarks/results/20261007-hardening-smoke2/campaign.json)
passed for both variants, each storing all 888 identities. Both emitted the
setup-barrier event and had complete lag evidence; [its compact verification](evidence/stream-hardening/setup-barrier-verification-20261007.json)
also records the shared harness binary hash. [Harness race checks](evidence/stream-hardening/harness-barrier-race-20261007.txt)
and [52 analysis checks](evidence/stream-hardening/python-tests-20261007-corrected.txt)
passed. The [amended campaign](../benchmarks/results/20261007-hardening-candidate1/campaign.json)
completed its first 2,000-span/s pair and 8,000-span/s baseline, then lost Docker
during the candidate's warmup. The first pair passed its applicable numerical
gates; the 8,000-span/s baseline recovered all 1,001,598 admitted identities in
the supplemental window, with 39,168 planned spans never emitted. These partial
results do not establish campaign acceptance.
The [retry's first high-load pair](evidence/stream-hardening/retry1-first-high-pair-20261007.json)
had complete accounting and lag evidence, but failed seven of eight numerical
gates. Measured admission was 956,094 baseline spans versus 910,590 candidate
spans, passing the 95% requirement. Completed write throughput was 2,912 versus
3,352.6 rows/s, below the required 1.25x improvement. ACK p95/p99 were
389,967,000/706,383,000 ns versus 644,468,000/895,130,000 ns. Drain upper bounds
were 125,925,850,000 versus 70,963,513,000 ns, above the required half-baseline
bound. Visibility censoring was 497/497 versus 427/474. Non-404 query failures
were 132/477 versus 135/371, and scheduled query drops were 0/480 versus 103/480.
These failures prevent promotion despite improved write throughput and drain.

Candidate 2 tests whether one larger synchronous write per existing cohort
reduces the observed insert contention. It retains the four partitions, batch
limits, quotas, workload and numerical gates. The exact
[candidate 1 source archive](evidence/stream-hardening/candidate1-source-archive.json)
preserves the failed implementation. Candidate 2 requires fresh correctness
checks, paired smoke, a diagnostic high-load pair, and the full campaign if the
diagnostic warrants continuing.

Promotion requires all
declared performance gates, plus a clean Kafka-default startup and a retained
v1/v2 cutover/rollback reconciliation. Results and the final decision will be
added after those observations; no ClickHouse capacity increase is claimed.

### Interrupted attempt and retention loss

At the October 7 recovery audit the driver process was absent and Docker was
unavailable. The inspected containers had stopped around 02:34 UTC; the cause
is not established. Docker was restored after 14:24 UTC, beyond the topic's
one-hour retention. [The interruption record](evidence/stream-hardening/amended-campaign-interruption-20261007.json)
retains exit state (no inspected container was marked OOM-killed), and the original
campaign snapshot is preserved before its stale `running` status was annotated.

The incomplete run's ordinary reconciliation could not connect to ClickHouse
and must not be interpreted as zero admission or zero loss. Reconstructing exact
ACKed identities from its complete batch events found **23,808 acknowledged
spans**, of which **16,074 remain stored and 7,734 are missing**. The stored set
contains no unknown or duplicate logical identities. This is observed admitted
loss in the interrupted attempt, not just an observation-deadline miss.
The [identity reconciliation](evidence/stream-hardening/interrupted-recovery-20261007/identity-reconciliation.json)
lists the missing identities. Broker start and end offsets both reached 124 on
every partition while the group's earlier commits were behind; the worker
stopped with `OFFSET_OUT_OF_RANGE` instead of skipping that gap. The
[retained-offset evidence](evidence/stream-hardening/interrupted-recovery-20261007/retained-offset-gap.json)
and [volume-preserving shutdown](evidence/stream-hardening/interrupted-recovery-20261007/preserved-shutdown.json)
are retained. No offset reset, topic deletion or fabricated replay repairs this
loss. The retained topic cannot recover those expired records.

This interruption exceeded the declared bounded recovery protocol and is not
used as a paired performance result. It remains part of the reliability evidence:
one-hour retention does not protect an acknowledged backlog across a twelve-hour
outage. A fresh complete campaign must use the unchanged application images,
quotas, workload and numerical gates; earlier partial pairs are not substituted.
The [fresh retry campaign](../benchmarks/results/20261007-hardening-candidate1-retry1/campaign.json)
was launched with a detached runner and an idle-sleep assertion, but was also
interrupted after five complete cases. Its
[launch record](evidence/stream-hardening/candidate1-retry1-launch.json) identifies
the process and unchanged shared binaries. This changes process supervision,
not the benchmarked application or the declared numerical criteria. The
[second interruption audit](evidence/stream-hardening/retry1-interruption-20261007/runtime.json)
and [preserved-volume cleanup](evidence/stream-hardening/retry1-interruption-20261007/preserved-shutdown.json)
record the later interruption; it does not invalidate the completed matched
high-load pair or turn it into a passing campaign.

Original and experiment volumes are preserved. The [acceptance shutdown snapshot](evidence/stream-hardening/acceptance1-final-snapshot.json)
records unchanged volume identities. Pre-existing ClickHouse/Docker failures and
host restarts are recorded separately under `evidence/stream-hardening/`; they
are not counted as candidate benchmark outcomes.
