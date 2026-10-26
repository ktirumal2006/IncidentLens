# Controlled retrospective analysis

Counts and durations are exact integers; rates/error fractions are reduced rational strings. A missing recorder/lag trailer or incomplete identities invalidates the affected case. Direct ACK is storage completion; streaming ACK is broker admission.

| Case | Path | Status | Generated measured | ACKed measured | FINAL unique | ACK p50/p95/p99 ns | ACKed missing at deadline |
| --- | --- | --- | ---: | ---: | ---: | --- | ---: |
| sustained-500-r1-direct | direct | observed | 59814 | 59814 | 65766 | 25112000/36844000/54551000 | 0 |
| sustained-500-r1-streaming | streaming | observed | 59814 | 59814 | 65766 | 21046000/35637000/43785000 | 0 |
| sustained-2000-r1-direct | direct | observed | 239838 | 239838 | 260766 | 24784000/64803000/198416000 | 0 |
| sustained-2000-r1-streaming | streaming | observed | 239838 | 239838 | 260766 | 19711000/30839000/44328000 | 0 |
| sustained-8000-r1-direct | direct | observed | 369726 | 369726 | 450558 | 1001670000/1801645000/2341424000 | 0 |
| sustained-8000-r1-streaming | streaming | observed | 958974 | 958974 | 689728 | 23929000/106344000/337187000 | 350078 |
| burst-2000-r1-direct | direct | observed | 19998 | 19998 | 20766 | 24290000/32355000/36476000 | 0 |
| burst-2000-r1-streaming | streaming | observed | 19998 | 19998 | 20766 | 19079000/29648000/34955000 | 0 |
| burst-8000-r1-direct | direct | observed | 76158 | 76158 | 76926 | 430515000/733722000/880151000 | 0 |
| burst-8000-r1-streaming | streaming | observed | 79614 | 79614 | 80382 | 27549000/77157000/95958000 | 0 |
| burst-32000-r1-direct | direct | observed | 85056 | 85056 | 85824 | 398597000/708509000/898253000 | 0 |
| burst-32000-r1-streaming | streaming | observed | 111936 | 111936 | 112704 | 298765000/466840000/490096000 | 0 |
| slowdown-2000-r1-direct | direct | observed | 207774 | 207774 | 228702 | 27908000/4998915000/7398135000 | 0 |
| slowdown-2000-r1-streaming | streaming | observed | 239838 | 239838 | 260766 | 20541000/35432000/51693000 | 0 |
| sustained-500-r2-streaming | streaming | observed | 59814 | 59814 | 65766 | 19480000/27429000/38814000 | 0 |
| sustained-500-r2-direct | direct | observed | 59814 | 59814 | 65766 | 24626000/34700000/53965000 | 0 |
| sustained-2000-r2-streaming | streaming | observed | 239838 | 239838 | 260766 | 19076000/36297000/272815000 | 0 |
| sustained-2000-r2-direct | direct | observed | 239838 | 239838 | 260766 | 24206000/44585000/99558000 | 0 |
| sustained-8000-r2-streaming | streaming | observed | 959934 | 959934 | 642816 | 36778000/246304000/367714000 | 397950 |
| sustained-8000-r2-direct | direct | observed | 419904 | 419904 | 500736 | 899202000/1396628000/1602188000 | 0 |
| burst-2000-r2-streaming | streaming | observed | 19998 | 19998 | 20766 | 18711000/27419000/33690000 | 0 |
| burst-2000-r2-direct | direct | observed | 19998 | 19998 | 20766 | 24082000/32606000/35218000 | 0 |
| burst-8000-r2-streaming | streaming | observed | 79998 | 79998 | 80766 | 20355000/46250000/65226000 | 0 |
| burst-8000-r2-direct | direct | observed | 79998 | 79998 | 80766 | 23651000/151771000/344941000 | 0 |
| burst-32000-r2-streaming | streaming | observed | 120384 | 120384 | 121152 | 283799000/391241000/675974000 | 0 |
| burst-32000-r2-direct | direct | observed | 93312 | 93312 | 94080 | 359460000/696139000/802920000 | 0 |
| slowdown-2000-r2-streaming | streaming | observed | 239838 | 239838 | 260766 | 32134000/73038000/108440000 | 0 |
| slowdown-2000-r2-direct | direct | observed | 186078 | 185694 | 207006 | 1510466000/2791304000/15508680000 | 0 |

## Hypothesis evidence map

- Burst absorption: compare exact ACK counts and latency during each ten-second pulse with one-second broker production, writer/commit buckets, lag growth and later drain bounds.
- Ingestion/storage decoupling: compare streaming broker ACK buckets with ClickHouse write/commit buckets and FINAL identity counts; do not equate broker acceptance with visibility.
- Backpressure isolation: compare scheduling drops, failed exports and producer failures with direct write failures at matched rates; distinguish caller/Collector queues from product capacity.
- Downstream slowdown recovery: compare the paired 0.25-CPU intervention records, write/commit progress, lag and post-restore drain. A case without a completed intervention cannot support this claim.

## Per-case evidence

### sustained-500-r1-direct
Path direct; status observed; planned 65766, never emitted 0, unresolved emitted 0. 
- ingest/clickhouse_write: 60006 completed physical rows; 0 completed failures; 10001/20 completed rows/s. Start-window latency cohort: 312 successes, 0 failures; total p50/p95/p99 ns 9074254/13721967/19339399.
  - prepare p50/p95/p99 ns 2345949/3622691/4154455 (n=312).
  - append p50/p95/p99 ns 1013176/2581082/3411660 (n=312).
  - send p50/p95/p99 ns 5286564/8332205/13333243 (n=312).
- Last successful ClickHouse write finished -184002176 ns relative to end of offering; positive values are post-pulse writes.
- services query: p95 47249000 ns; failures 0/120 (0).
- traces query: p95 46396000 ns; failures 0/120 (0).
- detail query: p95 80786000 ns; failures 0/120 (0).
- incidents query: p95 581425000 ns; failures 0/120 (0).
- detail scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- incidents scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- services scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- traces scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- Visibility probes, separate from paced queries: 60 HTTP requests, 29 HTTP failures; 31 sampled batches, 0 censored.
- ClickHouse server QueryFinish (all case): 343 records, 65766 written rows, source-ms duration p50/p95/p99 7/11/16; selected ProfileEvents totals {'InsertedRows': 65766, 'InsertedBytes': 67284629, 'DiskWriteElapsedMicroseconds': 36882, 'NetworkReceiveElapsedMicroseconds': 615684, 'MergeTreeDataWriterRows': 65766, 'InsertedCompactParts': 343}.

### sustained-500-r1-streaming
Path streaming; status observed; planned 65766, never emitted 0, unresolved emitted 0. 
- producer/kafka_produce: 60006 completed physical rows; 0 completed failures; 10001/20 completed rows/s. Start-window latency cohort: 312 successes, 0 failures; total p50/p95/p99 ns 2850206/5307766/23575040.
- worker/clickhouse_write: 60006 completed physical rows; 0 completed failures; 10001/20 completed rows/s. Start-window latency cohort: 313 successes, 0 failures; total p50/p95/p99 ns 9901939/15034309/24625396.
  - prepare p50/p95/p99 ns 2543420/4088562/7286242 (n=313).
  - append p50/p95/p99 ns 1687949/2866733/4280287 (n=313).
  - send p50/p95/p99 ns 5539908/8966266/13313913 (n=313).
- worker/consumer_commit: 60006 completed physical rows; 0 completed failures; 10001/20 completed rows/s. Start-window latency cohort: 313 successes, 0 failures; total p50/p95/p99 ns 7630191/10879672/25997504.
- Last successful ClickHouse write finished -177591008 ns relative to end of offering; positive values are post-pulse writes.
- Kafka lag records: first 0, max sampled 1, last 0; post-offering drain {'status': 'observed_bounded', 'lower_bound_ns': 0, 'upper_bound_ns': 1501098000, 'first_zero_started_ns': 1790893315294822000, 'second_zero_finished_ns': 1790893316305994000, 'stable_end': 343}.
- services query: p95 66448000 ns; failures 0/120 (0).
- traces query: p95 43780000 ns; failures 0/120 (0).
- detail query: p95 55832000 ns; failures 4/120 (1/30).
- incidents query: p95 587323000 ns; failures 0/120 (0).
- detail scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- incidents scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- services scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- traces scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- Visibility probes, separate from paced queries: 62 HTTP requests, 31 HTTP failures; 31 sampled batches, 0 censored.
- ClickHouse server QueryFinish (all case): 343 records, 65766 written rows, source-ms duration p50/p95/p99 8/13/22; selected ProfileEvents totals {'InsertedRows': 65766, 'InsertedBytes': 67284629, 'DiskWriteElapsedMicroseconds': 39869, 'NetworkReceiveElapsedMicroseconds': 949659, 'MergeTreeDataWriterRows': 65766, 'InsertedCompactParts': 343}.

### sustained-2000-r1-direct
Path direct; status observed; planned 260766, never emitted 0, unresolved emitted 0. 
- ingest/clickhouse_write: 240030 completed physical rows; 0 completed failures; 8001/4 completed rows/s. Start-window latency cohort: 1250 successes, 0 failures; total p50/p95/p99 ns 8934194/45093768/171689691.
  - prepare p50/p95/p99 ns 2369249/20556848/76800638 (n=1250).
  - append p50/p95/p99 ns 1179862/2923879/5504233 (n=1250).
  - send p50/p95/p99 ns 5196273/11520819/97612622 (n=1250).
- Last successful ClickHouse write finished -4310919 ns relative to end of offering; positive values are post-pulse writes.
- services query: p95 197830000 ns; failures 0/120 (0).
- traces query: p95 138284000 ns; failures 0/120 (0).
- detail query: p95 243025000 ns; failures 2/120 (1/60).
- incidents query: p95 779037000 ns; failures 81/120 (27/40).
- detail scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- incidents scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- services scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- traces scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- Visibility probes, separate from paced queries: 261 HTTP requests, 136 HTTP failures; 125 sampled batches, 0 censored.
- ClickHouse server QueryFinish (all case): 1359 records, 260766 written rows, source-ms duration p50/p95/p99 7/18/101; selected ProfileEvents totals {'InsertedRows': 260766, 'InsertedBytes': 266984985, 'DiskWriteElapsedMicroseconds': 163388, 'NetworkReceiveElapsedMicroseconds': 3082146, 'MergeTreeDataWriterRows': 260766, 'InsertedCompactParts': 1359}.

### sustained-2000-r1-streaming
Path streaming; status observed; planned 260766, never emitted 0, unresolved emitted 0. 
- producer/kafka_produce: 240030 completed physical rows; 0 completed failures; 8001/4 completed rows/s. Start-window latency cohort: 1250 successes, 0 failures; total p50/p95/p99 ns 2427723/4884492/16673709.
- worker/clickhouse_write: 240030 completed physical rows; 0 completed failures; 8001/4 completed rows/s. Start-window latency cohort: 1251 successes, 0 failures; total p50/p95/p99 ns 9775854/55861636/81655126.
  - prepare p50/p95/p99 ns 2551948/42447438/67183124 (n=1251).
  - append p50/p95/p99 ns 1685583/3296960/5407112 (n=1251).
  - send p50/p95/p99 ns 5468987/10403369/36364986 (n=1251).
- worker/consumer_commit: 240000 completed physical rows; 0 completed failures; 2000 completed rows/s. Start-window latency cohort: 1251 successes, 0 failures; total p50/p95/p99 ns 7242410/8857346/17404586.
- Last successful ClickHouse write finished -2736956 ns relative to end of offering; positive values are post-pulse writes.
- Kafka lag records: first 0, max sampled 6, last 0; post-offering drain {'status': 'observed_bounded', 'lower_bound_ns': 0, 'upper_bound_ns': 1830618000, 'first_zero_started_ns': 1790893724841035000, 'second_zero_finished_ns': 1790893725847584000, 'stable_end': 1359}.
- services query: p95 360816000 ns; failures 0/120 (0).
- traces query: p95 336135000 ns; failures 0/120 (0).
- detail query: p95 161323000 ns; failures 29/120 (29/120).
- incidents query: p95 844767000 ns; failures 81/120 (27/40).
- detail scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- incidents scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- services scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- traces scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- Visibility probes, separate from paced queries: 243 HTTP requests, 118 HTTP failures; 125 sampled batches, 0 censored.
- ClickHouse server QueryFinish (all case): 1359 records, 260766 written rows, source-ms duration p50/p95/p99 8/18/75; selected ProfileEvents totals {'InsertedRows': 260766, 'InsertedBytes': 266984985, 'DiskWriteElapsedMicroseconds': 160614, 'NetworkReceiveElapsedMicroseconds': 3391795, 'MergeTreeDataWriterRows': 260766, 'InsertedCompactParts': 1359}.

### sustained-8000-r1-direct
Path direct; status observed; planned 1040766, never emitted 590208, unresolved emitted 0. 
- ingest/clickhouse_write: 366144 completed physical rows; 0 completed failures; 15256/5 completed rows/s. Start-window latency cohort: 1907 successes, 0 failures; total p50/p95/p99 ns 85003456/189099770/282597197.
  - prepare p50/p95/p99 ns 69957776/82642498/118148620 (n=1907).
  - append p50/p95/p99 ns 1233888/5427531/8420905 (n=1907).
  - send p50/p95/p99 ns 12069551/107192826/196992365 (n=1907).
- Last successful ClickHouse write finished 8770447435 ns relative to end of offering; positive values are post-pulse writes.
- services query: p95 1284422000 ns; failures 30/106 (15/53).
- traces query: p95 2001390000 ns; failures 32/107 (32/107).
- detail query: p95 2193652000 ns; failures 42/110 (21/55).
- incidents query: p95 1165645000 ns; failures 103/109 (103/109).
- detail scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 112, 'queue_dropped': 8, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 110, 'late_start': 2}.
- incidents scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 112, 'queue_dropped': 8, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 109, 'late_start': 3}.
- services scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 107, 'queue_dropped': 13, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 106, 'late_start': 1}.
- traces scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 108, 'queue_dropped': 12, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 107, 'late_start': 1}.
- Visibility probes, separate from paced queries: 959 HTTP requests, 781 HTTP failures; 206 sampled batches, 28 censored.
- ClickHouse server QueryFinish (all case): 2347 records, 450558 written rows, source-ms duration p50/p95/p99 18/171/207; selected ProfileEvents totals {'InsertedRows': 450558, 'InsertedBytes': 461490647, 'DiskWriteElapsedMicroseconds': 2379512, 'NetworkReceiveElapsedMicroseconds': 14669560, 'MergeTreeDataWriterRows': 450558, 'InsertedCompactParts': 2347}.

### sustained-8000-r1-streaming
Path streaming; status observed; planned 1040766, never emitted 960, unresolved emitted 350078. 
- producer/kafka_produce: 954624 completed physical rows; 0 completed failures; 39776/5 completed rows/s. Start-window latency cohort: 4972 successes, 0 failures; total p50/p95/p99 ns 2447212/7134066/30806848.
- worker/clickhouse_write: 409536 completed physical rows; 0 completed failures; 17064/5 completed rows/s. Start-window latency cohort: 2133 successes, 0 failures; total p50/p95/p99 ns 14020002/77407538/161500547.
  - prepare p50/p95/p99 ns 3364305/62020282/69001115 (n=2133).
  - append p50/p95/p99 ns 2302497/4724377/7364955 (n=2133).
  - send p50/p95/p99 ns 7487907/56309116/92284920 (n=2133).
- worker/consumer_commit: 409536 completed physical rows; 0 completed failures; 17064/5 completed rows/s. Start-window latency cohort: 2133 successes, 0 failures; total p50/p95/p99 ns 7566547/10378011/23617441.
- Last successful ClickHouse write finished 110618479348 ns relative to end of offering; positive values are post-pulse writes.
- Kafka lag records: first 15, max sampled 3063, last 0; post-offering drain {'status': 'observed_bounded', 'lower_bound_ns': 109815030000, 'upper_bound_ns': 111821142000, 'first_zero_started_ns': 1790894255177013000, 'second_zero_finished_ns': 1790894256183260000, 'stable_end': 5416}.
- Separate supplemental drain: complete=True; original-deadline missing remains 350078 ACKed spans.
- services query: p95 496841000 ns; failures 3/119 (3/119).
- traces query: p95 846816000 ns; failures 2/119 (2/119).
- detail query: p95 None ns; failures 119/119 (1).
- incidents query: p95 1199038000 ns; failures 104/119 (104/119).
- detail scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 119, 'late_start': 1}.
- incidents scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 119, 'late_start': 1}.
- services scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 119, 'late_start': 1}.
- traces scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 119, 'late_start': 1}.
- Visibility probes, separate from paced queries: 995 HTTP requests, 995 HTTP failures; 500 sampled batches, 500 censored.
- ClickHouse server QueryFinish (all case): 5416 records, 1039806 written rows, source-ms duration p50/p95/p99 9/50/80; selected ProfileEvents totals {'InsertedRows': 1039806, 'InsertedBytes': 1065243227, 'DiskWriteElapsedMicroseconds': 1138448, 'NetworkReceiveElapsedMicroseconds': 17276716, 'MergeTreeDataWriterRows': 1039806, 'InsertedCompactParts': 5416}.
- Supplemental FINAL unique stored: 1039806; original snapshot stored 689728.

### burst-2000-r1-direct
Path direct; status observed; planned 20766, never emitted 0, unresolved emitted 0. 
- ingest/clickhouse_write: 19998 completed physical rows; 0 completed failures; 9999/5 completed rows/s. Start-window latency cohort: 105 successes, 0 failures; total p50/p95/p99 ns 8046720/11008798/12437153.
  - prepare p50/p95/p99 ns 2208343/2907935/3452146 (n=105).
  - append p50/p95/p99 ns 761594/2326968/2857238 (n=105).
  - send p50/p95/p99 ns 4808016/6423463/7161322 (n=105).
- Last successful ClickHouse write finished -5070424 ns relative to end of offering; positive values are post-pulse writes.
- services query: p95 36597000 ns; failures 0/10 (0).
- traces query: p95 37296000 ns; failures 0/10 (0).
- detail query: p95 26746000 ns; failures 0/10 (0).
- incidents query: p95 212315000 ns; failures 0/10 (0).
- detail scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- incidents scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- services scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- traces scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- Visibility probes, separate from paced queries: 22 HTTP requests, 11 HTTP failures; 11 sampled batches, 0 censored.
- ClickHouse server QueryFinish (all case): 109 records, 20766 written rows, source-ms duration p50/p95/p99 6/9/11; selected ProfileEvents totals {'InsertedRows': 20766, 'InsertedBytes': 21239219, 'DiskWriteElapsedMicroseconds': 11635, 'NetworkReceiveElapsedMicroseconds': 170391, 'MergeTreeDataWriterRows': 20766, 'InsertedCompactParts': 109}.

### burst-2000-r1-streaming
Path streaming; status observed; planned 20766, never emitted 0, unresolved emitted 0. 
- producer/kafka_produce: 19998 completed physical rows; 0 completed failures; 9999/5 completed rows/s. Start-window latency cohort: 105 successes, 0 failures; total p50/p95/p99 ns 2995913/4761609/20200620.
- worker/clickhouse_write: 20574 completed physical rows; 0 completed failures; 10287/5 completed rows/s. Start-window latency cohort: 107 successes, 0 failures; total p50/p95/p99 ns 7442008/11975765/16130591.
  - prepare p50/p95/p99 ns 2190725/4031096/8400657 (n=107).
  - append p50/p95/p99 ns 455644/912114/1682215 (n=107).
  - send p50/p95/p99 ns 4725171/7765756/10052830 (n=107).
- worker/consumer_commit: 20544 completed physical rows; 0 completed failures; 10272/5 completed rows/s. Start-window latency cohort: 108 successes, 0 failures; total p50/p95/p99 ns 7921241/9753465/12200443.
- Last successful ClickHouse write finished -1278284 ns relative to end of offering; positive values are post-pulse writes.
- Kafka lag records: first 2, max sampled 2, last 0; post-offering drain {'status': 'observed_bounded', 'lower_bound_ns': 0, 'upper_bound_ns': 1977643000, 'first_zero_started_ns': 1790894406730683000, 'second_zero_finished_ns': 1790894407738473000, 'stable_end': 109}.
- services query: p95 89757000 ns; failures 0/10 (0).
- traces query: p95 52723000 ns; failures 0/10 (0).
- detail query: p95 131216000 ns; failures 0/10 (0).
- incidents query: p95 154982000 ns; failures 0/10 (0).
- detail scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- incidents scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- services scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- traces scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- Visibility probes, separate from paced queries: 22 HTTP requests, 11 HTTP failures; 11 sampled batches, 0 censored.
- ClickHouse server QueryFinish (all case): 109 records, 20766 written rows, source-ms duration p50/p95/p99 6/10/14; selected ProfileEvents totals {'InsertedRows': 20766, 'InsertedBytes': 21239219, 'DiskWriteElapsedMicroseconds': 11267, 'NetworkReceiveElapsedMicroseconds': 113014, 'MergeTreeDataWriterRows': 20766, 'InsertedCompactParts': 109}.

### burst-8000-r1-direct
Path direct; status observed; planned 80766, never emitted 3840, unresolved emitted 0. 
- ingest/clickhouse_write: 60864 completed physical rows; 0 completed failures; 30432/5 completed rows/s. Start-window latency cohort: 319 successes, 0 failures; total p50/p95/p99 ns 14062713/83849354/170026401.
  - prepare p50/p95/p99 ns 3205736/69201334/75547909 (n=319).
  - append p50/p95/p99 ns 1105857/4591704/21925765 (n=319).
  - send p50/p95/p99 ns 7998268/68120920/94556388 (n=319).
- Last successful ClickHouse write finished 2329826366 ns relative to end of offering; positive values are post-pulse writes.
- services query: p95 202519000 ns; failures 0/10 (0).
- traces query: p95 289024000 ns; failures 0/10 (0).
- detail query: p95 248781000 ns; failures 0/10 (0).
- incidents query: p95 856956000 ns; failures 0/10 (0).
- detail scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- incidents scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- services scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- traces scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- Visibility probes, separate from paced queries: 89 HTTP requests, 51 HTTP failures; 38 sampled batches, 0 censored.
- ClickHouse server QueryFinish (all case): 401 records, 76926 written rows, source-ms duration p50/p95/p99 11/75/99; selected ProfileEvents totals {'InsertedRows': 76926, 'InsertedBytes': 78714385, 'DiskWriteElapsedMicroseconds': 68300, 'NetworkReceiveElapsedMicroseconds': 1189122, 'MergeTreeDataWriterRows': 76926, 'InsertedCompactParts': 401}.

### burst-8000-r1-streaming
Path streaming; status observed; planned 80766, never emitted 384, unresolved emitted 0. 
- producer/kafka_produce: 79488 completed physical rows; 0 completed failures; 39744/5 completed rows/s. Start-window latency cohort: 414 successes, 0 failures; total p50/p95/p99 ns 3795001/20160738/46903752.
- worker/clickhouse_write: 39360 completed physical rows; 0 completed failures; 3936 completed rows/s. Start-window latency cohort: 205 successes, 0 failures; total p50/p95/p99 ns 12105569/66803264/78794632.
  - prepare p50/p95/p99 ns 3023332/50310672/58150438 (n=205).
  - append p50/p95/p99 ns 2029160/4445218/6599715 (n=205).
  - send p50/p95/p99 ns 6810988/15163433/56855923 (n=205).
- worker/consumer_commit: 39360 completed physical rows; 0 completed failures; 3936 completed rows/s. Start-window latency cohort: 205 successes, 0 failures; total p50/p95/p99 ns 8595540/16282972/34954256.
- Last successful ClickHouse write finished 8153526120 ns relative to end of offering; positive values are post-pulse writes.
- Kafka lag records: first 13, max sampled 203, last 0; post-offering drain {'status': 'observed_bounded', 'lower_bound_ns': 7740495000, 'upper_bound_ns': 9746051000, 'first_zero_started_ns': 1790894582638269000, 'second_zero_finished_ns': 1790894583644142000, 'stable_end': 419}.
- services query: p95 138870000 ns; failures 0/10 (0).
- traces query: p95 180188000 ns; failures 0/10 (0).
- detail query: p95 None ns; failures 10/10 (1).
- incidents query: p95 610105000 ns; failures 0/10 (0).
- detail scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- incidents scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- services scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- traces scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- Visibility probes, separate from paced queries: 169 HTTP requests, 127 HTTP failures; 42 sampled batches, 0 censored.
- ClickHouse server QueryFinish (all case): 419 records, 80382 written rows, source-ms duration p50/p95/p99 9/29/71; selected ProfileEvents totals {'InsertedRows': 80382, 'InsertedBytes': 82253113, 'DiskWriteElapsedMicroseconds': 59586, 'NetworkReceiveElapsedMicroseconds': 1288395, 'MergeTreeDataWriterRows': 80382, 'InsertedCompactParts': 419}.

### burst-32000-r1-direct
Path direct; status observed; planned 320766, never emitted 234942, unresolved emitted 0. 
- ingest/clickhouse_write: 69696 completed physical rows; 0 completed failures; 34848/5 completed rows/s. Start-window latency cohort: 363 successes, 0 failures; total p50/p95/p99 ns 12488328/81712329/105241341.
  - prepare p50/p95/p99 ns 3074272/67265451/74346266 (n=363).
  - append p50/p95/p99 ns 944618/3487564/37893427 (n=363).
  - send p50/p95/p99 ns 7279671/63458361/92645483 (n=363).
- Last successful ClickHouse write finished 1909997011 ns relative to end of offering; positive values are post-pulse writes.
- services query: p95 183051000 ns; failures 0/10 (0).
- traces query: p95 227959000 ns; failures 0/10 (0).
- detail query: p95 363145000 ns; failures 0/10 (0).
- incidents query: p95 682117000 ns; failures 0/10 (0).
- detail scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- incidents scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- services scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- traces scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- Visibility probes, separate from paced queries: 106 HTTP requests, 62 HTTP failures; 44 sampled batches, 0 censored.
- ClickHouse server QueryFinish (all case): 447 records, 85824 written rows, source-ms duration p50/p95/p99 10/71/80; selected ProfileEvents totals {'InsertedRows': 85824, 'InsertedBytes': 87852264, 'DiskWriteElapsedMicroseconds': 79090, 'NetworkReceiveElapsedMicroseconds': 1516387, 'MergeTreeDataWriterRows': 85824, 'InsertedCompactParts': 447}.

### burst-32000-r1-streaming
Path streaming; status observed; planned 320766, never emitted 208062, unresolved emitted 0. 
- producer/kafka_produce: 96768 completed physical rows; 0 completed failures; 48384/5 completed rows/s. Start-window latency cohort: 504 successes, 0 failures; total p50/p95/p99 ns 4135424/39470726/51390071.
- worker/clickhouse_write: 38208 completed physical rows; 0 completed failures; 19104/5 completed rows/s. Start-window latency cohort: 199 successes, 0 failures; total p50/p95/p99 ns 11669843/65618982/112844669.
  - prepare p50/p95/p99 ns 3110651/54020126/62885173 (n=199).
  - append p50/p95/p99 ns 729338/2926977/5728969 (n=199).
  - send p50/p95/p99 ns 6893770/14984310/60810083 (n=199).
- worker/consumer_commit: 38400 completed physical rows; 0 completed failures; 3840 completed rows/s. Start-window latency cohort: 199 successes, 0 failures; total p50/p95/p99 ns 8964786/20207620/29854659.
- Last successful ClickHouse write finished 14173106382 ns relative to end of offering; positive values are post-pulse writes.
- Kafka lag records: first 22, max sampled 354, last 0; post-offering drain {'status': 'observed_bounded', 'lower_bound_ns': 13517244000, 'upper_bound_ns': 15522157000, 'first_zero_started_ns': 1790894767333632000, 'second_zero_finished_ns': 1790894768338422000, 'stable_end': 587}.
- services query: p95 136668000 ns; failures 0/10 (0).
- traces query: p95 103224000 ns; failures 0/10 (0).
- detail query: p95 None ns; failures 10/10 (1).
- incidents query: p95 538654000 ns; failures 0/10 (0).
- detail scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- incidents scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- services scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- traces scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- Visibility probes, separate from paced queries: 200 HTTP requests, 162 HTTP failures; 61 sampled batches, 23 censored.
- ClickHouse server QueryFinish (all case): 587 records, 112704 written rows, source-ms duration p50/p95/p99 8/21/64; selected ProfileEvents totals {'InsertedRows': 112704, 'InsertedBytes': 115379416, 'DiskWriteElapsedMicroseconds': 110870, 'NetworkReceiveElapsedMicroseconds': 1445925, 'MergeTreeDataWriterRows': 112704, 'InsertedCompactParts': 587}.

### slowdown-2000-r1-direct
Path direct; status observed; planned 260766, never emitted 32064, unresolved emitted 0. 
- ingest/clickhouse_write: 207966 completed physical rows; 5 completed failures; 34661/20 completed rows/s. Start-window latency cohort: 1083 successes, 5 failures; total p50/p95/p99 ns 10302230/474208598/1179550900.
  - prepare p50/p95/p99 ns 2702659/90045767/482631281 (n=1083).
  - append p50/p95/p99 ns 1308872/3587529/5408205 (n=1083).
  - send p50/p95/p99 ns 6076478/201014304/896335427 (n=1083).
- Last successful ClickHouse write finished -4406401 ns relative to end of offering; positive values are post-pulse writes.
- Controlled slowdown status completed; observed quota 250000000 then restored 2000000000 nanocpus.
- services query: p95 3000030000 ns; failures 10/105 (2/21).
- traces query: p95 3112835000 ns; failures 10/109 (10/109).
- detail query: p95 930118000 ns; failures 11/104 (11/104).
- incidents query: p95 1645187000 ns; failures 79/105 (79/105).
- detail scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 104, 'queue_dropped': 16, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 104, 'late_start': 0}.
- incidents scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 105, 'queue_dropped': 15, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 105, 'late_start': 0}.
- services scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 105, 'queue_dropped': 15, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 105, 'late_start': 0}.
- traces scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 109, 'queue_dropped': 11, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 109, 'late_start': 0}.
- Visibility probes, separate from paced queries: 451 HTTP requests, 350 HTTP failures; 107 sampled batches, 6 censored.
- ClickHouse server QueryFinish (all case): 1195 records, 229278 written rows, source-ms duration p50/p95/p99 8/301/981; selected ProfileEvents totals {'InsertedRows': 229278, 'InsertedBytes': 234737993, 'DiskWriteElapsedMicroseconds': 650941, 'NetworkReceiveElapsedMicroseconds': 8134413, 'MergeTreeDataWriterRows': 229278, 'InsertedCompactParts': 1195}.

### slowdown-2000-r1-streaming
Path streaming; status observed; planned 260766, never emitted 0, unresolved emitted 0. 
- producer/kafka_produce: 240030 completed physical rows; 0 completed failures; 8001/4 completed rows/s. Start-window latency cohort: 1251 successes, 0 failures; total p50/p95/p99 ns 2512952/4949834/12779284.
- worker/clickhouse_write: 240000 completed physical rows; 0 completed failures; 2000 completed rows/s. Start-window latency cohort: 1251 successes, 0 failures; total p50/p95/p99 ns 11320941/82910999/878350042.
  - prepare p50/p95/p99 ns 2916909/66741004/277215296 (n=1251).
  - append p50/p95/p99 ns 1897991/3703316/5487156 (n=1251).
  - send p50/p95/p99 ns 6238525/64342019/597308568 (n=1251).
- worker/consumer_commit: 240000 completed physical rows; 0 completed failures; 2000 completed rows/s. Start-window latency cohort: 1250 successes, 0 failures; total p50/p95/p99 ns 7500477/9617776/16981614.
- Last successful ClickHouse write finished 2172284 ns relative to end of offering; positive values are post-pulse writes.
- Kafka lag records: first 0, max sampled 271, last 0; post-offering drain {'status': 'observed_bounded', 'lower_bound_ns': 0, 'upper_bound_ns': 1679594000, 'first_zero_started_ns': 1790895175072407000, 'second_zero_finished_ns': 1790895176075870000, 'stable_end': 1359}.
- Controlled slowdown status completed; observed quota 250000000 then restored 2000000000 nanocpus.
- services query: p95 2776648000 ns; failures 16/114 (8/57).
- traces query: p95 2421438000 ns; failures 16/112 (1/7).
- detail query: p95 504489000 ns; failures 60/114 (10/19).
- incidents query: p95 3570112000 ns; failures 85/114 (85/114).
- detail scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 114, 'queue_dropped': 6, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 114, 'late_start': 0}.
- incidents scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 114, 'queue_dropped': 6, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 114, 'late_start': 0}.
- services scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 114, 'queue_dropped': 6, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 114, 'late_start': 0}.
- traces scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 112, 'queue_dropped': 8, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 112, 'late_start': 0}.
- Visibility probes, separate from paced queries: 527 HTTP requests, 436 HTTP failures; 125 sampled batches, 34 censored.
- ClickHouse server QueryFinish (all case): 1359 records, 260766 written rows, source-ms duration p50/p95/p99 9/71/600; selected ProfileEvents totals {'InsertedRows': 260766, 'InsertedBytes': 266984985, 'DiskWriteElapsedMicroseconds': 181438, 'NetworkReceiveElapsedMicroseconds': 6160764, 'MergeTreeDataWriterRows': 260766, 'InsertedCompactParts': 1359}.

### sustained-500-r2-streaming
Path streaming; status observed; planned 65766, never emitted 0, unresolved emitted 0. 
- producer/kafka_produce: 60006 completed physical rows; 0 completed failures; 10001/20 completed rows/s. Start-window latency cohort: 312 successes, 0 failures; total p50/p95/p99 ns 2532595/4743069/16910168.
- worker/clickhouse_write: 60006 completed physical rows; 0 completed failures; 10001/20 completed rows/s. Start-window latency cohort: 313 successes, 0 failures; total p50/p95/p99 ns 8194632/12169629/15107242.
  - prepare p50/p95/p99 ns 2231753/3367930/4195581 (n=313).
  - append p50/p95/p99 ns 1169486/2232441/2899424 (n=313).
  - send p50/p95/p99 ns 4835855/7605293/9537412 (n=313).
- worker/consumer_commit: 60006 completed physical rows; 0 completed failures; 10001/20 completed rows/s. Start-window latency cohort: 313 successes, 0 failures; total p50/p95/p99 ns 7333118/8756542/15007728.
- Last successful ClickHouse write finished -181261593 ns relative to end of offering; positive values are post-pulse writes.
- Kafka lag records: first 0, max sampled 1, last 0; post-offering drain {'status': 'observed_bounded', 'lower_bound_ns': 0, 'upper_bound_ns': 1164751000, 'first_zero_started_ns': 1790895400666796000, 'second_zero_finished_ns': 1790895401673817000, 'stable_end': 343}.
- services query: p95 94833000 ns; failures 0/120 (0).
- traces query: p95 38916000 ns; failures 0/120 (0).
- detail query: p95 60941000 ns; failures 6/120 (1/20).
- incidents query: p95 583261000 ns; failures 0/120 (0).
- detail scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- incidents scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- services scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- traces scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- Visibility probes, separate from paced queries: 61 HTTP requests, 30 HTTP failures; 31 sampled batches, 0 censored.
- ClickHouse server QueryFinish (all case): 343 records, 65766 written rows, source-ms duration p50/p95/p99 6/10/13; selected ProfileEvents totals {'InsertedRows': 65766, 'InsertedBytes': 67284629, 'DiskWriteElapsedMicroseconds': 34010, 'NetworkReceiveElapsedMicroseconds': 548578, 'MergeTreeDataWriterRows': 65766, 'InsertedCompactParts': 343}.

### sustained-500-r2-direct
Path direct; status observed; planned 65766, never emitted 0, unresolved emitted 0. 
- ingest/clickhouse_write: 60006 completed physical rows; 0 completed failures; 10001/20 completed rows/s. Start-window latency cohort: 313 successes, 0 failures; total p50/p95/p99 ns 7950491/12360111/24976411.
  - prepare p50/p95/p99 ns 2179505/3429659/4075271 (n=313).
  - append p50/p95/p99 ns 585588/2200471/2933054 (n=313).
  - send p50/p95/p99 ns 4793393/7532579/18832983 (n=313).
- Last successful ClickHouse write finished -191447746 ns relative to end of offering; positive values are post-pulse writes.
- services query: p95 48396000 ns; failures 0/120 (0).
- traces query: p95 35715000 ns; failures 0/120 (0).
- detail query: p95 61287000 ns; failures 0/120 (0).
- incidents query: p95 585543000 ns; failures 0/120 (0).
- detail scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- incidents scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- services scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- traces scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- Visibility probes, separate from paced queries: 60 HTTP requests, 29 HTTP failures; 31 sampled batches, 0 censored.
- ClickHouse server QueryFinish (all case): 343 records, 65766 written rows, source-ms duration p50/p95/p99 6/10/22; selected ProfileEvents totals {'InsertedRows': 65766, 'InsertedBytes': 67284629, 'DiskWriteElapsedMicroseconds': 34070, 'NetworkReceiveElapsedMicroseconds': 496131, 'MergeTreeDataWriterRows': 65766, 'InsertedCompactParts': 343}.

### sustained-2000-r2-streaming
Path streaming; status observed; planned 260766, never emitted 0, unresolved emitted 0. 
- producer/kafka_produce: 240030 completed physical rows; 0 completed failures; 8001/4 completed rows/s. Start-window latency cohort: 1250 successes, 0 failures; total p50/p95/p99 ns 2400077/4915507/11439719.
- worker/clickhouse_write: 240030 completed physical rows; 0 completed failures; 8001/4 completed rows/s. Start-window latency cohort: 1251 successes, 0 failures; total p50/p95/p99 ns 9477225/64602122/86306626.
  - prepare p50/p95/p99 ns 2482700/44855313/68981488 (n=1251).
  - append p50/p95/p99 ns 1619747/3012135/4256806 (n=1251).
  - send p50/p95/p99 ns 5290738/10754547/62785350 (n=1251).
- worker/consumer_commit: 240000 completed physical rows; 0 completed failures; 2000 completed rows/s. Start-window latency cohort: 1251 successes, 0 failures; total p50/p95/p99 ns 7240361/8845897/20272367.
- Last successful ClickHouse write finished -4261061 ns relative to end of offering; positive values are post-pulse writes.
- Kafka lag records: first 0, max sampled 11, last 0; post-offering drain {'status': 'observed_bounded', 'lower_bound_ns': 0, 'upper_bound_ns': 1956308000, 'first_zero_started_ns': 1790895806849650000, 'second_zero_finished_ns': 1790895807853769000, 'stable_end': 1359}.
- services query: p95 357091000 ns; failures 0/120 (0).
- traces query: p95 398762000 ns; failures 1/120 (1/120).
- detail query: p95 379768000 ns; failures 25/120 (5/24).
- incidents query: p95 1092879000 ns; failures 81/120 (27/40).
- detail scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- incidents scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- services scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- traces scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- Visibility probes, separate from paced queries: 247 HTTP requests, 122 HTTP failures; 125 sampled batches, 0 censored.
- ClickHouse server QueryFinish (all case): 1359 records, 260766 written rows, source-ms duration p50/p95/p99 7/33/75; selected ProfileEvents totals {'InsertedRows': 260766, 'InsertedBytes': 266984985, 'DiskWriteElapsedMicroseconds': 157531, 'NetworkReceiveElapsedMicroseconds': 3633922, 'MergeTreeDataWriterRows': 260766, 'InsertedCompactParts': 1359}.

### sustained-2000-r2-direct
Path direct; status observed; planned 260766, never emitted 0, unresolved emitted 0. 
- ingest/clickhouse_write: 240000 completed physical rows; 0 completed failures; 2000 completed rows/s. Start-window latency cohort: 1251 successes, 0 failures; total p50/p95/p99 ns 8480330/18531283/67953716.
  - prepare p50/p95/p99 ns 2246506/5724960/43653343 (n=1251).
  - append p50/p95/p99 ns 1044467/2794999/4933351 (n=1251).
  - send p50/p95/p99 ns 4910856/8470181/32704070 (n=1251).
- Last successful ClickHouse write finished 2998821 ns relative to end of offering; positive values are post-pulse writes.
- services query: p95 216066000 ns; failures 0/120 (0).
- traces query: p95 165587000 ns; failures 1/120 (1/120).
- detail query: p95 233215000 ns; failures 0/120 (0).
- incidents query: p95 706583000 ns; failures 81/120 (27/40).
- detail scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- incidents scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- services scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- traces scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- Visibility probes, separate from paced queries: 230 HTTP requests, 105 HTTP failures; 125 sampled batches, 0 censored.
- ClickHouse server QueryFinish (all case): 1359 records, 260766 written rows, source-ms duration p50/p95/p99 7/12/41; selected ProfileEvents totals {'InsertedRows': 260766, 'InsertedBytes': 266984985, 'DiskWriteElapsedMicroseconds': 190148, 'NetworkReceiveElapsedMicroseconds': 2602390, 'MergeTreeDataWriterRows': 260766, 'InsertedCompactParts': 1359}.

### sustained-8000-r2-streaming
Path streaming; status observed; planned 1040766, never emitted 0, unresolved emitted 397950. 
- producer/kafka_produce: 959616 completed physical rows; 0 completed failures; 39984/5 completed rows/s. Start-window latency cohort: 4998 successes, 0 failures; total p50/p95/p99 ns 2864719/9451021/42761069.
- worker/clickhouse_write: 320640 completed physical rows; 0 completed failures; 2672 completed rows/s. Start-window latency cohort: 1670 successes, 0 failures; total p50/p95/p99 ns 33530963/81034742/157447383.
  - prepare p50/p95/p99 ns 4717298/62680713/71172549 (n=1670).
  - append p50/p95/p99 ns 3019827/5141017/8271258 (n=1670).
  - send p50/p95/p99 ns 9708577/57792296/92922561 (n=1670).
- worker/consumer_commit: 320640 completed physical rows; 0 completed failures; 2672 completed rows/s. Start-window latency cohort: 1670 successes, 0 failures; total p50/p95/p99 ns 8025681/10844838/22027638.
- Last successful ClickHouse write finished 114094589035 ns relative to end of offering; positive values are post-pulse writes.
- Kafka lag records: first 5, max sampled 3484, last 0; post-offering drain {'status': 'observed_bounded', 'lower_bound_ns': 114088556000, 'upper_bound_ns': 116096748000, 'first_zero_started_ns': 1790896325674000000, 'second_zero_finished_ns': 1790896326681992000, 'stable_end': 5421}.
- Separate supplemental drain: complete=True; original-deadline missing remains 397950 ACKed spans.
- services query: p95 460039000 ns; failures 10/120 (1/12).
- traces query: p95 721541000 ns; failures 7/120 (7/120).
- detail query: p95 None ns; failures 120/120 (1).
- incidents query: p95 3096675000 ns; failures 104/120 (13/15).
- detail scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- incidents scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- services scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- traces scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 120, 'late_start': 0}.
- Visibility probes, separate from paced queries: 923 HTTP requests, 923 HTTP failures; 500 sampled batches, 500 censored.
- ClickHouse server QueryFinish (all case): 5421 records, 1040766 written rows, source-ms duration p50/p95/p99 9/52/73; selected ProfileEvents totals {'InsertedRows': 1040766, 'InsertedBytes': 1066226943, 'DiskWriteElapsedMicroseconds': 860914, 'NetworkReceiveElapsedMicroseconds': 17206847, 'MergeTreeDataWriterRows': 1040766, 'InsertedCompactParts': 5421}.
- Supplemental FINAL unique stored: 1040766; original snapshot stored 642816.

### sustained-8000-r2-direct
Path direct; status observed; planned 1040766, never emitted 540030, unresolved emitted 0. 
- ingest/clickhouse_write: 411264 completed physical rows; 0 completed failures; 17136/5 completed rows/s. Start-window latency cohort: 2142 successes, 0 failures; total p50/p95/p99 ns 82832846/179914723/190460095.
  - prepare p50/p95/p99 ns 70458663/81916331/86269645 (n=2142).
  - append p50/p95/p99 ns 1109789/4503646/7107251 (n=2142).
  - send p50/p95/p99 ns 10367684/99571958/107252659 (n=2142).
- Last successful ClickHouse write finished 5619170560 ns relative to end of offering; positive values are post-pulse writes.
- services query: p95 912039000 ns; failures 44/118 (22/59).
- traces query: p95 1570469000 ns; failures 41/119 (41/119).
- detail query: p95 2017736000 ns; failures 55/118 (55/118).
- incidents query: p95 1219719000 ns; failures 113/118 (113/118).
- detail scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 118, 'late_start': 2}.
- incidents scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 118, 'late_start': 2}.
- services scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 119, 'queue_dropped': 1, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 118, 'late_start': 1}.
- traces scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 120, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 119, 'late_start': 1}.
- Visibility probes, separate from paced queries: 863 HTTP requests, 642 HTTP failures; 221 sampled batches, 0 censored.
- ClickHouse server QueryFinish (all case): 2608 records, 500736 written rows, source-ms duration p50/p95/p99 17/106/175; selected ProfileEvents totals {'InsertedRows': 500736, 'InsertedBytes': 512895644, 'DiskWriteElapsedMicroseconds': 2369590, 'NetworkReceiveElapsedMicroseconds': 12667611, 'MergeTreeDataWriterRows': 500736, 'InsertedCompactParts': 2608}.

### burst-2000-r2-streaming
Path streaming; status observed; planned 20766, never emitted 0, unresolved emitted 0. 
- producer/kafka_produce: 19998 completed physical rows; 0 completed failures; 9999/5 completed rows/s. Start-window latency cohort: 105 successes, 0 failures; total p50/p95/p99 ns 2796138/4938202/11236724.
- worker/clickhouse_write: 20352 completed physical rows; 0 completed failures; 10176/5 completed rows/s. Start-window latency cohort: 107 successes, 0 failures; total p50/p95/p99 ns 7181458/9697393/10745456.
  - prepare p50/p95/p99 ns 2064315/2662067/2839262 (n=107).
  - append p50/p95/p99 ns 482551/1788983/2597915 (n=107).
  - send p50/p95/p99 ns 4340013/6048273/7620802 (n=107).
- worker/consumer_commit: 20352 completed physical rows; 0 completed failures; 10176/5 completed rows/s. Start-window latency cohort: 106 successes, 0 failures; total p50/p95/p99 ns 7602395/8559115/12106117.
- Last successful ClickHouse write finished 2448313 ns relative to end of offering; positive values are post-pulse writes.
- Kafka lag records: first 3, max sampled 3, last 0; post-offering drain {'status': 'observed_bounded', 'lower_bound_ns': 0, 'upper_bound_ns': 1025207000, 'first_zero_started_ns': 1790896593497738000, 'second_zero_finished_ns': 1790896594502729000, 'stable_end': 109}.
- services query: p95 42479000 ns; failures 0/10 (0).
- traces query: p95 39610000 ns; failures 0/10 (0).
- detail query: p95 45572000 ns; failures 2/10 (1/5).
- incidents query: p95 149278000 ns; failures 0/10 (0).
- detail scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- incidents scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- services scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- traces scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- Visibility probes, separate from paced queries: 22 HTTP requests, 11 HTTP failures; 11 sampled batches, 0 censored.
- ClickHouse server QueryFinish (all case): 109 records, 20766 written rows, source-ms duration p50/p95/p99 5/8/9; selected ProfileEvents totals {'InsertedRows': 20766, 'InsertedBytes': 21239219, 'DiskWriteElapsedMicroseconds': 9144, 'NetworkReceiveElapsedMicroseconds': 133993, 'MergeTreeDataWriterRows': 20766, 'InsertedCompactParts': 109}.

### burst-2000-r2-direct
Path direct; status observed; planned 20766, never emitted 0, unresolved emitted 0. 
- ingest/clickhouse_write: 19998 completed physical rows; 0 completed failures; 9999/5 completed rows/s. Start-window latency cohort: 105 successes, 0 failures; total p50/p95/p99 ns 8167790/12623830/14710555.
  - prepare p50/p95/p99 ns 2174156/3308496/4234238 (n=105).
  - append p50/p95/p99 ns 1182628/2601956/3864793 (n=105).
  - send p50/p95/p99 ns 4655398/7041145/7794327 (n=105).
- Last successful ClickHouse write finished -2215825 ns relative to end of offering; positive values are post-pulse writes.
- services query: p95 39014000 ns; failures 0/10 (0).
- traces query: p95 35575000 ns; failures 0/10 (0).
- detail query: p95 34240000 ns; failures 0/10 (0).
- incidents query: p95 209354000 ns; failures 0/10 (0).
- detail scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- incidents scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- services scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- traces scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- Visibility probes, separate from paced queries: 22 HTTP requests, 11 HTTP failures; 11 sampled batches, 0 censored.
- ClickHouse server QueryFinish (all case): 109 records, 20766 written rows, source-ms duration p50/p95/p99 6/10/12; selected ProfileEvents totals {'InsertedRows': 20766, 'InsertedBytes': 21239219, 'DiskWriteElapsedMicroseconds': 10976, 'NetworkReceiveElapsedMicroseconds': 192912, 'MergeTreeDataWriterRows': 20766, 'InsertedCompactParts': 109}.

### burst-8000-r2-streaming
Path streaming; status observed; planned 80766, never emitted 0, unresolved emitted 0. 
- producer/kafka_produce: 79872 completed physical rows; 0 completed failures; 39936/5 completed rows/s. Start-window latency cohort: 417 successes, 0 failures; total p50/p95/p99 ns 2913663/17284897/35972301.
- worker/clickhouse_write: 49344 completed physical rows; 0 completed failures; 24672/5 completed rows/s. Start-window latency cohort: 256 successes, 0 failures; total p50/p95/p99 ns 9830082/51360722/74711784.
  - prepare p50/p95/p99 ns 2548740/20138132/61118076 (n=256).
  - append p50/p95/p99 ns 1586582/3515071/4398923 (n=256).
  - send p50/p95/p99 ns 5611457/12903165/59513414 (n=256).
- worker/consumer_commit: 49152 completed physical rows; 0 completed failures; 24576/5 completed rows/s. Start-window latency cohort: 257 successes, 0 failures; total p50/p95/p99 ns 7911804/20616505/40542735.
- Last successful ClickHouse write finished 6343506122 ns relative to end of offering; positive values are post-pulse writes.
- Kafka lag records: first 4, max sampled 163, last 0; post-offering drain {'status': 'observed_bounded', 'lower_bound_ns': 6035492000, 'upper_bound_ns': 8041638000, 'first_zero_started_ns': 1790896764150114000, 'second_zero_finished_ns': 1790896765156130000, 'stable_end': 421}.
- services query: p95 291268000 ns; failures 0/10 (0).
- traces query: p95 137210000 ns; failures 0/10 (0).
- detail query: p95 None ns; failures 10/10 (1).
- incidents query: p95 790383000 ns; failures 0/10 (0).
- detail scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- incidents scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- services scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- traces scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- Visibility probes, separate from paced queries: 156 HTTP requests, 114 HTTP failures; 42 sampled batches, 0 censored.
- ClickHouse server QueryFinish (all case): 421 records, 80766 written rows, source-ms duration p50/p95/p99 8/25/70; selected ProfileEvents totals {'InsertedRows': 80766, 'InsertedBytes': 82646177, 'DiskWriteElapsedMicroseconds': 57352, 'NetworkReceiveElapsedMicroseconds': 1112391, 'MergeTreeDataWriterRows': 80766, 'InsertedCompactParts': 421}.

### burst-8000-r2-direct
Path direct; status observed; planned 80766, never emitted 0, unresolved emitted 0. 
- ingest/clickhouse_write: 77952 completed physical rows; 0 completed failures; 38976/5 completed rows/s. Start-window latency cohort: 408 successes, 0 failures; total p50/p95/p99 ns 7902255/61347910/81840957.
  - prepare p50/p95/p99 ns 2070277/46870148/71068717 (n=408).
  - append p50/p95/p99 ns 860301/2791370/3958905 (n=408).
  - send p50/p95/p99 ns 4652467/28612337/70448106 (n=408).
- Last successful ClickHouse write finished 304293102 ns relative to end of offering; positive values are post-pulse writes.
- services query: p95 143699000 ns; failures 0/10 (0).
- traces query: p95 157959000 ns; failures 0/10 (0).
- detail query: p95 210082000 ns; failures 0/10 (0).
- incidents query: p95 994702000 ns; failures 0/10 (0).
- detail scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- incidents scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- services scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- traces scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- Visibility probes, separate from paced queries: 81 HTTP requests, 39 HTTP failures; 42 sampled batches, 0 censored.
- ClickHouse server QueryFinish (all case): 421 records, 80766 written rows, source-ms duration p50/p95/p99 6/52/78; selected ProfileEvents totals {'InsertedRows': 80766, 'InsertedBytes': 82646177, 'DiskWriteElapsedMicroseconds': 49608, 'NetworkReceiveElapsedMicroseconds': 1091000, 'MergeTreeDataWriterRows': 80766, 'InsertedCompactParts': 421}.

### burst-32000-r2-streaming
Path streaming; status observed; planned 320766, never emitted 199614, unresolved emitted 0. 
- producer/kafka_produce: 105024 completed physical rows; 0 completed failures; 52512/5 completed rows/s. Start-window latency cohort: 547 successes, 0 failures; total p50/p95/p99 ns 3444254/35381132/49965256.
- worker/clickhouse_write: 40128 completed physical rows; 0 completed failures; 20064/5 completed rows/s. Start-window latency cohort: 210 successes, 0 failures; total p50/p95/p99 ns 11134487/67481336/77789743.
  - prepare p50/p95/p99 ns 3018300/52642273/62725753 (n=210).
  - append p50/p95/p99 ns 1594031/3718896/6096290 (n=210).
  - send p50/p95/p99 ns 6510091/13285896/48273336 (n=210).
- worker/consumer_commit: 40128 completed physical rows; 0 completed failures; 20064/5 completed rows/s. Start-window latency cohort: 209 successes, 0 failures; total p50/p95/p99 ns 8299909/32496513/62840200.
- Last successful ClickHouse write finished 14763169095 ns relative to end of offering; positive values are post-pulse writes.
- Kafka lag records: first 38, max sampled 381, last 0; post-offering drain {'status': 'observed_bounded', 'lower_bound_ns': 13837221000, 'upper_bound_ns': 15845720000, 'first_zero_started_ns': 1790896942475491000, 'second_zero_finished_ns': 1790896943483466000, 'stable_end': 631}.
- services query: p95 329939000 ns; failures 0/10 (0).
- traces query: p95 160225000 ns; failures 0/10 (0).
- detail query: p95 None ns; failures 10/10 (1).
- incidents query: p95 735958000 ns; failures 0/10 (0).
- detail scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- incidents scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- services scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- traces scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- Visibility probes, separate from paced queries: 188 HTTP requests, 156 HTTP failures; 59 sampled batches, 27 censored.
- ClickHouse server QueryFinish (all case): 631 records, 121152 written rows, source-ms duration p50/p95/p99 8/18/58; selected ProfileEvents totals {'InsertedRows': 121152, 'InsertedBytes': 124029512, 'DiskWriteElapsedMicroseconds': 86561, 'NetworkReceiveElapsedMicroseconds': 1648692, 'MergeTreeDataWriterRows': 121152, 'InsertedCompactParts': 631}.

### burst-32000-r2-direct
Path direct; status observed; planned 320766, never emitted 226686, unresolved emitted 0. 
- ingest/clickhouse_write: 77952 completed physical rows; 0 completed failures; 38976/5 completed rows/s. Start-window latency cohort: 406 successes, 0 failures; total p50/p95/p99 ns 11717190/81905505/88467500.
  - prepare p50/p95/p99 ns 2803503/69861132/73975559 (n=406).
  - append p50/p95/p99 ns 840292/3292544/14257495 (n=406).
  - send p50/p95/p99 ns 6835396/65263447/74062746 (n=406).
- Last successful ClickHouse write finished 1869536633 ns relative to end of offering; positive values are post-pulse writes.
- services query: p95 205495000 ns; failures 0/10 (0).
- traces query: p95 354237000 ns; failures 0/10 (0).
- detail query: p95 282808000 ns; failures 0/10 (0).
- incidents query: p95 1155441000 ns; failures 1/10 (1/10).
- detail scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- incidents scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- services scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- traces scheduled query outcomes: {'scheduled_measured': 10, 'issued_measured': 10, 'queue_dropped': 0, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 10, 'late_start': 0}.
- Visibility probes, separate from paced queries: 107 HTTP requests, 59 HTTP failures; 48 sampled batches, 0 censored.
- ClickHouse server QueryFinish (all case): 490 records, 94080 written rows, source-ms duration p50/p95/p99 9/74/81; selected ProfileEvents totals {'InsertedRows': 94080, 'InsertedBytes': 96303620, 'DiskWriteElapsedMicroseconds': 72182, 'NetworkReceiveElapsedMicroseconds': 1352226, 'MergeTreeDataWriterRows': 94080, 'InsertedCompactParts': 490}.

### slowdown-2000-r2-streaming
Path streaming; status observed; planned 260766, never emitted 0, unresolved emitted 0. 
- producer/kafka_produce: 239838 completed physical rows; 0 completed failures; 39973/20 completed rows/s. Start-window latency cohort: 1250 successes, 0 failures; total p50/p95/p99 ns 4210329/13101220/28247911.
- worker/clickhouse_write: 153216 completed physical rows; 0 completed failures; 6384/5 completed rows/s. Start-window latency cohort: 797 successes, 0 failures; total p50/p95/p99 ns 49007513/251047494/1555654457.
  - prepare p50/p95/p99 ns 17074332/73763259/404501986 (n=797).
  - append p50/p95/p99 ns 3313651/9388099/17126855 (n=797).
  - send p50/p95/p99 ns 13838149/177677619/1095766158 (n=797).
- worker/consumer_commit: 153216 completed physical rows; 0 completed failures; 6384/5 completed rows/s. Start-window latency cohort: 798 successes, 0 failures; total p50/p95/p99 ns 9555738/24065041/44294224.
- Last successful ClickHouse write finished 21547579022 ns relative to end of offering; positive values are post-pulse writes.
- Kafka lag records: first 2, max sampled 455, last 0; post-offering drain {'status': 'observed_bounded', 'lower_bound_ns': 20556332000, 'upper_bound_ns': 22563195000, 'first_zero_started_ns': 1790912476166665000, 'second_zero_finished_ns': 1790912477173452000, 'stable_end': 1359}.
- Controlled slowdown status completed; observed quota 250000000 then restored 2000000000 nanocpus.
- services query: p95 2290536000 ns; failures 38/110 (19/55).
- traces query: p95 1069133000 ns; failures 23/103 (23/103).
- detail query: p95 147033000 ns; failures 99/108 (11/12).
- incidents query: p95 3762837000 ns; failures 69/104 (69/104).
- detail scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 108, 'queue_dropped': 12, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 108, 'late_start': 0}.
- incidents scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 104, 'queue_dropped': 16, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 104, 'late_start': 0}.
- services scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 110, 'queue_dropped': 10, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 110, 'late_start': 0}.
- traces scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 103, 'queue_dropped': 17, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 103, 'late_start': 0}.
- Visibility probes, separate from paced queries: 852 HTTP requests, 831 HTTP failures; 125 sampled batches, 104 censored.
- ClickHouse server QueryFinish (all case): 1359 records, 260766 written rows, source-ms duration p50/p95/p99 16/115/504; selected ProfileEvents totals {'InsertedRows': 260766, 'InsertedBytes': 266984985, 'DiskWriteElapsedMicroseconds': 968151, 'NetworkReceiveElapsedMicroseconds': 9192326, 'MergeTreeDataWriterRows': 260766, 'InsertedCompactParts': 1359}.

### slowdown-2000-r2-direct
Path direct; status observed; planned 260766, never emitted 53760, unresolved emitted 0. 
- ingest/clickhouse_write: 171072 completed physical rows; 26 completed failures; 7128/5 completed rows/s. Start-window latency cohort: 891 successes, 26 failures; total p50/p95/p99 ns 171324466/413153880/1366195614.
  - prepare p50/p95/p99 ns 69133088/175824351/368624182 (n=891).
  - append p50/p95/p99 ns 2153226/7796615/11225865 (n=891).
  - send p50/p95/p99 ns 97821281/294925423/999822749 (n=891).
- Last successful ClickHouse write finished 2696525545 ns relative to end of offering; positive values are post-pulse writes.
- Controlled slowdown status completed; observed quota 250000000 then restored 2000000000 nanocpus.
- services query: p95 1301395000 ns; failures 32/98 (16/49).
- traces query: p95 1654442000 ns; failures 41/94 (41/94).
- detail query: p95 2311604000 ns; failures 36/92 (9/23).
- incidents query: p95 2369055000 ns; failures 78/107 (78/107).
- detail scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 92, 'queue_dropped': 28, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 92, 'late_start': 0}.
- incidents scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 107, 'queue_dropped': 13, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 107, 'late_start': 0}.
- services scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 98, 'queue_dropped': 22, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 98, 'late_start': 0}.
- traces scheduled query outcomes: {'scheduled_measured': 120, 'issued_measured': 94, 'queue_dropped': 26, 'skipped_no_acknowledged_trace': 0, 'started_in_window': 94, 'late_start': 0}.
- Visibility probes, separate from paced queries: 710 HTTP requests, 623 HTTP failures; 97 sampled batches, 10 censored.
- ClickHouse server QueryFinish (all case): 1086 records, 208350 written rows, source-ms duration p50/p95/p99 100/312/1183; selected ProfileEvents totals {'InsertedRows': 208350, 'InsertedBytes': 213305541, 'DiskWriteElapsedMicroseconds': 2291301, 'NetworkReceiveElapsedMicroseconds': 13133532, 'MergeTreeDataWriterRows': 208350, 'InsertedCompactParts': 1086}.

