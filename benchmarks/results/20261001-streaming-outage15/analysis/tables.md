# Raw benchmark analysis

All rows come from retained observations; unknowns do not pass targets.

| Stage | Validity | Offered spans/s | ACK within window spans/s | ACK p95 / p99 ms | Retries | Terminal batches |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| outage | raw_evidence_available | 497.00 | 497.00 | 32.79 / 45.76 | 0 | 0 |

Latency percentiles use sorted index floor(n×p/100), capped at n−1. Successful and failed API distributions, sample counts, censored visibility and resource observations remain in analysis.json. A small p99 sample is not a robust capacity claim.
