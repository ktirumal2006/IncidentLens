# Raw benchmark analysis

All rows come from retained observations; unknowns do not pass targets.

| Stage | Validity | Offered spans/s | ACK within window spans/s | ACK p95 / p99 ms | Retries | Terminal batches |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| mixed | raw_evidence_available | 498.45 | 498.45 | 35.81 / 51.82 | 0 | 0 |
| steady | raw_evidence_available | 498.60 | 498.60 | 27.46 / 30.00 | 0 | 0 |
| steady | incomplete | 161.60 | 161.60 | 26.41 / 28.85 | 0 | 0 |

Latency percentiles use sorted index floor(n×p/100), capped at n−1. Successful and failed API distributions, sample counts, censored visibility and resource observations remain in analysis.json. A small p99 sample is not a robust capacity claim.

## steady evidence gaps

- generator resource sample: missing/failed ps observation
- reconciliation snapshot incomplete or Collector settling unverified
