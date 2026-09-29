# Raw benchmark analysis

All rows come from retained observations; unknowns do not pass targets.

| Stage | Validity | Offered spans/s | ACK within window spans/s | ACK p95 / p99 ms | Retries | Terminal batches |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| mixed | raw_evidence_available | 498.45 | 498.45 | 124.10 / 336.41 | 0 | 0 |
| outage | raw_evidence_available | 497.00 | 497.00 | 18674.30 / 20469.94 | 28 | 0 |
| ramp-2000 | raw_evidence_available | 1996.00 | 1996.00 | 202.75 / 447.07 | 0 | 0 |
| ramp-500 | raw_evidence_available | 495.73 | 495.73 | 44.21 / 50.45 | 0 | 0 |
| ramp-8000 | raw_evidence_available | 7997.07 | 4074.67 | 998.48 / 1104.07 | 0 | 0 |
| steady | raw_evidence_available | 498.60 | 498.60 | 54.56 / 119.90 | 0 | 0 |
| steady | raw_evidence_available | 498.60 | 498.60 | 78.24 / 136.51 | 0 | 0 |
| steady | raw_evidence_available | 498.60 | 498.60 | 43.31 / 108.98 | 0 | 0 |

Latency percentiles use sorted index floor(n×p/100), capped at n−1. Successful and failed API distributions, sample counts, censored visibility and resource observations remain in analysis.json. A small p99 sample is not a robust capacity claim.
