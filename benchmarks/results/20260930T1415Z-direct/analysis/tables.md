# Raw benchmark analysis

All rows come from retained observations; unknowns do not pass targets.

| Stage | Validity | Offered spans/s | ACK within window spans/s | ACK p95 / p99 ms | Retries | Terminal batches |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| mixed | raw_evidence_available | 498.45 | 498.45 | 161.22 / 213.65 | 0 | 0 |
| outage | raw_evidence_available | 497.00 | 497.00 | 18474.57 / 20372.47 | 27 | 0 |
| ramp-2000 | raw_evidence_available | 1996.00 | 1996.00 | 38.38 / 408.33 | 0 | 0 |
| ramp-500 | raw_evidence_available | 495.73 | 495.73 | 29.38 / 30.69 | 0 | 0 |
| ramp-8000 | raw_evidence_available | 7997.07 | 4757.33 | 898.86 / 1101.13 | 0 | 0 |
| steady | raw_evidence_available | 498.60 | 498.60 | 41.07 / 63.51 | 0 | 0 |
| steady | raw_evidence_available | 498.60 | 498.60 | 51.80 / 75.55 | 0 | 0 |
| steady | raw_evidence_available | 498.60 | 498.60 | 28.92 / 34.17 | 0 | 0 |

Latency percentiles use sorted index floor(n×p/100), capped at n−1. Successful and failed API distributions, sample counts, censored visibility and resource observations remain in analysis.json. A small p99 sample is not a robust capacity claim.
