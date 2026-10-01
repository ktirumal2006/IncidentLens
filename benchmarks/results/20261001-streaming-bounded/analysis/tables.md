# Raw benchmark analysis

All rows come from retained observations; unknowns do not pass targets.

| Stage | Validity | Offered spans/s | ACK within window spans/s | ACK p95 / p99 ms | Retries | Terminal batches |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| mixed | raw_evidence_available | 498.45 | 498.45 | 28.29 / 37.51 | 0 | 0 |
| outage | incomplete | 497.00 | 497.00 | 48.48 / 89.13 | 0 | 0 |
| ramp-2000 | raw_evidence_available | 1996.00 | 1996.00 | 67.92 / 101.40 | 0 | 0 |
| ramp-500 | raw_evidence_available | 495.73 | 495.73 | 35.63 / 54.54 | 0 | 0 |
| ramp-8000 | raw_evidence_available | 7997.07 | 7903.20 | 387.33 / 578.27 | 0 | 0 |
| steady | raw_evidence_available | 498.60 | 498.60 | 27.29 / 34.21 | 0 | 0 |
| steady | raw_evidence_available | 498.60 | 498.60 | 33.02 / 44.63 | 0 | 0 |
| steady | raw_evidence_available | 498.60 | 498.60 | 34.30 / 58.70 | 0 | 0 |

Latency percentiles use sorted index floor(n×p/100), capped at n−1. Successful and failed API distributions, sample counts, censored visibility and resource observations remain in analysis.json. A small p99 sample is not a robust capacity claim.

## outage evidence gaps

- declared storage outage did not complete: not_started
