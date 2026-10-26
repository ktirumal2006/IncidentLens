# Retained Phase 5 benchmark evidence

`phase5-checkpoint-20261026/` contains compressed, byte-preserving copies of the
previously uncommitted Phase 5 campaign directories. Failed and interrupted
attempts are included. Compact campaign records and exact-analysis summaries are
also tracked at their original paths under `benchmarks/results/`.

The manifest records archive hashes, compressed/raw sizes and file counts. From
the repository root, verify all archives before extracting a selected campaign:

```sh
shasum -a 256 -c benchmarks/archives/phase5-checkpoint-20261026/SHA256SUMS
tar -xzf benchmarks/archives/phase5-checkpoint-20261026/20261011-candidate3-query3-probe.tar.gz
```

Extraction restores the original `benchmarks/results/<campaign>/...` paths used
by the reports and analyzers. Extract into a clean checkout to avoid overwriting
locally changed observations. Raw results are ignored by Git because the archive
retains them; original files in the working repository were not removed.

These are synthetic local benchmarks. Their configuration includes the documented
local-only ClickHouse fixture passwords and host paths. They are not deployment
credentials or production telemetry. No successful promotion is implied by
archiving a run; consult its saved status, exact gates and verification report.
