"""Bounded supplemental identity observations; never rewrites a benchmark result."""
import argparse
import base64
import json
import time
import urllib.request
from pathlib import Path

import environment

MAX_BYTES = 256 << 20


def account(expected, rows):
    seen = {}
    unknown = 0
    duplicates = 0
    for row in rows:
        trace, span, service = row
        try:
            number = int(span, 16)
        except ValueError:
            number = 0
        if trace not in expected or number not in range(1, 7) or service != expected[trace]['services'][(number-1)//2]:
            unknown += 1
            continue
        mask = 1 << (number-1)
        if seen.get(trace, 0) & mask:
            duplicates += 1
        seen[trace] = seen.get(trace, 0) | mask
    counts = dict(planned_missing=0, emitted_missing=0, acked_missing=0,
                  expected_stored=0, unknown=unknown, duplicate_logical_identities=duplicates)
    for trace, record in expected.items():
        stored = bin(seen.get(trace, 0)).count('1')
        counts['expected_stored'] += stored
        counts['planned_missing'] += 6-stored
        for flag in ('emitted', 'acked'):
            if record[flag]:
                counts[flag+'_missing'] += 6-stored
    return counts


def observe(directory, timeout=120):
    summary = json.loads((directory/'harness/summary.json').read_text())
    config = summary['config']
    ledger = directory/'harness/reconciliation.jsonl'
    if ledger.stat().st_size > MAX_BYTES:
        raise ValueError('ledger exceeds bound')
    expected = {}
    for line in ledger.open():
        row = json.loads(line)
        if row['kind'] == 'expected_trace':
            row['services'] = config['services']
            expected[row['trace_id']] = row
    if len(expected)*6 != summary['reconciliation']['planned_spans']:
        raise ValueError('incomplete original identity ledger')
    # Parameter binding prevents identifiers from becoming executable SQL.
    from urllib.parse import urlencode
    params = urlencode({'database': 'incidentlens', 'param_ns': config['namespace']})
    sql = 'SELECT trace_id, span_id, service_name FROM spans FINAL PREWHERE service_namespace={ns:String} FORMAT TabSeparated'
    request = urllib.request.Request('http://127.0.0.1:18123/?'+params, data=sql.encode())
    request.add_header('Authorization', 'Basic '+base64.b64encode(b'query:local-query').decode())
    started = time.monotonic()
    record = {'started_at': environment.utc(), 'bound_seconds': timeout,
              'original_summary': str(directory/'harness/summary.json'),
              'meaning': 'supplemental post-run observation; original deadline result remains unchanged', 'observations': []}
    output = directory/'supplemental-drain.json'
    if output.exists():
        raise FileExistsError(output)
    while True:
        observation = {'at': environment.utc(), 'elapsed_seconds': time.monotonic()-started}
        try:
            with urllib.request.urlopen(request, timeout=min(6, max(.1, timeout-(time.monotonic()-started)))) as response:
                raw = response.read(MAX_BYTES+1)
            if len(raw) > MAX_BYTES:
                raise ValueError('query result exceeds bound')
            observation.update(account(expected, [line.split('\t') for line in raw.decode().splitlines()]))
            observation['snapshot_verified'] = True
        except Exception as exc:
            observation.update(error=repr(exc), snapshot_verified=False)
        record['observations'].append(observation)
        complete = observation.get('snapshot_verified') and observation['emitted_missing'] == 0 and observation['unknown'] == 0 and observation['duplicate_logical_identities'] == 0
        record.update(complete=bool(complete), ended_at=environment.utc())
        output.write_text(json.dumps(record, indent=2)+'\n')
        if complete or time.monotonic()-started >= timeout:
            return record
        time.sleep(min(5, max(0, timeout-(time.monotonic()-started))))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    print(json.dumps(observe(args.directory), indent=2))
