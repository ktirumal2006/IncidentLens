#!/usr/bin/env python3
"""Follow one supported upstream Demo request through query search and detail.

Run with the product and external Demo Compose stacks up, from anywhere:
    python3 tests/e2e/demo_trace.py

Only the Python standard library is used. Exit status is nonzero unless a fresh
three-service trace with linked spans is visible through the public API.
"""

import json
import os
import secrets
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

API = os.environ.get("INCIDENTLENS_API_URL", "http://127.0.0.1:18081").rstrip("/")
DEMO = os.environ.get("INCIDENTLENS_DEMO_URL", "http://127.0.0.1:8080").rstrip("/")
SERVICES = {"frontend", "recommendationservice", "productcatalogservice"}


def stamp(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def request_json(url, headers=None):
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=15) as response:
        return response.status, json.load(response)


def api(path, params):
    url = f"{API}{path}?{urllib.parse.urlencode(params)}"
    status, result = request_json(url)
    if status != 200:
        raise AssertionError(f"{url} returned HTTP {status}")
    return result


def matching_trace(detail, trace_id, injected_parent_id):
    spans = detail.get("spans", [])
    services = {span.get("service_name") for span in spans}
    if services != SERVICES or detail.get("truncated"):
        return False
    ids = {span["span_id"] for span in spans}
    # The supported route has frontend -> recommendation -> catalog links.
    # The injected external parent is expected to be absent from storage.
    by_id = {span["span_id"]: span for span in spans}
    links = [
        (span, by_id[span["parent_span_id"]])
        for span in spans
        if span.get("parent_span_id") in ids
    ]
    edges = {(parent["service_name"], child["service_name"]) for child, parent in links}
    if not {("frontend", "recommendationservice"),
            ("recommendationservice", "productcatalogservice")} <= edges:
        return False
    missing = set(detail.get("missing_parent_ids", []))
    expected_missing = {injected_parent_id}
    if missing != expected_missing or detail.get("has_cycles") or detail.get("has_source_drops"):
        return False
    if detail.get("trace_id") != trace_id:
        return False
    if any(not isinstance(span.get("duration_ns"), str) or span.get("status_code") == 2 for span in spans):
        return False
    return True


def main():
    begin = datetime.now(timezone.utc) - timedelta(seconds=2)
    trace_id = secrets.token_hex(16)
    parent_id = secrets.token_hex(8)
    route = "/api/recommendations?productIds=OLJCESPC7Z&productIds=OLJCESPC7Z"
    demo_url = DEMO + route
    status, products = request_json(
        demo_url, {"traceparent": f"00-{trace_id}-{parent_id}-01"}
    )
    if status != 200 or not isinstance(products, list) or not products:
        raise AssertionError(f"supported Demo request returned {status}: {products!r}")

    params = {"from": stamp(begin), "to": stamp(begin + timedelta(minutes=2))}
    deadline = time.monotonic() + 60
    observed = None
    while time.monotonic() < deadline:
        search = api("/api/v1/traces", {**params, "service": "frontend", "limit": "500"})
        if any(x["trace_id"] == trace_id for x in search.get("traces", [])):
            try:
                detail = api(f"/api/v1/traces/{trace_id}", params)
            except urllib.error.HTTPError as exc:
                if exc.code != 404:
                    raise
            else:
                if matching_trace(detail, trace_id, parent_id):
                    observed = detail
                    break
        if observed:
            break
        time.sleep(2)
    if not observed:
        raise AssertionError("injected Demo trace ID was not queryable with linked frontend, recommendationservice, and productcatalogservice spans within 60 seconds")

    spans = observed["spans"]
    services = sorted({span["service_name"] for span in spans})
    print(json.dumps({
        "result": "PASS", "trace_id": observed["trace_id"], "attribution": "injected traceparent",
        "span_count": len(spans), "services": services,
        "from": params["from"], "to": params["to"],
        "missing_parent_ids": observed.get("missing_parent_ids", []),
        "has_source_drops": observed.get("has_source_drops"),
        "request": demo_url,
    }, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (AssertionError, urllib.error.URLError, ValueError, KeyError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        sys.exit(1)
