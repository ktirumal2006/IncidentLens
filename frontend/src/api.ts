export type SearchFilters = { from: string; to: string; service?: string; namespace?: string; operation?: string; min_duration_ns?: string; max_duration_ns?: string; status?: string; limit: number }
export type Summary = { service_namespace: string; service_name: string; operation: string; span_count: number; error_count: number; unset_count: number; error_rate: number; unset_rate: number; p50_duration_ns: string; p95_duration_ns: string }
export type Summaries = { observed_at: string; truncated: boolean; services: Summary[] }
export type SearchHit = { trace_id: string; matching_start_time: string; matching_span_count: number; matching_min_duration_ns: string; matching_max_duration_ns: string; matching_error_count: number }
export type SearchPage = { observed_at: string; ingestion_cutoff: string; traces: SearchHit[]; next_cursor: string }
export type Span = { span_id: string; parent_span_id: string; service_name: string; service_namespace: string; span_name: string; span_kind: number; status_code: number; status_message: string; start_time: string; end_time: string; duration_ns: string; ingested_at: string; resource_attributes: unknown; span_attributes: unknown; scope_attributes: unknown; events: unknown; links: unknown; scope_name: string; scope_version: string; resource_schema_url: string; scope_schema_url: string; trace_state: string; trace_flags: number; dropped_attributes_count: number; dropped_events_count: number; dropped_links_count: number; resource_dropped_attributes_count: number; scope_dropped_attributes_count: number }
export type TraceDetail = { observed_at: string; from: string; to: string; trace_id: string; spans: Span[]; truncated: boolean; truncation_reason: string; missing_parent_ids: string[]; root_count: number; has_missing_root: boolean; has_cycles: boolean; has_source_drops: boolean; observed_start_time: string; observed_end_time: string; observed_elapsed_ns: string }
export type IncidentStatistics = { span_count: number; error_count: number; unset_count: number; error_rate: number; unset_rate: number; p95_duration_ns: string | null }
export type IncidentIdentity = { service_namespace: string; service_name: string; operation: string }
export type IncidentEvidence = { trace_id: string; span_id: string; start_time: string; duration_ns: string; status_code: number; from: string; to: string; caveats: string[]; downstream_anomalies: IncidentIdentity[]; error_propagation: boolean }
export type IncidentOperation = IncidentIdentity & { state: 'normal' | 'candidate' | 'insufficient_evidence'; baseline: IncidentStatistics; current: IncidentStatistics; p95_increase_ns: string | null; error_rate_increase: number | null; triggered_rules: Array<'latency' | 'errors'>; evidence: IncidentEvidence[]; caveats: string[]; service_rank?: number }
export type IncidentEvaluation = { rule_version: string; config: { min_samples: number; latency_ratio_milli: number; latency_delta_ns: string; error_rate_bps: number; error_increase_bps: number }; observed_at: string; end: string; baseline: { from: string; to: string }; current: { from: string; to: string }; operations: IncidentOperation[]; candidates: IncidentOperation[]; caveats: string[] }

async function get<T>(path: string, params: Record<string, string | number | undefined>, signal?: AbortSignal): Promise<T> {
  const query = new URLSearchParams()
  for (const [key, value] of Object.entries(params)) if (value !== undefined && (value !== '' || key === 'namespace')) query.set(key, String(value))
  const response = await fetch(`/api/v1${path}?${query}`, { signal })
  if (!response.ok) {
    const body = await response.json().catch(() => null) as { error?: { message?: string } } | null
    throw new Error(body?.error?.message || `Request failed (${response.status})`)
  }
  return response.json() as Promise<T>
}

export const services = (filters: SearchFilters, signal?: AbortSignal) => get<Summaries>('/services', { from: filters.from, to: filters.to, service: filters.service, namespace: filters.namespace, operation: filters.operation, limit: 100 }, signal)
export const search = (filters: SearchFilters, cursor?: string, signal?: AbortSignal) => get<SearchPage>('/traces', { ...filters, cursor }, signal)
export const detail = (id: string, from: string, to: string, signal?: AbortSignal) => get<TraceDetail>(`/traces/${encodeURIComponent(id)}`, { from, to }, signal)
export const incidents = (end: string, service?: string, namespace?: string, signal?: AbortSignal) => get<IncidentEvaluation>('/incidents', { end, service, namespace }, signal)
