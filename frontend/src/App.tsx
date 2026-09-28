import { useEffect, useRef, useState } from 'react'
import { detail, search, services, type SearchFilters, type SearchHit, type SearchPage, type Span, type Summaries, type TraceDetail } from './api'
import { millisecondsToNs, timestampNs, utcField } from './time'
import { IncidentView } from './IncidentView'

const minute = 60_000
const maxWindow = 24 * 60 * minute
const utcInput = (date: Date) => date.toISOString().slice(0, 16)
const initialTo = new Date()
const initialFrom = new Date(initialTo.getTime() - 15 * minute)

function displayTime(value: string) { return new Date(value).toISOString().replace('T', ' ').replace(/\.\d{3}Z$/, ' UTC') }
function duration(value: string) {
  const ns = Number(value)
  if (!Number.isFinite(ns)) return value + ' ns'
  if (ns >= 1e9) return `${(ns / 1e9).toFixed(2)} s`
  if (ns >= 1e6) return `${(ns / 1e6).toFixed(2)} ms`
  if (ns >= 1e3) return `${(ns / 1e3).toFixed(2)} µs`
  return `${ns} ns`
}
const statusName = (code: number) => code === 2 ? 'ERROR' : code === 1 ? 'OK' : 'UNSET'
const initialForm = { from: utcInput(initialFrom), to: utcInput(initialTo), service: '', namespace: '', emptyNamespace: false, operation: '', minMs: '', maxMs: '', status: '', limit: 100 }
type Form = typeof initialForm

function filtersOf(form: Form): SearchFilters {
  const from = new Date(`${form.from}Z`)
  const to = new Date(`${form.to}Z`)
  if (!Number.isFinite(from.getTime()) || !Number.isFinite(to.getTime()) || from >= to) throw new Error('Choose a valid start and end time.')
  if (to.getTime() - from.getTime() > maxWindow) throw new Error('Choose a window of 24 hours or less.')
  const result: SearchFilters = { from: utcField(form.from), to: utcField(form.to), limit: form.limit }
  if (form.service !== '') result.service = form.service
  if (form.emptyNamespace) result.namespace = ''
  else if (form.namespace !== '') result.namespace = form.namespace
  if (form.operation !== '') result.operation = form.operation
  for (const [value, key] of [[form.minMs, 'min_duration_ns'], [form.maxMs, 'max_duration_ns']] as const) {
    if (value !== '') {
      result[key] = millisecondsToNs(value)
    }
  }
  if (result.min_duration_ns && result.max_duration_ns && BigInt(result.min_duration_ns) > BigInt(result.max_duration_ns)) throw new Error('Minimum duration must not exceed maximum duration.')
  if (form.status) result.status = form.status
  return result
}

function SpanContext({ span }: { span: Span }) {
  const context = [
    ['Resource attributes', span.resource_attributes], ['Span attributes', span.span_attributes], ['Scope attributes', span.scope_attributes],
    ['Events', span.events], ['Links', span.links]
  ] as const
  return <div className="span-context">
    <dl className="detail-grid"><dt>Service</dt><dd>{span.service_name}</dd><dt>Namespace</dt><dd>{span.service_namespace || '—'}</dd><dt>Operation</dt><dd>{span.span_name}</dd><dt>Span ID</dt><dd><code>{span.span_id}</code></dd><dt>Parent ID</dt><dd><code>{span.parent_span_id || 'Explicit root'}</code></dd><dt>Kind</dt><dd>{span.span_kind}</dd><dt>Status</dt><dd>{statusName(span.status_code)}{span.status_message && ` — ${span.status_message}`}</dd><dt>Start · UTC</dt><dd><code>{span.start_time}</code></dd><dt>End · UTC</dt><dd><code>{span.end_time}</code></dd><dt>Ingested · UTC</dt><dd><code>{span.ingested_at}</code></dd><dt>Instrumentation</dt><dd>{span.scope_name || '—'} {span.scope_version}</dd><dt>Trace flags</dt><dd>{span.trace_flags}</dd><dt>Trace state</dt><dd>{span.trace_state || '—'}</dd></dl>
    {context.map(([label, value]) => <details key={label}><summary>{label}</summary><pre>{JSON.stringify(value, null, 2)}</pre></details>)}
    <p className="muted">Source drops: {span.dropped_attributes_count} span attributes, {span.dropped_events_count} events, {span.dropped_links_count} links, {span.resource_dropped_attributes_count} resource attributes, {span.scope_dropped_attributes_count} scope attributes.</p>
  </div>
}

function Waterfall({ trace }: { trace: TraceDetail }) {
  const [selected, setSelected] = useState<string | null>(null)
  const spans = [...trace.spans].sort((a, b) => {
    const difference = timestampNs(a.start_time) - timestampNs(b.start_time)
    return difference < 0n ? -1 : difference > 0n ? 1 : a.span_id.localeCompare(b.span_id)
  })
  const byId = new Map(spans.map(span => [span.span_id, span]))
  const origin = timestampNs(trace.observed_start_time)
  const elapsed = BigInt(trace.observed_elapsed_ns) > 0n ? BigInt(trace.observed_elapsed_ns) : 1n
  function depth(span: Span) {
    let count = 0; let parent = span.parent_span_id; const visited = new Set([span.span_id])
    while (parent && byId.has(parent) && !visited.has(parent)) { visited.add(parent); count++; parent = byId.get(parent)!.parent_span_id }
    return Math.min(count, 8)
  }
  return <>
    <div className="waterfall-heading"><span>Observed spans · {spans.length}</span><span>Observed elapsed · {duration(trace.observed_elapsed_ns)}</span></div>
    <div className="waterfall" role="list" aria-label="Trace waterfall">{spans.map(span => {
      const left = Math.max(0, Math.min(100, Number(timestampNs(span.start_time) - origin) / Number(elapsed) * 100))
      const width = Math.max(.8, Math.min(100 - left, Number(BigInt(span.duration_ns)) / Number(elapsed) * 100))
      const active = selected === span.span_id
      return <div className="span-block" role="listitem" key={span.span_id}>
        <button type="button" className={`span-row ${active ? 'selected' : ''}`} onClick={() => setSelected(active ? null : span.span_id)} aria-expanded={active}>
          <span className="span-label" style={{ '--indent-full': `${depth(span) * 18}px`, '--indent-mobile': `${Math.min(depth(span), 2) * 6}px` } as React.CSSProperties}><span className={`status-dot ${statusName(span.status_code).toLowerCase()}`} aria-hidden="true"/><span className="span-identity"><strong>{span.service_name}</strong>{span.service_namespace && <small className="span-namespace">{span.service_namespace}</small>}<small className="span-operation">{span.span_name}{span.parent_span_id && !byId.has(span.parent_span_id) && ' · parent outside view'}</small></span>{span.status_code === 2 && <span className="error-badge">ERROR</span>}</span>
          <span className="track"><span className={`bar ${statusName(span.status_code).toLowerCase()}`} style={{ left: `${left}%`, width: `${width}%` }}/></span><span className="span-duration">{duration(span.duration_ns)}</span>
        </button>{active && <SpanContext span={span}/>}</div>
    })}</div>
  </>
}

export function App() {
  const [view, setView] = useState<'traces' | 'incidents'>('traces')
  const [incidentVisited, setIncidentVisited] = useState(false)
  const traceDetailElement = useRef<HTMLElement>(null)
  const [form, setForm] = useState<Form>(initialForm)
  const [filters, setFilters] = useState<SearchFilters | null>(null)
  const [pages, setPages] = useState<SearchPage[]>([])
  const [pageIndex, setPageIndex] = useState(0)
  const [summary, setSummary] = useState<Summaries | null>(null)
  const [trace, setTrace] = useState<TraceDetail | null>(null)
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [detailRange, setDetailRange] = useState<{ from: string; to: string } | null>(null)
  const [loading, setLoading] = useState(false)
  const [detailLoading, setDetailLoading] = useState(false)
  const [error, setError] = useState('')
  const [detailError, setDetailError] = useState('')
  const [requestCursor, setRequestCursor] = useState<string | undefined>()
  const [retryNonce, setRetryNonce] = useState(0)
  const page = pages[pageIndex] || null

  useEffect(() => {
    if (selectedId && view === 'traces') {
      traceDetailElement.current?.focus()
      traceDetailElement.current?.scrollIntoView?.({ block: 'start', behavior: 'smooth' })
    }
  }, [selectedId, view])

  useEffect(() => {
    if (!filters) return
    const controller = new AbortController()
    setLoading(true); setError('')
    Promise.all([search(filters, requestCursor, controller.signal), services(filters, controller.signal)]).then(([nextPage, nextSummary]) => {
      if (requestCursor === undefined) { setPages([nextPage]); setPageIndex(0) }
      else { setPages(current => [...current, nextPage]); setPageIndex(current => current + 1) }
      setSummary(nextSummary)
    }).catch(e => { if (!controller.signal.aborted) setError(e.message) }).finally(() => { if (!controller.signal.aborted) setLoading(false) })
    return () => controller.abort()
  }, [filters, requestCursor, retryNonce])

  useEffect(() => {
    if (!selectedId || !detailRange) return
    const controller = new AbortController()
    setDetailLoading(true); setDetailError(''); setTrace(null)
    detail(selectedId, detailRange.from, detailRange.to, controller.signal).then(setTrace).catch(e => { if (!controller.signal.aborted) setDetailError(e.message) }).finally(() => { if (!controller.signal.aborted) setDetailLoading(false) })
    return () => controller.abort()
  }, [selectedId, detailRange])

  function submit(event: React.FormEvent) { event.preventDefault(); try { const next = filtersOf(form); setSelectedId(null); setTrace(null); setPages([]); setPageIndex(0); setSummary(null); setRequestCursor(undefined); setFilters(next); setError('') } catch (e) { setError((e as Error).message) } }
  function open(hit: SearchHit) { if (!filters) return; setSelectedId(hit.trace_id); setDetailRange({ from: filters.from, to: filters.to }) }
  function openEvidence(selection: { traceId: string; from: string; to: string }) {
    setView('traces')
    setSelectedId(selection.traceId)
    setDetailRange({ from: selection.from, to: selection.to })
  }
  function expand() {
    if (!detailRange) return
    const now = Date.now(); const floor = now - 7 * 24 * 60 * minute; const ceiling = now + 5 * minute
    const oldFrom = Date.parse(detailRange.from), oldTo = Date.parse(detailRange.to)
    const available = maxWindow - (oldTo - oldFrom)
    if (available <= 0) { setDetailError('The maximum supported interval is already shown.'); return }
    const left = Math.min(15 * minute, Math.max(0, oldFrom - floor), Math.ceil(available / 2))
    const right = Math.min(15 * minute, Math.max(0, ceiling - oldTo), available - left)
    const from = oldFrom - left
    const to = oldTo + right
    if (from === Date.parse(detailRange.from) && to === Date.parse(detailRange.to)) { setDetailError('The maximum supported interval is already shown.'); return }
    setDetailRange({ from: new Date(from).toISOString(), to: new Date(to).toISOString() })
  }
  function refresh() { if (!filters) return; setSelectedId(null); setTrace(null); setPages([]); setPageIndex(0); setRequestCursor(undefined); setFilters({ ...filters }) }
  function nextPage() { if (!page?.next_cursor) return; if (pages[pageIndex + 1]) setPageIndex(pageIndex + 1); else setRequestCursor(page.next_cursor) }
  function previousPage() { setPageIndex(Math.max(0, pageIndex - 1)) }
  function retryPage() { if (filters) setRetryNonce(current => current + 1) }
  const update = (name: keyof Form, value: string | number | boolean) => setForm(current => ({ ...current, [name]: value }))

  return <div className="app-shell">
    <header className="site-header"><div className="brand"><span className="brand-mark">◎</span><span>IncidentLens</span></div><nav className="top-nav" aria-label="Main navigation"><button type="button" aria-current={view === 'traces' ? 'page' : undefined} onClick={() => setView('traces')}>Trace explorer</button><button type="button" aria-current={view === 'incidents' ? 'page' : undefined} onClick={() => { setIncidentVisited(true); setView('incidents') }}>Incidents</button></nav><span className="header-meta"><span className="live-dot"/> Local</span></header>
    <main><div className="intro"><div><p className="eyebrow">EXPLORE / {view === 'traces' ? 'TRACES' : 'INCIDENTS'}</p><h1>{view === 'traces' ? 'Follow the request.' : 'Investigate a window.'}</h1><p>{view === 'traces' ? 'Search stored spans, then inspect their timing and relationships across services.' : 'Compare trace-derived service operations with a fixed baseline and inspect supporting traces.'}</p></div><div className="scope-note">TRACE DATA ONLY<br/><span>Observed spans may arrive late or be missing.</span></div></div>
    <div hidden={view !== 'traces'}>
    <form className="search-panel" onSubmit={submit} aria-label="Trace search"><div className="panel-title"><h2>Search traces</h2><span>UTC query window · up to 24 hours</span></div>
      <div className="field-grid"><label>From (UTC)<input aria-label="From (UTC)" type="datetime-local" value={form.from} onChange={e => update('from', e.target.value)} required/></label><label>To (UTC)<input aria-label="To (UTC)" type="datetime-local" value={form.to} onChange={e => update('to', e.target.value)} required/></label><label>Service<input aria-label="Service" placeholder="e.g. frontend" value={form.service} onChange={e => update('service', e.target.value)}/></label><div className="namespace-field"><label>Namespace<input aria-label="Namespace" placeholder="Any namespace" disabled={form.emptyNamespace} value={form.namespace} onChange={e => update('namespace', e.target.value)}/></label><label className="check-label"><input type="checkbox" checked={form.emptyNamespace} onChange={e => update('emptyNamespace', e.target.checked)}/> Empty namespace only</label></div><label>Operation<input aria-label="Operation" placeholder="Exact span name" value={form.operation} onChange={e => update('operation', e.target.value)}/></label><label>Min duration · ms<input aria-label="Min duration · ms" type="text" inputMode="decimal" value={form.minMs} onChange={e => update('minMs', e.target.value)}/></label><label>Max duration · ms<input aria-label="Max duration · ms" type="text" inputMode="decimal" value={form.maxMs} onChange={e => update('maxMs', e.target.value)}/></label><label>Status<select aria-label="Status" value={form.status} onChange={e => update('status', e.target.value)}><option value="">Any status</option><option>ERROR</option><option>OK</option><option>UNSET</option></select></label></div>
      <div className="form-actions"><p>Filters apply to the same span. Durations below describe matching spans, not whole traces.</p><button className="primary" type="submit">Search traces <span aria-hidden="true">→</span></button></div>
    </form>
    {error && <div role="alert" className="notice error">{error} {filters && <button type="button" onClick={retryPage}>Retry</button>}</div>}
    {loading && <div role="status" className="notice">Loading traces and service summaries…</div>}
    {!filters && <div className="welcome"><span className="welcome-icon">⌁</span><h2>Start with a time window</h2><p>Search recent traces or narrow the results to a service, operation, duration, or status.</p></div>}
    {filters && !loading && page && <div className="results-layout"><section className="results-card"><div className="section-heading"><div><p className="eyebrow">RESULTS</p><h2>Matching traces <span className="count">{page.traces.length}</span></h2></div><button className="quiet" type="button" onClick={refresh}>↻ Refresh search</button></div><p className="caption">Observed {displayTime(page.observed_at)}. Refresh starts a new ingestion cutoff; pages retain the initial cutoff.</p>
      {page.traces.length === 0 ? <div className="empty">No matching traces in this window. Try a wider time range or fewer filters. This does not prove the service is healthy.</div> : <div className="trace-list">{page.traces.map(hit => <button className={`trace-hit ${selectedId === hit.trace_id ? 'active' : ''}`} type="button" key={hit.trace_id} onClick={() => open(hit)}><span><strong className="trace-id">{hit.trace_id}</strong><small>{displayTime(hit.matching_start_time)}</small></span><span className="hit-metrics"><b>{hit.matching_span_count} matched</b><small>{duration(hit.matching_min_duration_ns)}–{duration(hit.matching_max_duration_ns)} · {hit.matching_error_count} errors</small></span><span aria-hidden="true">↗</span></button>)}</div>}
      <div className="pagination"><button type="button" disabled={pageIndex === 0} onClick={previousPage}>← Previous</button><span>Page {pageIndex + 1}</span><button type="button" disabled={!page.next_cursor} onClick={nextPage}>Next →</button></div></section>
      <aside className="summary-card"><div className="section-heading"><div><p className="eyebrow">SERVER SPANS</p><h2>Service activity</h2></div></div><p className="caption">Trace-derived summaries use this time window and service, namespace, and operation filters. Duration and status filters apply only to trace results. UNSET does not mean success.</p>{summary?.truncated && <div className="notice">Summary limit reached. Narrow the filters to see more operations.</div>}{summary?.services.length === 0 && <div className="empty small">No SERVER spans observed. There is insufficient data for a service summary.</div>}{summary?.services.map((item, i) => <div className="summary-row" key={`${item.service_namespace}/${item.service_name}/${item.operation}/${i}`}><strong>{item.service_namespace && `${item.service_namespace}/`}{item.service_name}</strong><span>{item.operation}</span><div><span>{item.span_count} spans</span><span>p95 {duration(item.p95_duration_ns)}</span></div><small>{item.error_count} ERROR · {item.unset_count} UNSET</small></div>)}</aside></div>}
    {selectedId && <section ref={traceDetailElement} tabIndex={-1} className="detail-card" aria-label="Trace detail"><div className="section-heading"><div><p className="eyebrow">TRACE DETAIL</p><h2 className="detail-title">{selectedId}</h2></div><button type="button" className="quiet" onClick={() => { setSelectedId(null); setTrace(null) }}>Close ✕</button></div>{detailRange && <p className="caption">Observed interval: {displayTime(detailRange.from)} to {displayTime(detailRange.to)}.</p>}{detailLoading && <div role="status" className="notice">Loading trace spans…</div>}{detailError && <div role="alert" className="notice error">{detailError}</div>}{trace && <><div className="caveats"><div className="notice subtle">Only returned spans are shown. A root does not prove complete delivery. Refresh to reveal late arrivals.</div>{trace.truncated && <div className="notice warning">Trace truncated{trace.truncation_reason && `: ${trace.truncation_reason}`}. The waterfall contains only returned spans.</div>}{trace.has_missing_root && <div className="notice warning">No explicit root observed in this interval.</div>}{trace.missing_parent_ids.length > 0 && <div className="notice warning">{trace.missing_parent_ids.length} parent span ID{trace.missing_parent_ids.length === 1 ? '' : 's'} missing from this view: {trace.missing_parent_ids.join(', ')}.</div>}{trace.has_cycles && <div className="notice warning">Parent links contain a cycle; indentation may not represent a valid tree.</div>}{trace.has_source_drops && <div className="notice warning">The source reported dropped attributes, events, or links. Open a span for counts.</div>}</div><div className="detail-actions"><span>{trace.root_count} explicit root{trace.root_count === 1 ? '' : 's'} · {trace.spans.length} observed spans</span><div><button type="button" onClick={() => setDetailRange({ ...detailRange! })}>↻ Refresh trace</button><button type="button" onClick={expand}>Expand interval ±15 min</button></div></div><Waterfall trace={trace}/></>}</section>}
    </div>
    {incidentVisited && <div hidden={view !== 'incidents'}><IncidentView onOpenEvidence={openEvidence}/></div>}
    </main><footer>IncidentLens · Local trace investigation</footer></div>
}
