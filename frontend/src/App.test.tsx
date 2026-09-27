import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { App } from './App'

const traceId = '0123456789abcdef0123456789abcdef'
const apiResponse = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } })
const summary = { observed_at: '2026-09-26T12:00:00Z', truncated: false, services: [{ service_namespace: 'shop', service_name: 'catalog', operation: 'GetProduct', span_count: 5, error_count: 1, unset_count: 2, error_rate: .2, unset_rate: .4, p50_duration_ns: '2000000', p95_duration_ns: '9000000' }] }
const page = { observed_at: '2026-09-26T12:00:00Z', ingestion_cutoff: '2026-09-26T12:00:00Z', next_cursor: '', traces: [{ trace_id: traceId, matching_start_time: '2026-09-26T11:59:00Z', matching_span_count: 2, matching_min_duration_ns: '1000000', matching_max_duration_ns: '9000000', matching_error_count: 1 }] }
const span = (overrides: Record<string, unknown> = {}) => ({ span_id: 'aaaaaaaaaaaaaaaa', parent_span_id: '', service_name: 'frontend', service_namespace: 'shop', span_name: 'GET /', span_kind: 2, status_code: 1, status_message: '', start_time: '2026-09-26T11:59:00Z', end_time: '2026-09-26T11:59:01Z', duration_ns: '1000000000', ingested_at: '2026-09-26T11:59:02Z', resource_attributes: { 'service.name': { stringValue: 'frontend' } }, span_attributes: { 'http.method': { stringValue: 'GET' } }, scope_attributes: {}, events: [], links: [], scope_name: 'test', scope_version: '1', resource_schema_url: '', scope_schema_url: '', trace_state: '', trace_flags: 1, dropped_attributes_count: 0, dropped_events_count: 0, dropped_links_count: 0, resource_dropped_attributes_count: 0, scope_dropped_attributes_count: 0, ...overrides })
const trace = { trace_id: traceId, observed_at: '2026-09-26T12:00:00Z', from: '2026-09-26T11:00:00Z', to: '2026-09-26T12:00:00Z', spans: [span(), span({ span_id: 'bbbbbbbbbbbbbbbb', parent_span_id: 'aaaaaaaaaaaaaaaa', service_name: 'catalog', span_name: 'GetProduct', status_code: 2, status_message: 'failed', start_time: '2026-09-26T11:59:00.1Z', end_time: '2026-09-26T11:59:00.3Z', duration_ns: '200000000', dropped_events_count: 1 })], truncated: false, truncation_reason: '', missing_parent_ids: [], root_count: 1, has_missing_root: false, has_cycles: false, has_source_drops: true, observed_start_time: '2026-09-26T11:59:00Z', observed_end_time: '2026-09-26T11:59:01Z', observed_elapsed_ns: '1000000000' }
let requests: URL[]
let responses: (url: URL) => Response

beforeEach(() => {
  requests = []
  responses = url => url.pathname.endsWith('/services') ? apiResponse(summary) : url.pathname.endsWith(`/traces/${traceId}`) ? apiResponse(trace) : apiResponse(page)
  vi.stubGlobal('fetch', vi.fn((input: string) => { const url = new URL(input, 'http://localhost'); requests.push(url); return Promise.resolve(responses(url)) }))
})
afterEach(() => vi.unstubAllGlobals())

describe('trace explorer', () => {
  it('searches by keyboard and shows matching-span facts and service summaries', async () => {
    const user = userEvent.setup()
    render(<App />)
    await user.type(screen.getByRole('textbox', { name: 'Service' }), 'catalog')
    await user.selectOptions(screen.getByRole('combobox', { name: 'Status' }), 'ERROR')
    await user.click(screen.getByRole('button', { name: /Search traces/ }))
    expect(await screen.findByText(traceId)).toBeTruthy()
    expect(screen.getByText('2 matched')).toBeTruthy()
    expect(screen.getByText('5 spans')).toBeTruthy()
    const query = requests.find(url => url.pathname.endsWith('/traces'))!
    expect(query.searchParams.get('service')).toBe('catalog')
    expect(query.searchParams.get('status')).toBe('ERROR')
    expect(query.searchParams.get('from')).toMatch(/Z$/)
  })

  it('shows a loading state until search completes', async () => {
    let finishSearch!: (response: Response) => void
    const heldSearch = new Promise<Response>(resolve => { finishSearch = resolve })
    vi.stubGlobal('fetch', vi.fn((input: string) => {
      const url = new URL(input, 'http://localhost')
      return url.pathname.endsWith('/services') ? Promise.resolve(apiResponse(summary)) : heldSearch
    }))
    const user = userEvent.setup(); render(<App />)
    await user.click(screen.getByRole('button', { name: /Search traces/ }))
    expect(screen.getByRole('status')).toHaveProperty('textContent', 'Loading traces and service summaries…')
    finishSearch(apiResponse(page))
    expect(await screen.findByText(traceId)).toBeTruthy()
    await waitFor(() => expect(screen.queryByRole('status')).toBeNull())
  })

  it('opens a multi-service waterfall and exposes source-loss details by keyboard', async () => {
    const user = userEvent.setup()
    render(<App />)
    await user.click(screen.getByRole('button', { name: /Search traces/ }))
    await user.click(await screen.findByRole('button', { name: new RegExp(traceId) }))
    expect(await screen.findByRole('list', { name: 'Trace waterfall' })).toBeTruthy()
    expect(within(screen.getByRole('list', { name: 'Trace waterfall' })).getAllByRole('listitem')).toHaveLength(2)
    expect(screen.getByText(/source reported dropped/i)).toBeTruthy()
    expect(within(screen.getByRole('list', { name: 'Trace waterfall' })).getByText('ERROR')).toBeTruthy()
    await user.click(screen.getByRole('button', { name: /catalog.*GetProduct/i }))
    expect(screen.getByText(/ERROR — failed/)).toBeTruthy()
    expect(screen.getByText(/1 events/)).toBeTruthy()
    expect(screen.getByText(/Parent ID/)).toBeTruthy()
    const metadata = screen.getByText('Parent ID').closest('dl')!
    expect(within(metadata).getByText('catalog')).toBeTruthy()
    expect(within(metadata).getByText('shop')).toBeTruthy()
    expect(within(metadata).getByText('GetProduct')).toBeTruthy()
  })

  it('shows incomplete and truncated observations, and expands detail interval', async () => {
    responses = url => url.pathname.endsWith('/services') ? apiResponse(summary) : url.pathname.endsWith(`/traces/${traceId}`) ? apiResponse({ ...trace, truncated: true, truncation_reason: 'span cap', has_missing_root: true, has_cycles: true, missing_parent_ids: ['cccccccccccccccc'] }) : apiResponse(page)
    const user = userEvent.setup(); render(<App />)
    await user.click(screen.getByRole('button', { name: /Search traces/ }))
    await user.click(await screen.findByRole('button', { name: new RegExp(traceId) }))
    expect(await screen.findByText(/Trace truncated: span cap/)).toBeTruthy()
    expect(screen.getByText(/No explicit root observed/)).toBeTruthy()
    expect(screen.getByText(/Parent links contain a cycle/)).toBeTruthy()
    await user.click(screen.getByRole('button', { name: /Expand interval/ }))
    await waitFor(() => expect(requests.filter(url => url.pathname.endsWith(`/traces/${traceId}`))).toHaveLength(2))
    const ranges = requests.filter(url => url.pathname.endsWith(`/traces/${traceId}`))
    expect(Date.parse(ranges[1].searchParams.get('from')!)).toBeLessThan(Date.parse(ranges[0].searchParams.get('from')!))
  })

  it('shows empty, insufficient, and request failure states', async () => {
    responses = url => url.pathname.endsWith('/services') ? apiResponse({ ...summary, services: [] }) : apiResponse({ ...page, traces: [] })
    const user = userEvent.setup(); const view = render(<App />)
    await user.click(screen.getByRole('button', { name: /Search traces/ }))
    expect(await screen.findByText(/No matching traces/)).toBeTruthy()
    expect(screen.getByText(/insufficient data for a service summary/)).toBeTruthy()
    view.unmount()
    responses = () => apiResponse({ error: { code: 'unavailable', message: 'Database unavailable' } }, 503)
    render(<App />)
    await user.click(screen.getByRole('button', { name: /Search traces/ }))
    expect(await screen.findByRole('alert')).toHaveProperty('textContent', expect.stringContaining('Database unavailable'))
  })

  it('uses the cursor for the next page and clears it on refresh', async () => {
    responses = url => url.pathname.endsWith('/services') ? apiResponse(summary) : apiResponse(url.searchParams.has('cursor') ? { ...page, next_cursor: '', traces: [] } : { ...page, next_cursor: 'opaque-page-2' })
    const user = userEvent.setup(); render(<App />)
    await user.click(screen.getByRole('button', { name: /Search traces/ }))
    await user.click(await screen.findByRole('button', { name: 'Next →' }))
    expect(await screen.findByText(/No matching traces/)).toBeTruthy()
    expect(requests.filter(url => url.pathname.endsWith('/traces')).at(-1)?.searchParams.get('cursor')).toBe('opaque-page-2')
    await user.click(screen.getByRole('button', { name: /Refresh search/ }))
    await screen.findByText(traceId)
    expect(requests.filter(url => url.pathname.endsWith('/traces')).at(-1)?.searchParams.has('cursor')).toBe(false)
  })

  it('returns to the cached first page after a late arrival', async () => {
    const lateId = 'ffffffffffffffffffffffffffffffff'
    let freshCalls = 0
    responses = url => {
      if (url.pathname.endsWith('/services')) return apiResponse(summary)
      if (url.searchParams.has('cursor')) return apiResponse({ ...page, next_cursor: '', traces: [] })
      freshCalls++
      return apiResponse({ ...page, next_cursor: 'opaque-page-2', traces: freshCalls === 1 ? page.traces : [{ ...page.traces[0], trace_id: lateId }] })
    }
    const user = userEvent.setup(); render(<App />)
    await user.click(screen.getByRole('button', { name: /Search traces/ }))
    await user.click(await screen.findByRole('button', { name: 'Next →' }))
    await screen.findByText(/No matching traces/)
    await user.click(screen.getByRole('button', { name: '← Previous' }))
    expect(await screen.findByText(traceId)).toBeTruthy()
    expect(freshCalls).toBe(1)
    await user.click(screen.getByRole('button', { name: /Refresh search/ }))
    expect(await screen.findByText(lateId)).toBeTruthy()
    expect(freshCalls).toBe(2)
  })

  it('retains the current page and number if next-page fetch fails, then retries', async () => {
    let fail = true
    responses = url => {
      if (url.pathname.endsWith('/services')) return apiResponse(summary)
      if (url.searchParams.has('cursor') && fail) return apiResponse({ error: { message: 'Page temporarily unavailable' } }, 503)
      return apiResponse({ ...page, next_cursor: url.searchParams.has('cursor') ? '' : 'opaque-page-2', traces: url.searchParams.has('cursor') ? [] : page.traces })
    }
    const user = userEvent.setup(); render(<App />)
    await user.click(screen.getByRole('button', { name: /Search traces/ }))
    await user.click(await screen.findByRole('button', { name: 'Next →' }))
    expect(await screen.findByRole('alert')).toHaveProperty('textContent', expect.stringContaining('Page temporarily unavailable'))
    expect(screen.getByText('Page 1')).toBeTruthy()
    expect(screen.getByText(traceId)).toBeTruthy()
    fail = false
    await user.click(screen.getByRole('button', { name: 'Retry' }))
    expect(await screen.findByText('Page 2')).toBeTruthy()
    const searches = requests.filter(url => url.pathname.endsWith('/traces'))
    expect(searches).toHaveLength(3)
    expect(searches[0].searchParams.has('cursor')).toBe(false)
    expect(searches[1].searchParams.get('cursor')).toBe('opaque-page-2')
    expect(searches[2].searchParams.get('cursor')).toBe('opaque-page-2')
  })

  it('keeps exact whitespace filters and sends an explicit empty namespace', async () => {
    const user = userEvent.setup(); render(<App />)
    await user.type(screen.getByRole('textbox', { name: 'Service' }), ' catalog ')
    await user.type(screen.getByRole('textbox', { name: 'Operation' }), ' GetProduct ')
    await user.click(screen.getByRole('checkbox', { name: 'Empty namespace only' }))
    await user.type(screen.getByRole('textbox', { name: 'Min duration · ms' }), '0.000001')
    await user.click(screen.getByRole('button', { name: /Search traces/ }))
    await screen.findByText(traceId)
    const query = requests.find(url => url.pathname.endsWith('/traces'))!
    expect(query.searchParams.get('service')).toBe(' catalog ')
    expect(query.searchParams.get('operation')).toBe(' GetProduct ')
    expect(query.searchParams.has('namespace')).toBe(true)
    expect(query.searchParams.get('namespace')).toBe('')
    expect(query.searchParams.get('min_duration_ns')).toBe('1')
  })

  it('orders sub-millisecond spans precisely and displays their exact UTC timestamps', async () => {
    const precise = { ...trace, observed_start_time: '2026-09-26T11:59:00Z', observed_end_time: '2026-09-26T11:59:00.000001Z', observed_elapsed_ns: '1000', spans: [span({ span_id: 'bbbbbbbbbbbbbbbb', span_name: 'later', start_time: '2026-09-26T11:59:00.000000500Z', end_time: '2026-09-26T11:59:00.000000600Z', duration_ns: '100' }), span({ span_id: 'aaaaaaaaaaaaaaaa', span_name: 'first', start_time: '2026-09-26T11:59:00Z', end_time: '2026-09-26T11:59:00.000000100Z', duration_ns: '100' })] }
    responses = url => url.pathname.endsWith('/services') ? apiResponse(summary) : url.pathname.endsWith(`/traces/${traceId}`) ? apiResponse(precise) : apiResponse(page)
    const user = userEvent.setup(); render(<App />)
    await user.click(screen.getByRole('button', { name: /Search traces/ }))
    await user.click(await screen.findByRole('button', { name: new RegExp(traceId) }))
    const rows = within(await screen.findByRole('list', { name: 'Trace waterfall' })).getAllByRole('listitem')
    expect(rows[0].textContent).toContain('first')
    expect(rows[1].textContent).toContain('later')
    expect((rows[1].querySelector('.bar') as HTMLElement).style.left).toBe('50%')
    await user.click(within(rows[1]).getByRole('button'))
    expect(screen.getByText('2026-09-26T11:59:00.000000500Z')).toBeTruthy()
  })

  it('does not discard the selected interval when expansion hits the 24-hour cap', async () => {
    const user = userEvent.setup(); render(<App />)
    await user.clear(screen.getByLabelText('From (UTC)'))
    await user.type(screen.getByLabelText('From (UTC)'), '2026-09-25T12:00')
    await user.clear(screen.getByLabelText('To (UTC)'))
    await user.type(screen.getByLabelText('To (UTC)'), '2026-09-26T12:00')
    await user.click(screen.getByRole('button', { name: /Search traces/ }))
    await user.click(await screen.findByRole('button', { name: new RegExp(traceId) }))
    await screen.findByRole('list', { name: 'Trace waterfall' })
    await user.click(screen.getByRole('button', { name: /Expand interval/ }))
    expect(screen.getByText(/maximum supported interval/)).toBeTruthy()
    expect(requests.filter(url => url.pathname.endsWith(`/traces/${traceId}`))).toHaveLength(1)
  })
})
