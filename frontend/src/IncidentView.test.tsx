import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { App } from './App'
import type { IncidentEvaluation, IncidentOperation } from './api'

const traceId = '0123456789abcdef0123456789abcdef'
const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
const window = {
  from: '2026-09-26T11:55:00.123456789Z',
  to: '2026-09-26T12:00:00.123456789Z',
}
const stats = (
  count: number,
  errors: number,
  unset: number,
  p95: string | null,
) => ({
  span_count: count,
  error_count: errors,
  unset_count: unset,
  error_rate: count ? errors / count : 0,
  unset_rate: count ? unset / count : 0,
  p95_duration_ns: p95,
})
const evidence = [
  {
    trace_id: traceId,
    span_id: 'aaaaaaaaaaaaaaaa',
    start_time: '2026-09-26T11:59:00Z',
    duration_ns: '300000000',
    status_code: 2,
    from: window.from,
    to: window.to,
    caveats: ['missing_parent'],
    downstream_anomalies: [
      {
        service_namespace: 'shop',
        service_name: 'payment',
        operation: 'Charge',
      },
    ],
    error_propagation: true,
  },
]
const candidate = (
  rules: Array<'latency' | 'errors'> = ['errors'],
): IncidentOperation => ({
  service_namespace: 'shop',
  service_name: 'catalog',
  operation: 'GetProduct',
  state: 'candidate',
  baseline: stats(120, 0, 4, '100000000'),
  current: stats(120, 24, 8, '300000000'),
  p95_increase_ns: '200000000',
  error_rate_increase: 0.2,
  triggered_rules: rules,
  evidence,
  caveats: ['unset_status'],
  service_rank: 1,
})
const normal: IncidentOperation = {
  ...candidate([]),
  service_name: 'frontend',
  operation: 'GET /',
  state: 'normal',
  baseline: stats(120, 0, 0, '100000000'),
  current: stats(120, 0, 0, '100000000'),
  p95_increase_ns: '0',
  error_rate_increase: 0,
  triggered_rules: [],
  evidence: [],
  caveats: [],
  service_rank: undefined,
}
const insufficient: IncidentOperation = {
  ...candidate([]),
  service_name: 'recommendation',
  operation: 'Recommend',
  state: 'insufficient_evidence',
  baseline: stats(4, 0, 2, '100000000'),
  current: stats(0, 0, 0, null),
  p95_increase_ns: null,
  error_rate_increase: null,
  triggered_rules: [],
  evidence: [],
  caveats: ['no_server_coverage'],
  service_rank: undefined,
}
const evaluation = (
  operations: IncidentOperation[],
  candidates: IncidentOperation[] = operations.filter(
    (item) => item.state === 'candidate',
  ),
): IncidentEvaluation => ({
  rule_version: 'trace-v1',
  config: {
    min_samples: 100,
    latency_ratio_milli: 2000,
    latency_delta_ns: '100000000',
    error_rate_bps: 500,
    error_increase_bps: 500,
  },
  observed_at: '2026-09-26T12:01:00Z',
  end: '2026-09-26T12:00:00Z',
  baseline: { from: '2026-09-26T11:25:00Z', to: '2026-09-26T11:55:00Z' },
  current: window,
  operations,
  candidates,
  caveats: [],
})
const detail = {
  trace_id: traceId,
  observed_at: '2026-09-26T12:01:00Z',
  from: window.from,
  to: window.to,
  spans: [
    {
      span_id: 'aaaaaaaaaaaaaaaa',
      parent_span_id: '',
      service_name: 'catalog',
      service_namespace: 'shop',
      span_name: 'GetProduct',
      span_kind: 2,
      status_code: 2,
      status_message: 'failed',
      start_time: '2026-09-26T11:59:00Z',
      end_time: '2026-09-26T11:59:00.3Z',
      duration_ns: '300000000',
      ingested_at: '2026-09-26T11:59:01Z',
      resource_attributes: {},
      span_attributes: {},
      scope_attributes: {},
      events: [],
      links: [],
      scope_name: 'test',
      scope_version: '1',
      resource_schema_url: '',
      scope_schema_url: '',
      trace_state: '',
      trace_flags: 1,
      dropped_attributes_count: 0,
      dropped_events_count: 0,
      dropped_links_count: 0,
      resource_dropped_attributes_count: 0,
      scope_dropped_attributes_count: 0,
    },
  ],
  truncated: false,
  truncation_reason: '',
  missing_parent_ids: [],
  root_count: 1,
  has_missing_root: false,
  has_cycles: false,
  has_source_drops: false,
  observed_start_time: '2026-09-26T11:59:00Z',
  observed_end_time: '2026-09-26T11:59:00.3Z',
  observed_elapsed_ns: '300000000',
}

let requests: URL[]
let respond: (url: URL) => Promise<Response>
beforeEach(() => {
  requests = []
  respond = async (url) =>
    url.pathname.endsWith('/incidents')
      ? json(evaluation([normal]))
      : json(detail)
  vi.stubGlobal(
    'fetch',
    vi.fn((input: string) => {
      const url = new URL(input, 'http://localhost')
      requests.push(url)
      return respond(url)
    }),
  )
})
afterEach(() => vi.unstubAllGlobals())

async function openIncidents() {
  const user = userEvent.setup()
  render(<App />)
  await user.click(screen.getByRole('button', { name: 'Incidents' }))
  return user
}

describe('incident investigation', () => {
  it('shows a healthy evaluation with window, rule, thresholds and counts', async () => {
    await openIncidents()
    expect(await screen.findByText('0 candidate operations')).toBeTruthy()
    expect(screen.getByText(/Rule trace-v1/)).toBeTruthy()
    expect(
      screen.getByText(/At least 100 SERVER spans per window/),
    ).toBeTruthy()
    expect(
      screen.getByText(/No candidate among operations with enough samples/),
    ).toBeTruthy()
    const query = requests.find((url) => url.pathname.endsWith('/incidents'))!
    expect(query.searchParams.get('end')).toMatch(/Z$/)
    expect(query.searchParams.has('service')).toBe(false)
    expect(query.searchParams.has('namespace')).toBe(false)
    await userEvent.click(screen.getByText(/Other operation evaluations/))
    expect(screen.getAllByText('120')).toHaveLength(2)
    expect(screen.getByText('Normal')).toBeTruthy()
  })

  it('shows insufficient evidence without calling it healthy', async () => {
    respond = async (url) =>
      json(
        url.pathname.endsWith('/incidents')
          ? evaluation([insufficient])
          : detail,
      )
    await openIncidents()
    expect(
      await screen.findByText(
        /Insufficient evidence for every observed operation/,
      ),
    ).toBeTruthy()
    await userEvent.click(screen.getByText(/Other operation evaluations/))
    expect(
      screen.getByText(/Both windows need the configured minimum/),
    ).toBeTruthy()
    expect(
      screen.getByText(/No SERVER spans cover this operation/),
    ).toBeTruthy()
  })

  it('shows no SERVER coverage as insufficient data', async () => {
    respond = async () => json(evaluation([]))
    await openIncidents()
    expect(
      await screen.findByText(/No SERVER operation groups were observed/),
    ).toBeTruthy()
    expect(
      screen.getByText(/There is insufficient data to assess service health/),
    ).toBeTruthy()
  })

  it('shows configured threshold precision and readable caveats', async () => {
    const result = evaluation([candidate()])
    result.config.latency_ratio_milli = 2001
    result.config.latency_delta_ns = '100000001'
    result.config.error_rate_bps = 505
    result.config.error_increase_bps = 501
    result.caveats = ['source_drops', 'new_caveat_code']
    respond = async () => json(result)
    await openIncidents()
    expect(await screen.findByText(/p95 ≥ 2.001×/)).toBeTruthy()
    expect(screen.getByText(/100000001 ns/)).toBeTruthy()
    expect(screen.getByText(/ERROR share ≥ 5.05% and \+5.01 pp/)).toBeTruthy()
    expect(
      screen.getByText(/source reported dropped trace context/),
    ).toBeTruthy()
    expect(screen.getByText('new caveat code')).toBeTruthy()
  })

  it.each([
    [['errors'] as Array<'latency' | 'errors'>, 'error proportion'],
    [['latency'] as Array<'latency' | 'errors'>, 'latency p95'],
  ])('shows an explained candidate triggered by %s', async (rules, label) => {
    respond = async (url) =>
      json(
        url.pathname.endsWith('/incidents')
          ? evaluation([candidate(rules)])
          : detail,
      )
    await openIncidents()
    expect(await screen.findByText('1 candidate operation')).toBeTruthy()
    expect(screen.getByText(`Triggered: ${label}`)).toBeTruthy()
    expect(screen.getByText('Service rank 1')).toBeTruthy()
    expect(screen.getByText(/p95 change: 200.00 ms/)).toBeTruthy()
    expect(screen.getByText(/ERROR share change: \+20.00 pp/)).toBeTruthy()
    expect(screen.getByText(/Some observed spans have UNSET status/)).toBeTruthy()
    expect(screen.getByText(/Downstream co-occurring anomalies/)).toBeTruthy()
    expect(
      screen.getByText(/ERROR propagation along a parent chain/),
    ).toBeTruthy()
    expect(screen.getByText(/not root-cause findings/)).toBeTruthy()
  })

  it('opens supporting trace detail by keyboard in the evidence interval', async () => {
    respond = async (url) =>
      json(
        url.pathname.endsWith('/incidents')
          ? evaluation([candidate()])
          : detail,
      )
    const user = await openIncidents()
    const button = await screen.findByRole('button', {
      name: new RegExp(`Open trace ${traceId}`),
    })
    button.focus()
    await user.keyboard('{Enter}')
    expect(
      await screen.findByRole('list', { name: 'Trace waterfall' }),
    ).toBeTruthy()
    expect(document.activeElement).toBe(
      screen.getByRole('region', { name: 'Trace detail' }),
    )
    const request = requests.find((url) =>
      url.pathname.endsWith(`/traces/${traceId}`),
    )!
    expect(request.searchParams.get('from')).toBe(window.from)
    expect(request.searchParams.get('to')).toBe(window.to)
    expect(
      screen
        .getByRole('button', { name: 'Trace explorer' })
        .getAttribute('aria-current'),
    ).toBe('page')
    await user.click(screen.getByRole('button', { name: 'Incidents' }))
    expect(screen.getByText('1 candidate operation')).toBeTruthy()
    expect(
      requests.filter((url) => url.pathname.endsWith('/incidents')),
    ).toHaveLength(1)
  })

  it('shows loading, API failure, and retry', async () => {
    let finish!: (response: Response) => void
    let fail = true
    respond = (url) => {
      if (!url.pathname.endsWith('/incidents'))
        return Promise.resolve(json(detail))
      if (fail)
        return new Promise<Response>((resolve) => {
          finish = resolve
        })
      return Promise.resolve(json(evaluation([normal])))
    }
    const user = await openIncidents()
    expect(screen.getByRole('status')).toHaveProperty(
      'textContent',
      'Evaluating trace windows…',
    )
    finish(
      json(
        { error: { code: 'unavailable', message: 'ClickHouse unavailable' } },
        503,
      ),
    )
    expect(await screen.findByRole('alert')).toHaveProperty(
      'textContent',
      expect.stringContaining('ClickHouse unavailable'),
    )
    fail = false
    await user.click(screen.getByRole('button', { name: 'Retry evaluation' }))
    expect(await screen.findByText('0 candidate operations')).toBeTruthy()
  })

  it('keeps a fixed end on refresh and permits exact service/empty-namespace filters', async () => {
    const user = await openIncidents()
    await screen.findByText('0 candidate operations')
    await user.type(
      screen.getByRole('textbox', { name: 'Incident service' }),
      ' catalog ',
    )
    await user.click(
      screen.getByRole('checkbox', { name: 'Empty namespace only' }),
    )
    await user.click(screen.getByRole('button', { name: /Evaluate incidents/ }))
    await waitFor(() =>
      expect(
        requests.filter((url) => url.pathname.endsWith('/incidents')),
      ).toHaveLength(2),
    )
    const second = requests.filter((url) =>
      url.pathname.endsWith('/incidents'),
    )[1]
    expect(second.searchParams.get('service')).toBe(' catalog ')
    expect(second.searchParams.has('namespace')).toBe(true)
    expect(second.searchParams.get('namespace')).toBe('')
    await user.click(
      await screen.findByRole('button', { name: /Refresh same end/ }),
    )
    await waitFor(() =>
      expect(
        requests.filter((url) => url.pathname.endsWith('/incidents')),
      ).toHaveLength(3),
    )
    const third = requests.filter((url) =>
      url.pathname.endsWith('/incidents'),
    )[2]
    expect(third.searchParams.toString()).toBe(second.searchParams.toString())
    expect(
      screen.getByRole('button', { name: /Refresh same end/ }),
    ).toBeTruthy()
  })
})
