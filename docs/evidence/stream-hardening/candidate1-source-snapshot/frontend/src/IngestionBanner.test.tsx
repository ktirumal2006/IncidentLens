import { afterEach, describe, expect, it, vi } from 'vitest'
import { act, render, screen, waitFor } from '@testing-library/react'
import { IngestionBanner } from './IngestionBanner'

const reply = (value: unknown) => new Response(JSON.stringify(value), { status: 200 })
const observed = () => new Date().toISOString()
const sample = (lag: number, at = observed()) => ({ mode: 'streaming', state: lag ? 'backlog' : 'ok', scope: 'consumer_group_lag_and_local_worker_progress', lag_records: lag, partitions_total: 4, partitions_observed: 4, sampled_at: at, sample_started_at: at, sample_finished_at: at, last_write_at: null, last_commit_at: null, processing_delay_ns: null, pending_age_ns: null, processing_state: 'idle' })

afterEach(() => vi.unstubAllGlobals())

describe('ingestion progress banner', () => {
  it('shows observed record backlog and never calls it missing spans', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => reply(sample(12))))
    render(<IngestionBanner />)
    const banner = await screen.findByRole('status')
    await waitFor(() => expect(banner.textContent).toContain('12 records'))
    expect(banner.textContent).toContain('recent admitted traces may not appear')
    expect(banner.textContent).not.toContain('12 spans')
  })

  it('labels zero observed lag without claiming complete visibility', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => reply({ ...sample(0), reason_code: 'empty_partition_uninitialized' })))
    render(<IngestionBanner />)
    await waitFor(() => expect(screen.getByRole('status').textContent).toContain('No broker backlog observed'))
    expect(screen.getByRole('status').textContent).toContain('does not prove all telemetry is queryable')
    expect(screen.getByRole('status').textContent).toContain('no committed consumer starting position')
  })

  it('rejects stale and partial healthy samples and shows worker failure', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => reply(sample(0, new Date(Date.now() - 30_000).toISOString()))))
    const view = render(<IngestionBanner />)
    await waitFor(() => expect(screen.getByRole('status').textContent).toContain('is stale'))
    view.unmount()
    vi.stubGlobal('fetch', vi.fn(async () => reply({ ...sample(0), partitions_observed: 3 })))
    render(<IngestionBanner />)
    await waitFor(() => expect(screen.getByRole('status').textContent).toContain('is unavailable'))
  })

  it('hides direct reference mode and surfaces a failed request', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => reply({ mode: 'direct', state: 'not_applicable' })))
    const view = render(<IngestionBanner />)
    await waitFor(() => expect(globalThis.fetch).toHaveBeenCalled())
    expect(screen.queryByRole('status')).toBeNull()
    view.unmount()
    vi.stubGlobal('fetch', vi.fn(async () => { throw new Error('offline') }))
    render(<IngestionBanner />)
    await waitFor(() => expect(screen.getByRole('status').textContent).toContain('is unavailable'))
  })

  it('shows bounded processing times and recovers from unavailable status', async () => {
    let current: unknown = { mode: 'streaming', state: 'unavailable', lag_records: null, partitions_total: 4, partitions_observed: 0, processing_state: 'retrying' }
    vi.stubGlobal('fetch', vi.fn(async () => reply(current)))
    render(<IngestionBanner />)
    await waitFor(() => expect(screen.getByRole('status').textContent).toContain('is unavailable'))
    current = { ...sample(7), processing_state: 'retrying', processing_delay_ns: '1500000000', pending_age_ns: '2500000000' }
    act(() => document.dispatchEvent(new Event('visibilitychange')))
    await waitFor(() => expect(screen.getByRole('status').textContent).toContain('7 records'))
    expect(screen.getByRole('status').textContent).toContain('Last completed receipt-to-write delay: about 1.5 s')
    expect(screen.getByRole('status').textContent).toContain('Pending work age (worker clock): about 2.5 s')
    expect(screen.getByRole('status').textContent).toContain('Worker retrying')
  })

  it('does not present malformed processing age as a healthy observation', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => reply({ ...sample(0), pending_age_ns: '1e9' })))
    render(<IngestionBanner />)
    await waitFor(() => expect(screen.getByRole('status').textContent).toContain('is unavailable'))
  })
})
