import { useEffect, useState } from 'react'
import { ingestionStatus, type IngestionStatus } from './api'

const unknown: IngestionStatus = { mode: 'streaming', state: 'unavailable', scope: 'consumer_group_lag_and_local_worker_progress', lag_records: null, partitions_total: 0, partitions_observed: 0, sampled_at: null, sample_started_at: null, sample_finished_at: null, last_write_at: null, last_commit_at: null, processing_delay_ns: null, pending_age_ns: null, processing_state: 'idle' }

function spanTime(value: string | null): string | null {
  if (value === null || !/^\d{1,20}$/.test(value)) return null
  const ns = BigInt(value)
  if (ns > 18_446_744_073_709_551_615n) return null
  for (const [unit, size] of [['d', 86_400_000_000_000n], ['h', 3_600_000_000_000n], ['min', 60_000_000_000n], ['s', 1_000_000_000n], ['ms', 1_000_000n], ['µs', 1_000n]] as const) {
    if (ns >= size) return `${ns / size}${ns % size ? `.${((ns % size) * 10n) / size}` : ''} ${unit}`
  }
  return `${ns} ns`
}

function safeStatus(value: IngestionStatus): IngestionStatus {
  if (value.mode === 'direct' && value.state === 'not_applicable') return value
  const complete = value.mode === 'streaming' && value.partitions_total === 4 && value.partitions_observed === 4 && value.sampled_at && value.sample_started_at && value.sample_finished_at && Number.isFinite(Date.parse(value.sampled_at))
  if ((value.state === 'ok' || value.state === 'backlog') && (!complete || value.processing_state === 'failed' || value.lag_records === null || !Number.isSafeInteger(value.lag_records) || value.lag_records < 0 || (value.state === 'ok') !== (value.lag_records === 0))) return unknown
  if (value.processing_delay_ns !== null && spanTime(value.processing_delay_ns) === null) return unknown
  if (value.pending_age_ns !== null && spanTime(value.pending_age_ns) === null) return unknown
  if (value.mode !== 'streaming' || !['ok', 'backlog', 'stale', 'unavailable'].includes(value.state)) return unknown
  return value
}

export function IngestionBanner() {
  const [status, setStatus] = useState<IngestionStatus | null>(null)
  const [now, setNow] = useState(Date.now())

  useEffect(() => {
    let stopped = false
    let controller: AbortController | null = null
    let timer: ReturnType<typeof setTimeout> | undefined
    const poll = async () => {
      if (stopped || document.visibilityState === 'hidden') return
      controller = new AbortController()
      try { setStatus(safeStatus(await ingestionStatus(controller.signal))) }
      catch { if (!controller.signal.aborted) setStatus(unknown) }
      finally { controller = null; if (!stopped) timer = setTimeout(poll, 5000) }
    }
    const visibility = () => {
      if (document.visibilityState === 'hidden') { if (timer) clearTimeout(timer); controller?.abort() }
      else { if (timer) clearTimeout(timer); if (!controller) void poll() }
    }
    document.addEventListener('visibilitychange', visibility)
    void poll()
    const clock = setInterval(() => setNow(Date.now()), 1000)
    return () => { stopped = true; if (timer) clearTimeout(timer); clearInterval(clock); controller?.abort(); document.removeEventListener('visibilitychange', visibility) }
  }, [])

  if (!status || status.state === 'not_applicable') return null
  const stale = status.sampled_at && now - Date.parse(status.sampled_at) > 10000
  const state = stale ? 'stale' : status.processing_state === 'failed' ? 'unavailable' : status.state
  const observed = status.sampled_at && Number.isFinite(Date.parse(status.sampled_at)) ? ` Observed ${status.sampled_at}.` : ''
  let message: string
  if (state === 'backlog') message = `Broker backlog observed (${status.lag_records?.toLocaleString()} records); recent admitted traces may not appear in search or incidents yet.${observed}`
  else if (state === 'ok') message = `No broker backlog observed.${observed} This does not prove all telemetry is queryable.`
  else message = `Streaming delivery status ${state === 'stale' ? 'is stale' : 'is unavailable'}; recent results may be behind.${observed}`
  const delay = state === 'ok' || state === 'backlog' ? spanTime(status.processing_delay_ns) : null
  const pending = state === 'ok' || state === 'backlog' ? spanTime(status.pending_age_ns) : null
  const uninitializedEmpty = (state === 'ok' || state === 'backlog') && status.reason_code === 'empty_partition_uninitialized'
  return <div className={`ingestion-banner ${state}`} role="status" aria-live="polite"><strong>Ingestion status</strong><span>{message}{uninitializedEmpty && ' Empty broker partitions have no committed consumer starting position yet.'}{delay && ` Last completed receipt-to-write delay: about ${delay}.`}{pending && ` Pending work age (worker clock): about ${pending}.`}{status.processing_state === 'retrying' && ' Worker retrying storage or commit.'}</span></div>
}
