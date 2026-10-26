import { useEffect, useState } from 'react'
import {
  incidents,
  type IncidentEvaluation,
  type IncidentEvidence,
  type IncidentOperation,
  type IncidentStatistics,
} from './api'
import { utcField } from './time'

type EvidenceSelection = { traceId: string; from: string; to: string }
type Props = { onOpenEvidence: (selection: EvidenceSelection) => void }
type Request = {
  end: string
  service?: string
  namespace?: string
  revision: number
}

const minute = 60_000
const defaultEnd = () =>
  new Date(Date.now() - minute).toISOString().slice(0, 19)
const formatTime = (value: string) =>
  value.replace('T', ' ').replace(/Z$/, ' UTC')
const formatRate = (value: number) => `${(value * 100).toFixed(2)}%`
const formatSignedRate = (value: number | null) =>
  value === null
    ? '—'
    : `${value >= 0 ? '+' : ''}${(value * 100).toFixed(2)} pp`
const formatDuration = (value: string | null) => {
  if (value === null) return '—'
  const ns = BigInt(value)
  const negative = ns < 0n
  const abs = negative ? -ns : ns
  const prefix = negative ? '−' : ''
  if (abs >= 1_000_000_000n)
    return `${prefix}${(Number(abs) / 1e9).toFixed(2)} s`
  if (abs >= 1_000_000n) return `${prefix}${(Number(abs) / 1e6).toFixed(2)} ms`
  if (abs >= 1_000n) return `${prefix}${(Number(abs) / 1e3).toFixed(2)} µs`
  return `${prefix}${abs} ns`
}
const identity = (entry: {
  service_namespace: string
  service_name: string
  operation: string
}) =>
  `${entry.service_namespace ? `${entry.service_namespace}/` : ''}${entry.service_name} · ${entry.operation}`
const caveatLabels: Record<string, string> = {
  missing_parent: 'A parent span is missing from the observed trace.',
  missing_root: 'No root span was observed in the trace interval.',
  cycle: 'Observed parent links contain a cycle.',
  source_drops: 'The source reported dropped trace context.',
  unset_status:
    'Some observed spans have UNSET status; this is not evidence of success.',
  no_server_coverage:
    'No SERVER spans cover this operation in one or both windows.',
}
const caveatText = (value: string) =>
  caveatLabels[value] || value.replaceAll('_', ' ')

function Statistics({
  label,
  value,
}: {
  label: string
  value: IncidentStatistics
}) {
  return (
    <div className="incident-stat-column">
      <h4>{label}</h4>
      <dl>
        <dt>SERVER spans</dt>
        <dd>{value.span_count}</dd>
        <dt>p95</dt>
        <dd>
          {formatDuration(value.p95_duration_ns)}
          {value.p95_duration_ns !== null && (
            <small className="exact-ns">{value.p95_duration_ns} ns</small>
          )}
        </dd>
        <dt>ERROR</dt>
        <dd>
          {value.error_count} · {formatRate(value.error_rate)}
        </dd>
        <dt>UNSET</dt>
        <dd>
          {value.unset_count} · {formatRate(value.unset_rate)}
        </dd>
      </dl>
    </div>
  )
}

function Evidence({
  evidence,
  onOpen,
}: {
  evidence: IncidentEvidence[]
  onOpen: Props['onOpenEvidence']
}) {
  if (evidence.length === 0)
    return (
      <p className="incident-muted">
        No supporting current-window trace was selected.
      </p>
    )
  return (
    <div className="evidence-list" aria-label="Supporting trace evidence">
      {evidence.map((item) => (
        <div className="evidence-item" key={`${item.trace_id}/${item.span_id}`}>
          <button
            type="button"
            onClick={() =>
              onOpen({ traceId: item.trace_id, from: item.from, to: item.to })
            }
          >
            <strong>Open trace {item.trace_id}</strong>
            <span>
              {item.status_code === 2
                ? 'ERROR'
                : item.status_code === 1
                  ? 'OK'
                  : 'UNSET'}{' '}
              · {formatDuration(item.duration_ns)} ·{' '}
              {formatTime(item.start_time)}
            </span>
          </button>
          {item.downstream_anomalies.length > 0 && (
            <p>
              Downstream co-occurring anomalies:{' '}
              {item.downstream_anomalies.map(identity).join(', ')}.
              Co-occurrence does not prove causation.
            </p>
          )}
          {item.error_propagation && (
            <p>
              Observed ERROR propagation along a parent chain; causation is
              uncertain.
            </p>
          )}
          {item.caveats.map((caveat, index) => (
            <p key={index}>{caveatText(caveat)}</p>
          ))}
        </div>
      ))}
    </div>
  )
}

function Operation({
  value,
  onOpen,
  showEvidence = false,
}: {
  value: IncidentOperation
  onOpen: Props['onOpenEvidence']
  showEvidence?: boolean
}) {
  return (
    <article className={`incident-operation ${value.state}`}>
      <div className="incident-operation-head">
        <div>
          <p className="incident-identity">{identity(value)}</p>
          <span className={`incident-state ${value.state}`}>
            {value.state === 'candidate'
              ? 'Candidate'
              : value.state === 'normal'
                ? 'Normal'
                : 'Insufficient evidence'}
          </span>
        </div>
        {value.service_rank !== undefined && (
          <span className="service-rank">
            Service rank {value.service_rank}
          </span>
        )}
      </div>
      {value.triggered_rules.length > 0 && (
        <p className="triggered-rules">
          Triggered:{' '}
          {value.triggered_rules
            .map((rule) =>
              rule === 'errors' ? 'error proportion' : 'latency p95',
            )
            .join(' and ')}
        </p>
      )}
      <div className="incident-stats">
        <Statistics label="Baseline" value={value.baseline} />
        <Statistics label="Current" value={value.current} />
      </div>
      <p className="incident-change">
        p95 change: {formatDuration(value.p95_increase_ns)}
        {value.p95_increase_ns !== null && ` (${value.p95_increase_ns} ns)`} ·
        ERROR share change: {formatSignedRate(value.error_rate_increase)}
      </p>
      {value.state === 'insufficient_evidence' && (
        <p className="incident-muted">
          Both windows need the configured minimum SERVER-span sample count
          before the rules can evaluate this operation.
        </p>
      )}
      {value.caveats.length > 0 && (
        <ul className="incident-caveats">
          {value.caveats.map((caveat, index) => (
            <li key={index}>{caveatText(caveat)}</li>
          ))}
        </ul>
      )}
      {showEvidence && <Evidence evidence={value.evidence} onOpen={onOpen} />}
    </article>
  )
}

export function IncidentView({ onOpenEvidence }: Props) {
  const [endInput, setEndInput] = useState(defaultEnd)
  const [serviceInput, setServiceInput] = useState('')
  const [namespaceInput, setNamespaceInput] = useState('')
  const [emptyNamespace, setEmptyNamespace] = useState(false)
  const [request, setRequest] = useState<Request>(() => ({
    end: utcField(endInput),
    revision: 0,
  }))
  const [evaluation, setEvaluation] = useState<IncidentEvaluation | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  useEffect(() => {
    const controller = new AbortController()
    setLoading(true)
    setError('')
    incidents(
      request.end,
      request.service,
      request.namespace,
      controller.signal,
    )
      .then(setEvaluation)
      .catch((cause) => {
        if (!controller.signal.aborted) setError(cause.message)
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false)
      })
    return () => controller.abort()
  }, [request])

  function evaluate(event: React.FormEvent) {
    event.preventDefault()
    try {
      const end = utcField(endInput)
      if (Date.parse(end) > Date.now())
        throw new Error('Evaluation end must not be in the future.')
      setEvaluation(null)
      setRequest((current) => ({
        end,
        service: serviceInput === '' ? undefined : serviceInput,
        namespace: emptyNamespace
          ? ''
          : namespaceInput === ''
            ? undefined
            : namespaceInput,
        revision: current.revision + 1,
      }))
    } catch (cause) {
      setError((cause as Error).message)
    }
  }

  function latest() {
    setEndInput(defaultEnd())
  }
  function refresh() {
    setRequest((current) => ({ ...current, revision: current.revision + 1 }))
  }
  const candidates = evaluation?.candidates || []
  const others =
    evaluation?.operations.filter((item) => item.state !== 'candidate') || []
  const insufficient = others.filter(
    (item) => item.state === 'insufficient_evidence',
  ).length

  return (
    <section className="incident-view" aria-label="Incident investigation">
      <form
        className="search-panel incident-form"
        onSubmit={evaluate}
        aria-label="Incident evaluation"
      >
        <div className="panel-title">
          <h2>Evaluate a window</h2>
          <span>Fixed trace rule · UTC</span>
        </div>
        <div className="incident-fields">
          <label>
            Evaluation end (UTC)
            <input
              aria-label="Evaluation end (UTC)"
              type="datetime-local"
              step="1"
              value={endInput}
              onChange={(event) => setEndInput(event.target.value)}
              required
            />
          </label>
          <label>
            Service
            <input
              aria-label="Incident service"
              value={serviceInput}
              onChange={(event) => setServiceInput(event.target.value)}
              placeholder="All services"
            />
          </label>
          <div className="namespace-field">
            <label>
              Namespace
              <input
                aria-label="Incident namespace"
                value={namespaceInput}
                onChange={(event) => setNamespaceInput(event.target.value)}
                disabled={emptyNamespace}
                placeholder="All namespaces"
              />
            </label>
            <label className="check-label">
              <input
                type="checkbox"
                checked={emptyNamespace}
                onChange={(event) => setEmptyNamespace(event.target.checked)}
              />{' '}
              Empty namespace only
            </label>
          </div>
        </div>
        <div className="form-actions">
          <p>
            Baseline: previous 30 minutes. Current: final 5 minutes. One-minute
            default delay reduces late arrivals, without guaranteeing complete
            telemetry.
          </p>
          <div className="incident-form-buttons">
            <button type="button" className="quiet" onClick={latest}>
              Use latest end
            </button>
            <button className="primary" type="submit">
              Evaluate incidents →
            </button>
          </div>
        </div>
      </form>
      {loading && (
        <div role="status" className="notice">
          Evaluating trace windows…
        </div>
      )}
      {error && (
        <div role="alert" className="notice error">
          {error}{' '}
          <button type="button" onClick={refresh}>
            Retry evaluation
          </button>
        </div>
      )}
      {evaluation && !loading && (
        <div className="incident-results">
          <div className="incident-overview results-card">
            <div className="section-heading">
              <div>
                <p className="eyebrow">WINDOW INVESTIGATION</p>
                <h2>
                  {candidates.length} candidate operation
                  {candidates.length === 1 ? '' : 's'}
                </h2>
              </div>
              <button type="button" className="quiet" onClick={refresh}>
                ↻ Refresh same end
              </button>
            </div>
            <p className="caption">
              Rule {evaluation.rule_version} · observed{' '}
              {formatTime(evaluation.observed_at)} · end{' '}
              {formatTime(evaluation.end)}. Refreshing the same end can change
              results if spans arrive late; a frozen input and configuration
              repeat deterministically.
            </p>
            <div className="incident-windows">
              <div>
                <strong>Baseline</strong>
                <span>
                  {formatTime(evaluation.baseline.from)} to{' '}
                  {formatTime(evaluation.baseline.to)}
                </span>
              </div>
              <div>
                <strong>Current</strong>
                <span>
                  {formatTime(evaluation.current.from)} to{' '}
                  {formatTime(evaluation.current.to)}
                </span>
              </div>
            </div>
            <div className="rule-config">
              <strong>Rule thresholds</strong>
              <span>
                At least {evaluation.config.min_samples} SERVER spans per window
                · p95 ≥ {evaluation.config.latency_ratio_milli / 1000}× baseline
                and +{formatDuration(evaluation.config.latency_delta_ns)} (
                {evaluation.config.latency_delta_ns} ns) · ERROR share ≥{' '}
                {(evaluation.config.error_rate_bps / 100).toFixed(2)}% and +
                {(evaluation.config.error_increase_bps / 100).toFixed(2)} pp
              </span>
            </div>
            <p className="incident-muted">
              Candidates are investigation hypotheses, not root-cause findings.
              Missing spans, unsampled requests and late arrivals can change the
              picture. UNSET status is not success.
            </p>
            {evaluation.caveats.length > 0 && (
              <ul className="incident-caveats">
                {evaluation.caveats.map((caveat, index) => (
                  <li key={index}>{caveatText(caveat)}</li>
                ))}
              </ul>
            )}
          </div>
          {evaluation.operations.length === 0 && (
            <div className="notice warning">
              No SERVER operation groups were observed. There is insufficient
              data to assess service health.
            </div>
          )}
          {evaluation.operations.length > 0 && candidates.length === 0 && (
            <div className="notice subtle">
              {insufficient === evaluation.operations.length
                ? 'Insufficient evidence for every observed operation.'
                : `No candidate among operations with enough samples. ${insufficient} operation${insufficient === 1 ? '' : 's'} still have insufficient evidence.`}
            </div>
          )}
          {candidates.length > 0 && (
            <div className="candidate-list">
              <h2>Ranked candidates</h2>
              <p className="caption">
                Ordered by ERROR-share increase, then p95 increase. Service
                ranks group operations from the same service.
              </p>
              {candidates.map((item) => (
                <Operation
                  key={`${item.service_namespace}/${item.service_name}/${item.operation}`}
                  value={item}
                  onOpen={onOpenEvidence}
                  showEvidence
                />
              ))}
            </div>
          )}
          {others.length > 0 && (
            <details className="other-operations">
              <summary>Other operation evaluations · {others.length}</summary>
              <div className="other-operation-list">
                {others.map((item) => (
                  <Operation
                    key={`${item.service_namespace}/${item.service_name}/${item.operation}`}
                    value={item}
                    onOpen={onOpenEvidence}
                  />
                ))}
              </div>
            </details>
          )}
        </div>
      )}
    </section>
  )
}
