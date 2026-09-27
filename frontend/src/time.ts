const maxNs = (1n << 64n) - 1n

// The query API emits UTC RFC3339 timestamps with at most nine fractional digits.
export function timestampNs(value: string): bigint {
  const match = /^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?Z$/.exec(value)
  if (!match) throw new Error(`Invalid API timestamp: ${value}`)
  const seconds = Date.parse(`${match[1]}Z`)
  if (!Number.isFinite(seconds)) throw new Error(`Invalid API timestamp: ${value}`)
  return BigInt(seconds) * 1_000_000n + BigInt((match[2] || '').padEnd(9, '0') || '0')
}

export function millisecondsToNs(value: string): string {
  if (!/^(?:\d+)(?:\.\d+)?$/.test(value)) throw new Error('Duration must be a positive decimal number of milliseconds.')
  const [whole, fraction = ''] = value.split('.')
  if (fraction.length > 6 && /[1-9]/.test(fraction.slice(6))) throw new Error('Duration precision cannot be finer than one nanosecond.')
  const ns = BigInt(whole) * 1_000_000n + BigInt(fraction.slice(0, 6).padEnd(6, '0') || '0')
  if (ns < 1n || ns > maxNs) throw new Error('Duration must be between 1 and 18446744073709551615 nanoseconds.')
  return ns.toString()
}

export function utcField(value: string): string {
  return new Date(`${value}Z`).toISOString()
}
