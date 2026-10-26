import { describe, expect, it } from 'vitest'
import { millisecondsToNs, timestampNs, utcField } from './time'

describe('precision at the API boundary', () => {
  it('orders whole and fractional seconds at nanosecond resolution', () => {
    const first = timestampNs('2026-09-26T11:59:00Z')
    const next = timestampNs('2026-09-26T11:59:00.000000001Z')
    expect(next - first).toBe(1n)
    expect(timestampNs('2026-09-26T11:59:00.1Z') - first).toBe(100_000_000n)
  })

  it('converts decimal milliseconds exactly and rejects sub-nanosecond values', () => {
    expect(millisecondsToNs('0.000001')).toBe('1')
    expect(millisecondsToNs('18446744073709.551615')).toBe('18446744073709551615')
    expect(() => millisecondsToNs('0.0000001')).toThrow(/precision/)
    expect(() => millisecondsToNs('0')).toThrow(/between/)
  })

  it('treats the input clock as UTC regardless of browser timezone', () => {
    expect(utcField('2026-09-26T12:34')).toBe('2026-09-26T12:34:00.000Z')
  })
})
