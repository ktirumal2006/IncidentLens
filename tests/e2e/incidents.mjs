import { mkdirSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { chromium } from 'playwright';

if (!process.argv[2]) throw new Error('usage: node incidents.mjs <passed-phase3-evidence.json>');
const evidence = JSON.parse(readFileSync(resolve(process.argv[2]), 'utf8'));
if (evidence.status !== 'passed') throw new Error(`Phase 3 scenario must pass API evaluation first; status=${evidence.status}`);
const api = process.env.INCIDENTLENS_API_URL ?? 'http://127.0.0.1:18081';
const ui = process.env.INCIDENTLENS_UI_URL ?? 'http://127.0.0.1:18081/';
const browser = await chromium.launch({ headless: true });
const results = [];
const screenshotDirectory = new URL('../../docs/evidence/', import.meta.url);
mkdirSync(screenshotDirectory, { recursive: true });
const pageErrors = [];
async function capture(page, phase, view) {
  for (const [layout, viewport] of [['desktop', { width: 1440, height: 1000 }], ['mobile', { width: 390, height: 844 }]]) {
    await page.setViewportSize(viewport);
    await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
    const overflow = await page.evaluate(() => document.documentElement.scrollWidth > document.documentElement.clientWidth);
    if (overflow) throw new Error(`${phase}/${view}/${layout}: horizontal page overflow`);
    await page.screenshot({ path: new URL(`phase3-${phase}-${view}-${layout}.png`, screenshotDirectory).pathname, fullPage: true });
  }
  await page.setViewportSize({ width: 1440, height: 1000 });
}
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  page.on('pageerror', error => pageErrors.push(error.message));
  await page.goto(ui, { waitUntil: 'networkidle' });
  for (const name of ['healthy_control', 'latency_fault', 'latency_recovery', 'error_fault', 'error_recovery']) {
    const phase = evidence.phases.find(p => p.name === name);
    // The UI accepts UTC seconds. No request occurs near the recorded T (the
    // runner waits ten seconds for export), so this subsecond rounding preserves
    // the same real source-span windows and is verified with the public API.
    const end = phase.evaluation_end.replace(/\.\d+Z$/, 'Z');
    const query = new URLSearchParams({ end, namespace: evidence.namespace });
    const response = await fetch(`${api}/api/v1/incidents?${query}`);
    if (!response.ok) throw new Error(`${name}: API HTTP ${response.status}`);
    const evaluated = await response.json();
    const expected = evidence.expected.find(p => p.phase === name);
    const target = expected.candidate_service === null ? null : evaluated.candidates.find(c =>
      c.service_name === expected.candidate_service && c.triggered_rules.includes(expected.triggered_rule) && c.service_rank <= 3);
    if (expected.candidate_service !== null && !target) throw new Error(`${name}: expected target missing at UI end`);
    if (expected.candidate_service === null && evaluated.candidates.length) throw new Error(`${name}: control has candidates at UI end`);
    await page.getByRole('button', { name: 'Incidents', exact: true }).click();
    await page.getByLabel('Evaluation end (UTC)').fill(end.slice(0, -1));
    await page.getByLabel('Incident namespace').fill(evidence.namespace);
    await page.getByLabel('Incident service').fill('');
    await page.getByRole('button', { name: /Evaluate incidents/ }).focus();
    const rendered = page.waitForResponse(r => r.url().includes('/api/v1/incidents?') && r.url().includes(encodeURIComponent(evidence.namespace)) && r.status() === 200);
    await page.keyboard.press('Enter');
    await rendered;
    const investigation = page.getByRole('region', { name: 'Incident investigation' });
    await investigation.getByText(/At least 100 SERVER spans per window/).waitFor({ state: 'visible' });
    await investigation.getByText(/Candidates are investigation hypotheses, not root-cause findings/).waitFor({ state: 'visible' });
    if (target) {
      const identity = `${target.service_namespace ? `${target.service_namespace}/` : ''}${target.service_name} · ${target.operation}`;
      const card = investigation.locator('.incident-operation.candidate').filter({
        has: page.locator('.incident-identity').filter({ hasText: identity }),
      });
      await card.getByText(`Service rank ${target.service_rank}`, { exact: true }).waitFor({ state: 'visible' });
      await card.getByText(expected.triggered_rule === 'latency' ? /Triggered: latency p95/ : /Triggered: error proportion/).waitFor({ state: 'visible' });
      await card.getByText('Baseline', { exact: true }).waitFor({ state: 'visible' });
      await card.getByText('Current', { exact: true }).waitFor({ state: 'visible' });
      if (name === 'latency_fault') await capture(page, name, 'candidate');
      const known = new Set(phase.requests.map(r => r.trace_id));
      const traceId = target.evidence[0].trace_id;
      if (!known.has(traceId)) throw new Error(`${name}: trace evidence is not an emitted Demo request`);
      await card.getByRole('button', { name: new RegExp(`Open trace ${traceId}`) }).focus();
      await page.keyboard.press('Enter');
      const detail = page.getByRole('region', { name: 'Trace detail' });
      await detail.getByRole('heading', { name: traceId, exact: true }).waitFor({ state: 'visible' });
      await detail.getByRole('list', { name: 'Trace waterfall' }).waitFor({ state: 'visible' });
      await detail.getByText(target.service_name, { exact: false }).first().waitFor({ state: 'visible' });
      if (name === 'latency_fault') await capture(page, name, 'waterfall');
      results.push({ phase: name, service: target.service_name, rank: target.service_rank, trace_id: traceId, keyboard_evidence_open: true });
    } else {
      await investigation.getByText(/No candidate among operations with enough samples/).waitFor({ state: 'visible' });
      results.push({ phase: name, candidates: 0 });
    }
  }
  if (pageErrors.length) throw new Error(`browser page errors: ${pageErrors.join('; ')}`);
  console.log(JSON.stringify({ result: 'PASS', namespace: evidence.namespace, checks: results }, null, 2));
} finally {
  await browser.close();
}
