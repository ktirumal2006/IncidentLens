import assert from 'node:assert/strict';
import { spawn, execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { resolve, isAbsolute, basename } from 'node:path';
import { chromium } from 'playwright';

const runFile = promisify(execFile);
const root = resolve(new URL('../..', import.meta.url).pathname);
const evidence = process.env.STREAM_STATUS_EVIDENCE_DIR;
const project = process.env.STREAM_TEST_PROJECT;
const files = (process.env.INCIDENTLENS_STREAM_TEST_COMPOSE_FILES ?? '').split(':').filter(Boolean);
const binary = process.env.BENCH_BINARY;
const api = process.env.INCIDENTLENS_API_URL ?? 'http://127.0.0.1:18081';
const ui = process.env.INCIDENTLENS_UI_URL ?? `${api}/`;
assert(evidence && isAbsolute(evidence), 'STREAM_STATUS_EVIDENCE_DIR must be absolute');
assert(binary && isAbsolute(binary), 'BENCH_BINARY must be absolute');
assert(project?.startsWith('incidentlens-retro-hardening-') && /^[a-z0-9-]+$/.test(project), 'dedicated STREAM_TEST_PROJECT required');
assert(files.length >= 2 && files.every(isAbsolute), 'absolute colon-separated compose files required');
await mkdir(evidence);

const commands = [];
const observations = [];
const compose = ['compose', ...files.flatMap(file => ['-f', file]), '-p', project];
const command = async (file, args, timeout = 30000) => {
  const started = new Date().toISOString();
  try {
    const result = await runFile(file, args, { cwd: root, timeout, maxBuffer: 2 << 20, env: { ...process.env, STREAM_TEST_PROJECT: project } });
    commands.push({ file, args, started, finished: new Date().toISOString(), exit_code: 0, stdout: result.stdout, stderr: result.stderr });
    return result;
  } catch (error) {
    commands.push({ file, args, started, finished: new Date().toISOString(), exit_code: error.code, stdout: error.stdout, stderr: error.stderr });
    throw error;
  }
};
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const status = async phase => {
  const at = new Date().toISOString();
  try {
    const response = await fetch(`${api}/api/v1/ingestion-status`, { signal: AbortSignal.timeout(2500) });
    const body = await response.json();
    const row = { phase, at, http_status: response.status, body };
    observations.push(row);
    return row;
  } catch (error) {
    const row = { phase, at, error: String(error) };
    observations.push(row);
    return row;
  }
};
const waitFor = async (predicate, phase, timeout = 30000) => {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const row = await status(phase);
    if (predicate(row)) return row;
    await sleep(1000);
  }
  throw new Error(`${phase} status condition not observed`);
};
const benchArgs = (name, rate, duration, settle, batch) => [
  '--output-dir', resolve(evidence, name), '--seed', `${project}-${basename(evidence)}-${name}`, '--scenario', name,
  '--rate', String(rate), '--duration', `${duration}s`, '--warmup', '0s',
  '--query-rate', '0', '--baseline-traces', '0', '--visibility-every', '0',
  '--batch-traces', String(batch), '--settle', `${settle}s`, '--drain', '30s',
];
const launchBench = (name, rate, duration, settle, batch) => {
  const args = benchArgs(name, rate, duration, settle, batch);
  const started = new Date().toISOString();
  const child = spawn(binary, args, { cwd: root, stdio: ['ignore', 'pipe', 'pipe'] });
  let stdout = '', stderr = '';
  child.stdout.on('data', chunk => { stdout += chunk; });
  child.stderr.on('data', chunk => { stderr += chunk; });
  let timer;
  const done = new Promise((resolveDone, rejectDone) => {
    timer = setTimeout(() => { child.kill('SIGTERM'); rejectDone(new Error(`${name} timed out`)); }, 90000);
    child.once('error', rejectDone);
    child.once('close', code => {
      clearTimeout(timer);
      commands.push({ file: binary, args, started, finished: new Date().toISOString(), exit_code: code, stdout, stderr });
      code === 0 ? resolveDone() : rejectDone(new Error(`${name} exited ${code}: ${stderr}`));
    });
  });
  // Attach a handler immediately; the main flow still awaits the rejecting promise.
  void done.catch(() => {});
  return { child, done, name };
};
const verifyBench = async name => {
  const summary = JSON.parse(await readFile(resolve(evidence, name, 'summary.json'), 'utf8'));
  const rec = summary.reconciliation;
  assert.equal(rec?.verified, true, `${name} reconciliation`);
  assert.equal(rec?.snapshot_verified, true, `${name} storage snapshot`);
  assert.equal(rec?.acked_missing, 0, `${name} ACKed identity loss`);
  assert.equal(rec?.emitted_missing, 0, `${name} emitted identity loss`);
  return summary;
};
const waitForLoadStart = async (name, timeout = 15000) => {
  const events = resolve(evidence, name, 'events.jsonl');
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const content = await readFile(events, 'utf8').catch(error => {
      if (error.code === 'ENOENT') return '';
      throw error;
    });
    if (content.split('\n').some(line => line && (() => {
      try { return JSON.parse(line).kind === 'load_start'; }
      catch { return false; }
    })())) return;
    await sleep(100);
  }
  throw new Error(`${name} load_start not observed`);
};

let browser, page, active, clickhousePaused = false, workerPaused = false;
const result = { status: 'running', started_at: new Date().toISOString(), project, api, ui, evidence, checks: {} };
try {
  active = launchBench('prime', 96, 3, 2, 4);
  await active.done;
  result.checks.prime = await verifyBench('prime');
  const initial = await waitFor(row => row.http_status === 200 && row.body?.state === 'ok' && row.body?.lag_records === 0 && row.body?.partitions_total === 4 && row.body?.partitions_observed === 4 && row.body?.reason_code !== 'empty_partition_uninitialized', 'primed', 25000);
  result.checks.initial_status = initial;
  browser = await chromium.launch({ headless: true });
  page = await browser.newPage();
  await page.goto(ui, { waitUntil: 'domcontentloaded' });
  const banner = page.locator('.ingestion-banner');
  await banner.getByText(/No broker backlog observed/).waitFor({ timeout: 15000 });
  await page.screenshot({ path: resolve(evidence, 'initial.png'), fullPage: true });

  active = launchBench('stalled', 600, 6, 15, 4);
  await waitForLoadStart('stalled');
  await command('docker', [...compose, 'pause', 'clickhouse']);
  clickhousePaused = true;
  const pausedAt = Date.now();
  const positive = await waitFor(row => row.http_status === 200 && row.body?.lag_records > 0 && row.body?.state === 'backlog' && row.body?.pending_age_ns !== null, 'stalled', 20000);
  await sleep(2500);
  const later = await waitFor(row => row.http_status === 200 && row.body?.lag_records > 0 && row.body?.pending_age_ns !== null && BigInt(row.body.pending_age_ns) > BigInt(positive.body.pending_age_ns), 'pending_age_growth', 12000);
  const retry = await waitFor(row => row.body?.processing_state === 'retrying', 'retrying', 12000);
  await banner.getByText(/Broker backlog observed/).waitFor({ timeout: 12000 });
  await page.screenshot({ path: resolve(evidence, 'backlog.png'), fullPage: true });
  result.checks.stall = { positive, later, retry, paused_at: new Date(pausedAt).toISOString() };
  if (Date.now() - pausedAt < 8000) await sleep(8000 - (Date.now() - pausedAt));
  await command('docker', [...compose, 'unpause', 'clickhouse']);
  clickhousePaused = false;
  await active.done;
  result.checks.stalled_bench = await verifyBench('stalled');
  const drained = await waitFor(row => row.http_status === 200 && row.body?.state === 'ok' && row.body?.lag_records === 0 && row.body?.processing_delay_ns !== null, 'drained', 30000);
  await banner.getByText(/No broker backlog observed/).waitFor({ timeout: 12000 });
  assert.match(await banner.innerText(), /Last completed receipt-to-write delay/);
  await page.screenshot({ path: resolve(evidence, 'drained.png'), fullPage: true });
  result.checks.drained = drained;

  await command('docker', [...compose, 'pause', 'stream-worker']);
  workerPaused = true;
  const unavailable = await waitFor(row => row.body?.state === 'unavailable', 'worker_paused', 15000);
  await banner.getByText(/status is unavailable/).waitFor({ timeout: 12000 });
  await page.screenshot({ path: resolve(evidence, 'unavailable.png'), fullPage: true });
  result.checks.unavailable = unavailable;
  await command('docker', [...compose, 'unpause', 'stream-worker']);
  workerPaused = false;
  result.checks.recovered = await waitFor(row => row.body?.state === 'ok' && row.body?.lag_records === 0, 'worker_recovered', 20000);

  // Browser-only stale rendering: freeze a previously successful real response.
  const frozen = result.checks.recovered.body;
  let frozenResponses = 0;
  await page.route(url => url.pathname === '/api/v1/ingestion-status', route => {
    frozenResponses++;
    return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(frozen) });
  });
  await page.reload({ waitUntil: 'domcontentloaded' });
  await banner.getByText(/No broker backlog observed/).waitFor({ timeout: 12000 });
  assert(frozenResponses > 0, 'frozen status response was intercepted');
  await banner.getByText(/status is stale/).waitFor({ timeout: 16000 });
  await page.screenshot({ path: resolve(evidence, 'stale-browser-only.png'), fullPage: true });
  result.checks.browser_only_stale = { source_sampled_at: frozen.sampled_at, observed_at: new Date().toISOString(), frozen_responses: frozenResponses };
  result.status = 'passed';
} catch (error) {
  result.status = 'failed';
  result.error = String(error?.stack ?? error);
  throw error;
} finally {
  if (clickhousePaused) await command('docker', [...compose, 'unpause', 'clickhouse']).catch(error => { result.cleanup_error = String(error); });
  if (workerPaused) await command('docker', [...compose, 'unpause', 'stream-worker']).catch(error => { result.cleanup_error = String(error); });
  if (active?.child?.exitCode === null) active.child.kill('SIGTERM');
  if (browser) await browser.close();
  result.finished_at = new Date().toISOString();
  await writeFile(resolve(evidence, 'status-observations.jsonl'), observations.map(row => JSON.stringify(row)).join('\n') + '\n');
  await writeFile(resolve(evidence, 'commands.json'), JSON.stringify(commands, null, 2) + '\n');
  await writeFile(resolve(evidence, 'result.json'), JSON.stringify(result, null, 2) + '\n');
}
