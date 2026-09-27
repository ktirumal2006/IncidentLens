import { execFileSync } from 'node:child_process';
import { chromium } from 'playwright';

// This launches one supported external Demo request, verifies its public API
// search/detail path, then follows that same trace in the real browser UI.
const probe = JSON.parse(execFileSync('python3', ['demo_trace.py'], {
  cwd: new URL('.', import.meta.url), encoding: 'utf8', timeout: 90000,
}));
const browser = await chromium.launch({ headless: true });
try {
  const page = await browser.newPage();
  const ui = process.env.INCIDENTLENS_UI_URL ?? 'http://127.0.0.1:18081/';
  await page.goto(ui, { waitUntil: 'networkidle' });
  await page.getByLabel('From (UTC)').fill(probe.from.slice(0, 16));
  await page.getByLabel('To (UTC)').fill(probe.to.slice(0, 16));
  await page.getByLabel('Service').fill('frontend');
  await page.getByRole('button', { name: /Search traces/ }).focus();
  await page.keyboard.press('Enter');
  const result = page.getByRole('button', { name: new RegExp(probe.trace_id) });
  await result.waitFor({ state: 'visible', timeout: 15000 });
  await result.focus();
  await page.keyboard.press('Enter');
  const detail = page.locator('section[aria-label="Trace detail"]');
  await detail.waitFor({ state: 'visible' });
  await detail.getByRole('list', { name: 'Trace waterfall' }).waitFor({ state: 'visible' });
  for (const service of probe.services) {
    await detail.getByText(service, { exact: false }).first().waitFor({ state: 'visible' });
  }
  console.log(JSON.stringify({ result: 'PASS', trace_id: probe.trace_id,
    services: probe.services, api_attribution: probe.attribution,
    browser: 'search submitted and result opened by keyboard; multi-service waterfall visible' }, null, 2));
} finally {
  await browser.close();
}
