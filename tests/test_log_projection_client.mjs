import { build } from 'esbuild';
import assert from 'node:assert/strict';
import { test } from 'node:test';

const built = await build({ entryPoints: ['framework_shells/ui/src/log_projection_client.ts'], bundle: true, write: false, format: 'esm', platform: 'node' });
const client = await import(`data:text/javascript;base64,${Buffer.from(built.outputFiles[0].contents).toString('base64')}`);
globalThis.window = { location: { pathname: '/proxy/app/fws/' } };
const view = { generation: 'g', start: 5, end: 6, total: 6, at_start: false, at_tail: true, pending_bytes: 0, records: [] };

test('window uses proxy prefix and bounded cursor request', async () => {
  globalThis.fetch = async url => {
    const parsed = new URL(url, 'http://example.test');
    assert.equal(parsed.pathname, '/proxy/app/api/framework_shells/logs/shell/window');
    assert.equal(parsed.searchParams.get('count'), '200');
    assert.equal(parsed.searchParams.get('generation'), 'g');
    assert.equal(parsed.searchParams.get('current'), '5');
    assert.equal(parsed.searchParams.get('action'), 'older');
    return Response.json({ ok: true, data: view });
  };
  assert.deepEqual(await client.loadWindow('shell', 'stdout', 'older', view), view);
});

test('stale response resyncs once and does not poll', async () => {
  let requests = 0;
  globalThis.fetch = async url => {
    requests += 1;
    if (requests === 1) return new Response('', { status: 409 });
    assert.equal(new URL(url, 'http://example.test').searchParams.has('generation'), false);
    return Response.json({ ok: true, data: view });
  };
  await client.loadWindow('shell', 'stdout', 'current', view);
  assert.equal(requests, 2);
  globalThis.fetch = async () => new Response('', { status: 409 });
  await assert.rejects(client.loadWindow('shell', 'stdout', 'tail'), /409/);
});

test('raw retrieval requests one bounded page', async () => {
  globalThis.fetch = async url => {
    const params = new URL(url, 'http://example.test').searchParams;
    assert.equal(params.get('limit'), '65536');
    assert.equal(params.get('offset'), '2');
    return Response.json({ ok: true, data: { hex: '696407', next_offset: 5, eof: true } });
  };
  assert.equal((await client.loadRawPage('shell', 'stdout', {generation: 'g', byte_start: 0, byte_end: 5}, 2)).hex, '696407');
});

test('newer refreshes the boundary record before extending the window', async () => {
  globalThis.fetch = async url => {
    const params = new URL(url, 'http://example.test').searchParams;
    assert.equal(params.get('current'), '5');
    return Response.json({ok: true, data: view});
  };
  await client.loadWindow('shell', 'stdout', 'newer', view);
});

function page(start, end, total = 1000, generation = 'g') {
  return {generation, start, end, total, at_start: start === 0, at_tail: end === total, pending_bytes: 0,
    records: Array.from({length: end - start}, (_, offset) => ({text: String(start + offset),
      raw: {generation, byte_start: start + offset, byte_end: start + offset + 1}, omissions: [], diagnostic: null}))};
}

test('sliding windows retain overlap and bound both scroll directions', () => {
  let view = page(800, 1000);
  for (let i = 0; i < 16; i++) {
    const next = client.slideWindow(view, page(view.start - 50, view.start), 'older');
    assert.equal(next.records.length, 200);
    assert.equal(next.start, view.start - 50);
    assert.equal(next.end, view.end - 50);
    assert.deepEqual(next.records.slice(50), view.records.slice(0, 150));
    view = next;
  }
  assert.equal(view.at_start, true);
  for (let i = 0; i < 16; i++) {
    const next = client.slideWindow(view, page(view.end - 1, view.end + 50), 'newer');
    assert.equal(next.records.length, 200);
    assert.equal(next.start, view.start + 50);
    assert.equal(next.end, view.end + 50);
    assert.equal(new Set(next.records.map(r => r.raw.byte_start)).size, 200);
    view = next;
  }
  assert.equal(view.at_tail, true);
});

test('byte-budget shortened slices preserve contiguous history', () => {
  let view = page(90, 100);
  view = client.slideWindow(view, page(87, 90), 'older');
  assert.deepEqual([view.start, view.end, view.records.length], [87, 100, 13]);
  view = client.slideWindow(view, page(99, 102), 'newer');
  assert.deepEqual([view.start, view.end, view.records.length], [87, 102, 15]);
});

test('boundary reread replaces a partial line instead of duplicating it', () => {
  const prior = page(0, 3, 3);
  prior.records[2].text = 'part';
  const fresh = page(2, 4, 4);
  fresh.records[0].text = 'partial\n';
  const next = client.slideWindow(prior, fresh, 'newer');
  assert.deepEqual(next.records.map(r => r.text), ['0', '1', 'partial\n', '3']);
  assert.equal(prior.records[2].text, 'part');
});

test('reset and missing adjacency never combine unrelated windows', () => {
  const prior = page(50, 250);
  const reset = page(0, 2, 2, 'reset');
  assert.equal(client.slideWindow(prior, reset, 'older'), reset);
  const gap = page(300, 350);
  assert.equal(client.slideWindow(prior, gap, 'newer'), gap);
});

test('live snap replaces detached history rather than accumulating it', async () => {
  const prior = page(0, 200);
  globalThis.fetch = async () => Response.json({data: page(9800, 10000, 10000)});
  const next = await client.loadWindow('shell', 'stdout', 'tail', prior);
  assert.deepEqual([next.start, next.end, next.records.length], [9800, 10000, 200]);
  assert.equal(prior.start, 0);
});

test('scroll intent detaches live tail and shifts only in the user direction', () => {
  assert.deepEqual(client.viewportScroll(page(800, 1000), 800, 1500, 500, -100, true, false), {following: false, action: null});
  assert.deepEqual(client.viewportScroll(page(800, 1000), 200, 1500, 500, -100, false, false), {following: false, action: 'older'});
  assert.deepEqual(client.viewportScroll(page(400, 600), 900, 1500, 500, 100, false, false), {following: false, action: 'newer'});
  assert.deepEqual(client.viewportScroll(page(800, 1000), 1000, 1500, 500, 100, false, false), {following: true, action: 'tail'});
});

test('busy requests and stationary/programmatic geometry cannot cascade history shifts', () => {
  assert.equal(client.viewportScroll(page(400, 600), 0, 1500, 500, -100, false, true).action, null);
  assert.equal(client.viewportScroll(page(400, 600), 0, 1500, 500, 0, false, false).action, null);
  assert.equal(client.viewportScroll(page(0, 200), 0, 1500, 500, -100, false, false).action, null);
});
