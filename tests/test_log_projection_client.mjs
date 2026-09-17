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
    assert.equal(parsed.searchParams.get('count'), '1000');
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

test('newer starts at the returned end, not the requested window size', async () => {
  globalThis.fetch = async url => {
    const params = new URL(url, 'http://example.test').searchParams;
    assert.equal(params.get('current'), '6');
    return Response.json({ok: true, data: view});
  };
  await client.loadWindow('shell', 'stdout', 'newer', view);
});
