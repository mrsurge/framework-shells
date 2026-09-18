import {build} from 'esbuild';
import assert from 'node:assert/strict';
import {test} from 'node:test';

const built = await build({entryPoints: ['framework_shells/ui/src/log_pane_layout.ts'], bundle: true, write: false, format: 'esm', platform: 'node'});
const layout = await import(`data:text/javascript;base64,${Buffer.from(built.outputFiles[0].contents).toString('base64')}`);

test('first use opens only stdout', () => {
  assert.deepEqual(layout.readPaneState(null), {collapsed: {stdin: true, stdout: false, stderr: true}, sizes: {}});
});

test('collapse and proportions round trip by expanded combination', () => {
  const saved = {collapsed: {stdin: false, stdout: true, stderr: false}, sizes: {'stdin,stderr': [25, 75], 'stdout,stderr': [60, 40]}};
  assert.deepEqual(layout.readPaneState(JSON.stringify(saved)), saved);
});

test('bad storage cannot create invalid splitter geometry', () => {
  assert.equal(layout.readPaneState('bad json').collapsed.stdout, false);
  const parsed = layout.readPaneState(JSON.stringify({collapsed: {stdout: 'true'}, sizes: {
    'stdout,stderr': [0, 3], 'stdin,stdout': [1, '2'], 'bad': [3], 'stdin,stdin': [1, 1], 'stderr': [20],
  }}));
  assert.equal(parsed.collapsed.stdout, false);
  assert.deepEqual(parsed.sizes, {'stderr': [20]});
});

test('resize preserves pair total and leaves the other pane alone', () => {
  const original = [200, 300, 400];
  assert.deepEqual(layout.resizePair(original, 0, 40), [240, 260, 400]);
  assert.deepEqual(original, [200, 300, 400]);
  assert.deepEqual(layout.resizePair(original, 1, -40), [200, 260, 440]);
});

test('resize cannot collapse a pane, including small viewports', () => {
  assert.deepEqual(layout.resizePair([200, 300], 0, 5000), [400, 100]);
  assert.deepEqual(layout.resizePair([20, 20], 0, -5000), [10, 30]);
  assert.deepEqual(layout.resizePair([200, 300], 0, NaN), [200, 300]);
});
