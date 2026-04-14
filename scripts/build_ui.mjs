import esbuild from 'esbuild';

await esbuild.build({
  entryPoints: ['framework_shells/ui/src/fws.ts'],
  bundle: true,
  format: 'iife',
  platform: 'browser',
  target: ['es2020'],
  outfile: 'framework_shells/ui/fws.js',
  logLevel: 'info',
});
