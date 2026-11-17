// Isolated local preview. Never imports the production server or its schedulers.
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { createCustomerAccessHarness } from '../tests/helpers/customerAccessHarness.mjs';
const h = await createCustomerAccessHarness({ port: 4087, actualAdminPages: true });
const cwd = fileURLToPath(new URL('../../frontend/', import.meta.url));
const vite = spawn(
  process.execPath,
  [
    cwd + 'node_modules/vite/bin/vite.js',
    '--host',
    '127.0.0.1',
    '--port',
    '5187',
    '--strictPort',
  ],
  { cwd, env: { ...process.env, VITE_API_URL: h.base }, stdio: 'inherit' }
);
console.log('Isolated Rover preview: http://127.0.0.1:5187');
console.log('Synthetic logins: admin@example.test / bettina@example.test');
console.log('Synthetic password: Preview-Only-2026!');
console.log(
  'All records and authentication are local, generated fixtures. No production connection.'
);
const close = async () => {
  vite.kill('SIGTERM');
  await h.close();
  process.exit(0);
};
process.on('SIGINT', close);
process.on('SIGTERM', close);
vite.on('exit', () => {
  void h.close().then(() => process.exit(0));
});
