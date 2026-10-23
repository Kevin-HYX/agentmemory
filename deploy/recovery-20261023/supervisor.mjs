import { spawn } from 'node:child_process';
import { setTimeout as delay } from 'node:timers/promises';
import { mkdirSync } from 'node:fs';
import { isResponsiveHealth } from './health-policy.mjs';

process.umask(0o077);
const runtimeDirectory = '/data/runtime-redis-v22';
mkdirSync(runtimeDirectory, { recursive: true, mode: 0o700 });
if (process.env.AGENTMEMORY_STATE_BACKEND !== 'redis' || !process.env.AGENTMEMORY_REDIS_URL || !process.env.AGENTMEMORY_SECRET) {
  throw new Error('部署必须显式提供 Redis 后端及 API 凭据');
}

const children = [];
let stopping = false;
const alive = child => child.exitCode === null && child.signalCode === null;
async function stop(code, reason) {
  if (stopping) return;
  stopping = true;
  console.error(`[supervisor] ${reason}`);
  const deadline = setTimeout(() => {
    for (const child of children) if (alive(child)) child.kill('SIGKILL');
    process.exit(code);
  }, 15000);
  for (const child of [...children].reverse()) {
    if (alive(child)) {
      const done = new Promise(resolve => child.once('exit', resolve));
      child.kill('SIGTERM');
      await Promise.race([done, delay(6000)]);
      if (alive(child)) child.kill('SIGKILL');
    }
  }
  clearTimeout(deadline);
  process.exit(code);
}
function launch(command, args) {
  const child = spawn(command, args, { stdio: 'inherit', cwd: runtimeDirectory });
  children.push(child);
  child.once('error', error => stop(1, `${command} 启动失败：${error.message}`));
  child.once('exit', (code, signal) => {
    if (!stopping) void stop(1, `${command} 意外退出：${signal ?? code}`);
  });
  return child;
}
process.on('SIGTERM', () => void stop(0, '收到停止请求'));
process.on('SIGINT', () => void stop(0, '收到中断请求'));
launch('/usr/local/bin/iii', ['--no-update-check', '-c', '/app/iii-config.yaml']);
await delay(1000);
if (!stopping) launch(process.execPath, ['/app/dist/index.mjs']);
const started = Date.now();
let ready = false;
let failures = 0;
while (!stopping) {
  await delay(10000);
  try {
    const response = await fetch('http://127.0.0.1:3111/agentmemory/health', {
      signal: AbortSignal.timeout(5000),
      headers: { Authorization: `Bearer ${process.env.AGENTMEMORY_SECRET}` },
    });
    if (!isResponsiveHealth(response.status, await response.json())) throw new Error('状态存储或工作连接不健康');
    ready = true;
    failures = 0;
  } catch (error) {
    console.error(`[supervisor] 健康探测失败：${error.message}`);
    if ((ready && ++failures >= 3) || (!ready && Date.now() - started > 300000)) {
      await stop(1, '健康检查连续失败，退出以触发容器重启');
    }
  }
}
