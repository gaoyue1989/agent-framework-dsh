#!/usr/bin/env node
/**
 * 本地启动器（设计 §7「启动器」职责的薄封装）：准备 DSH_HOME（profiles 软链）
 * 并透传环境变量启动 dsh oaf-web。
 *
 * 环境变量（AF C18 契约）：
 *   LLM_API_KEY / LLM_MODEL_ID / LLM_BASE_URL   系统模型（必填）
 *   SERVER_HOST / SERVER_PORT                    监听面（默认 127.0.0.1:8100）
 *   CHECKPOINT_JDBC_URL / _USERNAME / _PASSWORD  MySQL（session_log + 事件镜像）
 *
 * 用法：node scripts/boot.mjs   （或 make run）
 */
import { spawn } from 'node:child_process';
import { mkdirSync, symlinkSync, existsSync, lstatSync, rmSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.dirname(fileURLToPath(import.meta.url)) + '/..';
const home = path.join(ROOT, process.env.DSH_HOME_DIR ?? '.dsh-home');

// DSH_HOME/profiles 必须是软链本身（不能先 mkdir 成目录，否则链接嵌套失效）；
// 兼容历史坏状态：同名目录（非软链）先移除再重建
mkdirSync(home, { recursive: true });
const link = path.join(home, 'profiles');
if (existsSync(link) && !lstatSync(link).isSymbolicLink()) {
  rmSync(link, { recursive: true, force: true });
}
if (!existsSync(link)) symlinkSync(path.join(ROOT, 'profiles'), link, 'dir');

if (!process.env.LLM_API_KEY || !process.env.LLM_MODEL_ID || !process.env.LLM_BASE_URL) {
  console.error('boot: 缺少 LLM_API_KEY / LLM_MODEL_ID / LLM_BASE_URL（AF 环境变量契约）');
  process.exit(1);
}

// 独立进程组启动：dsh 及其子进程与 boot 隔离，终止时整组回收（issue dsh#4）
const child = spawn('./node_modules/.bin/dsh', ['--profile', 'oaf-web'], {
  cwd: ROOT,
  env: { ...process.env, DSH_HOME: home },
  stdio: 'inherit',
  detached: true,
});

// 终止信号转发（issue dsh#4）：boot 被 SIGTERM/SIGINT 时向子进程组转发，
// 避免 boot 退出后 dsh 成孤儿继续占端口；cordis 优雅关停可能挂住，8s 后整组 SIGKILL 兜底
let shuttingDown = false;
const shutdown = (signal) => {
  if (shuttingDown || child.pid === undefined) return;
  shuttingDown = true;
  try {
    process.kill(-child.pid, signal);
  } catch {
    try { child.kill(signal); } catch { /* 进程组已不存在 */ }
  }
  setTimeout(() => {
    try { process.kill(-child.pid, 'SIGKILL'); } catch { /* 已退出 */ }
  }, 8000).unref?.();
};
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
child.on('exit', (code) => process.exit(code ?? 0));
