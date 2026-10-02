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

const child = spawn('./node_modules/.bin/dsh', ['--profile', 'oaf-web'], {
  cwd: ROOT,
  env: { ...process.env, DSH_HOME: home },
  stdio: 'inherit',
});
child.on('exit', (code) => process.exit(code ?? 0));
