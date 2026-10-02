#!/bin/bash
# AF e2e 套件对 dsh 运行时的完整编排（对齐 agent-framework/e2e env-up 形态）：
# mock LLM + bench/approval MCP + dsh oaf-web（AF e2e agent-config fixture）→ playwright api-core
set -e
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
AF="$(cd "$ROOT/.." && pwd)/agent-framework/e2e"
[ -d "$AF" ] || { echo "需要 agent-framework 仓库同层克隆（$AF）"; exit 1; }
BENCH_MCP_PORT=18082 APPROVAL_MCP_PORT=8813 MOCK_LLM_PORT=18081
cleanup() { for f in /tmp/af-e2e-*.pid; do kill -9 "$(cat $f)" 2>/dev/null; rm -f $f; done; [ -n "$RT" ] && kill -9 $RT 2>/dev/null; }
trap cleanup EXIT
MOCK_LLM_PORT=$MOCK_LLM_PORT BENCH_MCP_PORT=$BENCH_MCP_PORT node "$AF/mock/llm-server.mjs" > /tmp/af-e2e-llm.log 2>&1 & echo $! > /tmp/af-e2e-llm.pid
MOCK_MCP_PORT=$BENCH_MCP_PORT node "$AF/../bench/mock-mcp/server.js" > /tmp/af-e2e-bench.log 2>&1 & echo $! > /tmp/af-e2e-bench.pid
APPROVAL_MCP_PORT=$APPROVAL_MCP_PORT python3 "$AF/mock/approval-mcp.py" > /tmp/af-e2e-approval.log 2>&1 & echo $! > /tmp/af-e2e-approval.pid
sleep 2
rm -rf /tmp/af-e2e-agent-config && cp -r "$AF/fixtures/agent-config" /tmp/af-e2e-agent-config
sed -i "s|http://172.17.0.1:18082/mcp|http://127.0.0.1:${BENCH_MCP_PORT}/mcp|g" /tmp/af-e2e-agent-config/mcp-configs/*/config.yaml
for i in $(seq 1 40); do curl -s -m 1 http://127.0.0.1:${BENCH_MCP_PORT}/health >/dev/null 2>&1 && break; sleep 0.5; done
cd "$ROOT"
DSH_HOME="$ROOT/.dsh-home" LLM_API_KEY=mock-key LLM_MODEL_ID=gpt-mock LLM_BASE_URL=http://127.0.0.1:${MOCK_LLM_PORT}/v1 \
  SERVER_HOST=127.0.0.1 SERVER_PORT=8100 AGENT_CONFIG_DIR=/tmp/af-e2e-agent-config OAF_FILES_DIR=/tmp/af-e2e-files \
  FILE_EXTERNAL_URL_PREFIXES=http://127.0.0.1:${BENCH_MCP_PORT} \
  CHECKPOINT_JDBC_URL="${CHECKPOINT_JDBC_URL:-jdbc:mysql://127.0.0.1:13306/agent_framework_dsh}" \
  CHECKPOINT_USERNAME="${CHECKPOINT_USERNAME:-e2e}" CHECKPOINT_PASSWORD="${CHECKPOINT_PASSWORD:-e2e-pass}" \
  ./node_modules/.bin/dsh --profile oaf-web > /tmp/af-e2e-runtime.log 2>&1 &
RT=$!
for i in $(seq 1 60); do curl -s -m 1 http://127.0.0.1:8100/health >/dev/null 2>&1 && break; sleep 1; done
cd "$AF"
E2E_BASE=http://127.0.0.1:8100 npx playwright test --project=api-core "$@"
