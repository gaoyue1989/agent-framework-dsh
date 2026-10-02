# OAF 平行运行时镜像（设计 §7）：pnpm 构建 → node:22-slim 运行时，精确锁版 dsh。
# 平台零改动：同 env 契约、同 Ingress、同 agent-card 注册时序——发布的是「另一种运行时镜像」的 OAF 服务。
#
# 构建：docker build -t agent-framework-dsh:m0 .
# 运行：docker run --rm -p 8100:8100 \
#         -e LLM_API_KEY=... -e LLM_MODEL_ID=... -e LLM_BASE_URL=... \
#         -e CHECKPOINT_JDBC_URL=jdbc:mysql://...:3306/oaf_dsh -e CHECKPOINT_USERNAME=... -e CHECKPOINT_PASSWORD=... \
#         -e SERVER_HOST=0.0.0.0 -e SERVER_PORT=8100 \
#         -v /path/to/oaf-package:/config:ro \
#         agent-framework-dsh:m0

FROM node:22-slim AS build
WORKDIR /app
RUN corepack enable
COPY package.json pnpm-workspace.yaml pnpm-lock.yaml* ./
# 全部 workspace 包的 package.json 先行复制（依赖图完整后再 install，保层缓存；
# 注意 docker COPY glob 不保留子目录结构，必须逐包显式列出）
COPY packages/oaf-common/package.json packages/oaf-common/
COPY packages/oaf-server/package.json packages/oaf-server/
COPY packages/oaf-mysql-persistence/package.json packages/oaf-mysql-persistence/
COPY packages/oaf-loader/package.json packages/oaf-loader/
COPY packages/oaf-tools/package.json packages/oaf-tools/
COPY profiles/oaf-web/package.json profiles/oaf-web/
RUN pnpm install --frozen-lockfile || pnpm install
COPY packages/ packages/
COPY profiles/ profiles/
COPY scripts/ scripts/

FROM node:22-slim
WORKDIR /app
ENV NODE_ENV=production DSH_HOME=/app/.dsh-home
COPY --from=build /app/ /app/
# profiles 必须是软链本身（先 mkdir 成目录会让链接嵌套失效——boot.mjs 亦有自愈，镜像内直接做对）
RUN mkdir -p /app/.dsh-home /config /workspace && ln -s /app/profiles /app/.dsh-home/profiles
# 平台保留键（AGENTS.md 约定）：AGENT_CONFIG_DIR=/config 只读挂载；工作区独立卷
ENV AGENT_CONFIG_DIR=/config AGENT_WORKSPACE_DIR=/workspace
EXPOSE 8100
ENTRYPOINT ["node", "scripts/boot.mjs"]
