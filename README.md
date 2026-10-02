# agent-framework-dsh — 基于 DeepSeek Harness 插件重建的 OAF 平行运行时

> 独立仓库（自主仓 [gaoyue1989/agent-manager](https://github.com/gaoyue1989/agent-manager) 的
> `agent-framework-dsh/` 模块迁移而来，2026-10-02）。e2e 的 mock LLM 复用主仓
> `agent-framework/e2e/mock/llm-server.mjs`——请将本仓库与 agent-manager **同层克隆**，
> 或设 `AF_E2E_DIR` 指向其 `agent-framework/e2e` 目录。
>
> 设计依据：[docs/design/deepseek-harness-plugin-runtime-design.md](https://github.com/gaoyue1989/agent-manager/blob/master/docs/design/deepseek-harness-plugin-runtime-design.md)
> 当前状态：**M0 探针已完成并通过全部验收**（2026-10-02，三种形态 e2e 24/24 全绿 + release-agent 已切换 dsh），Java 运行时（agent-framework）不动、继续服役。

以 dsh（`@deepseek-ai/dsh`，**精确锁版 `0.2.0-rc.2`**）微内核为底座，用 TypeScript/Node 插件重建
agent-framework（下称 AF）的对外功能契约。**平台（backend/frontend/K8s）零改动**——发布的是
「另一种运行时镜像」的 OAF 服务。

## M0 交付与验收

设计 §9 M0 口径：profile + webserver + 单 turn 对话 + SSE 三帧 + MySQL 落事件。实际交付超出口径：

| 验收项 | 结果 | 断言位置 |
|---|---|---|
| profile 组合（dsh-base + webserver + 插件族） | ✅ | `make dump-config` |
| 单 turn 对话 + 全帧词表（session_created / AGENT_START / MODEL_CALL_* / TEXT_BLOCK_* / AGENT_END…） | ✅ 24 断言 | `e2e/smoke.mjs` |
| **MySQL 落事件**：dsh 会话日志（session_log，`SessionPersistence` MySQL provider）+ AF 事件镜像（session_event） | ✅ 双层落库 | smoke §3.3 |
| durable SSE：`/subscribe?afterSeq=` 严格续传 + done 收尾；`/status` 五态（M0 覆盖 idle/working/completed/interrupted） | ✅ | smoke §3.4/3.5 |
| **冷恢复**：SIGKILL 重启后同 sessionId 续聊，turn 计数/seq/日志全部续接 | ✅ | smoke §3.8 |
| 多用户隔离最小面：`/threads?userId=` 过滤（C7） | ✅ | smoke §3.6 |
| 帧序列化与 AF 帧表一致（R3 锚：delta 拼接 == 真实 LLM 录制件原文） | ✅ | smoke §3.2 + `test/frames.test.mjs`（11 单测） |

复跑：`make install && make test && make smoke`（冒烟自起 mock LLM，复用
`agent-framework/e2e/mock/llm-server.mjs` 真实 LLM 录制回放件，零密钥）。

## 目录结构

```
agent-framework-dsh/
├── profiles/oaf-web/            # dsh profile：dsh.profile.bundles=[dsh-base] + cordis.patch.yml
│   └── cordis.patch.yml         #   env→配置合成（设计 §7「启动器」）、jsonl 持久化关闭、插件挂载
├── packages/
│   ├── oaf-common/              # JDBC URL 解析 / 连接池 / 轻量版本表迁移器（设计 §6.3）
│   ├── oaf-mysql-persistence/   # ctx.sessionPersistence 的 MySQL provider（设计 §6.1 session_log）
│   └── oaf-server/              # AF 对外面：/threads/chat SSE + subscribe/status + 事件镜像
│       └── lib/frames.js        #   帧序列化器（纯函数，AF api-frontend-sse.md §9 词表）
├── scripts/boot.mjs             # 薄启动器（env 契约 → dsh --profile oaf-web）
├── test/                        # 帧序列化快照单测（node:test，零额外依赖）
├── e2e/smoke.mjs                # M0 冒烟（mock LLM + 运行时编排 + 24 断言）
├── Dockerfile                   # 多阶段：pnpm build → node:22-slim（锁版）
└── Makefile                     # install / dump-config / run / test / smoke
```

## 架构对位（与 AF 同构）

| AF 概念 | 本实现 | 说明 |
|---|---|---|
| `/threads/chat` 单次流 | `oaf-server` exact 路由 → `ctx.agents.create/resume` + `followup` | 协议驱动范式（dsh-acp 同款，设计 §5.1） |
| AF SSE 帧词表 | `frames.js`：`session/event`（结算）+ `agent/assistant-stream`（活流）→ AF 帧 | `id:` 行 = seq 游标；`data.id` = `e{seq}` |
| `session_event` 事件存储 | MySQL `session_event` 表（seq PK，镜像） | AF SessionEventStore 同构；**M2 按设计迁 Redis Streams** |
| `agent_state` 状态 | dsh 会话事件日志 → `dsh_session_header/log` 表（自建 MySQL provider） | 事件溯源：history/回放从日志派生（设计 §4.3） |
| Turn 租约 | 进程内 busy + FIFO 排队 + waiting 帧 | 跨副本 Redis 租约列入 M2（设计 §5.8） |
| `session_user` | MySQL `session_user` 表（userId 过滤 / title） | C7 最小面 |
| Flyway | `@oaf/oaf-common` 轻量版本表迁移器（每插件独立版本表） | 设计 §6.3：只新增版本 / 已合并不可改 / 失败即启动失败 |
| LLM_* 契约 | profile patch：pi-ai `openai-completions` 路由（baseURL/apiKeyEnv） | apiKeyEnv 经 ctx.credentials 按请求解析，密钥不进配置 |

## M0 已声明的边界（诚实清单，后续里程碑补齐）

- `fileIds` 上传物化（AF `UploadWorkspaceInjector` 语义）→ **M1**（现返回 400 `fileIds_not_supported_in_m0`）
- 会话级模型切换 / 托管模型面（model_config 表）→ **M3**（现仅接受系统模型，未知模型 400）
- HITL（hitl-bridge + `/confirm[-stream]`）、A2A、Agent Protocol、MCP 桥、OAF 包加载（oaf-loader）、
  记忆/技能、OpenSandbox、OTel/审计 → **M1–M4**（见设计 §9 路线表）
- `error`/`interrupted` 控制帧不落镜像（AF 同为控制帧无 seq）；`/status` 的 `interrupted` 态由
  「有事件但无活跃租约且末帧非 AGENT_END」推导
- `session/event` 的 `aborted/blocked` 终止原因一律按终态关流（HITL 挂起分流列入 M2）
- 跨副本写者互斥（Redis 租约双层）与多副本 `/subscribe` 对账 → M2（e2e-multi 移植）

## 本地开发

```bash
make install                 # pnpm workspace + dsh-home 软链
make dump-config             # 验证 profile 组合（不启动）
make test                    # 帧序列化单测
make smoke                   # M0 冒烟（需本机 MySQL；默认 docker e2e-mysql:13306/agent_framework_dsh）
make run                     # 启动（需 LLM_* / CHECKPOINT_*，见 scripts/boot.mjs）
```

环境变量同 AF C18 契约：`LLM_API_KEY/LLM_MODEL_ID/LLM_BASE_URL`（必填）、
`CHECKPOINT_JDBC_URL/_USERNAME/_PASSWORD`、`SERVER_HOST/SERVER_PORT`、`AGENT_WORKSPACE_DIR`。

## release-agent 切换验证（2026-10-02，已切换）

`oaf-release-agent`（智能发布助手）已切换为 dsh 运行时：`packages/oaf-loader` 实现 OAF 包最小加载
（AGENTS.md frontmatter → 系统提示词段；`mcp-configs/*` → `dsh-mcp-client` 桥；部署清单
[manifests/oaf-release-agent-dsh.yaml](manifests/oaf-release-agent-dsh.yaml)，同 OAF 包 PVC subPath、
同 env CM+Secret、独立 checkpoint 库）。**切换方式**：`oaf-release-agent-svc` selector 增加
`runtime: dsh`（Java Deployment 原样保留，回滚 = `kubectl patch svc oaf-release-agent-svc -p
'{"spec":{"selector":{"app.kubernetes.io/name":"oaf-release-agent"}}}'`）。

**与 Java 运行时一致（实测对照）**：/health 契约字段与取值（agent/slug/version 读包 frontmatter，
version=1.1.0 双侧一致）、OAF 人设行为（自我介绍/确认工作流约束同样生效）、platform-publisher
MCP 17 工具注册与真实调用（list_services / get_service_status 实测返回真实平台数据）、SSE 帧
词表（session_created/AGENT_START/THINKING/TEXT/TOOL_CALL/TOOL_RESULT/MODEL_CALL/AGENT_END）、
durable SSE 续传、Ingress 全链路（宿主 nginx → /agent/release-agent → svc → dsh Pod）。

**已知差距（M1–M3 补齐，切换前必读）**：
- **HITL 语义差异**：Java 对 ask 工具（publish/update_env/republish/unpublish/delete）发
  `permission_ask` 挂起等待确认卡；dsh M0 无确认桥，经 `tools/pre-execute` 安全降级为 **deny**
  （确定性验证 `make test-ask-deny` 4 断言：拦截为 ERROR + 拒绝原因 + 工具体不执行）。**变更类
  操作在 dsh 实例上当前会被拒绝**，需在 Java 实例（或 M2 hitl-bridge 上线后）完成。
- 工具命名：dsh 向 LLM 暴露 `mcp__{server}__{tool}` 全名（AF 为裸名）——对模型行为无实质影响，
  与 `/tools` 展示契约的差异在 M1 对齐。
- 合成帧（`tool_call_summary`/`tool_result_preview`）、`/tools`、`/skills`、`/mcp`、`/models`、
  `/metadata`、agent-card、`/admin/reload`、fileIds 上传、A2A/Agent Protocol、OTel/审计未实现。

## 部署与 e2e（三种运行时驱动，2026-10-02 全部验证 24/24）

冒烟 e2e（`e2e/smoke.mjs`）按 `OAF_RUNTIME_DRIVER` 切换被测运行时形态，**同一套 24 断言**（含冷恢复）在三种形态下全部通过：

| 驱动 | 被测形态 | 冷恢复语义 | 命令 |
|---|---|---|---|
| `spawn`（默认） | 本地 dsh 进程（开发回归） | 进程 SIGKILL → 重启 | `node e2e/smoke.mjs` |
| `docker` | 多阶段镜像容器（部署制品验证） | `docker rm -f` 强杀 → 重建 | `OAF_RUNTIME_DRIVER=docker node e2e/smoke.mjs` |
| `k8s` | kind 集群 Deployment（C19 平台集成验证） | Pod `--force --grace-period=0` 强杀 → Deployment 重建（真实故障接管路径） | `OAF_RUNTIME_DRIVER=k8s node e2e/smoke.mjs` |

K8s 部署（`manifests/k8s-m0.yaml`）：Deployment + Service(NodePort 30890) + readiness/liveness `/health` 探针，
独立 checkpoint 库（架构原则二：服务间数据隔离）。注意 kind 集群需在创建时映射 30890 宿主端口
（本环境未映射，e2e 经 `kubectl port-forward` 访问；生产 Ingress 接入按平台发布链路走）。

镜像：`docker build -t agent-framework-dsh:m0 .`（多阶段，pnpm 锁版 + dsh 精确版本钉死）；
kind 导入：`kind load docker-image agent-framework-dsh:m0 --name agent-manager`。

e2e 的 LLM 依赖：自起 AF mock LLM（`agent-framework/e2e/mock/llm-server.mjs`，真实 LLM 录制回放零密钥）；
k8s 驱动下集群 Pod 经宿主转发访问 mock（`0.0.0.0:18099 → 127.0.0.1:18081` socat，kind 网关 `172.20.0.1`）。

## dsh 版本风险（设计 R1）

锁版 `0.2.0-rc.2`（设计编制时点为 0.1.7-rc.2，升级由本模块 CI 回归兜底）；插件面收敛在文档化
seam（webserver / agents / session-persistence / session-event / assistant-stream），未依赖内部 API。
M0 实测记录的 seam 行为：

- 类插件 = default export + `static inject`（服务名）/ `static Config`（schemastery）；`[Service.init]`
  仅在类插件形态下被激活期 await，函数插件内手动构造 Service **不会**触发 init
- Service 构造器即服务注册（`super(ctx, name)`），手动 `ctx.xxx =` 会抛 `without provide`
- out-of-tree 插件解析自 profile 目录 node_modules（pnpm workspace 链接即可），包不得缺直接依赖声明
  （严格隔离，无隐式提升）
- 持久化 backend 必须自装 live 路由（`session/event` → 写 handle、`session/disposed` → close），
  这是 jsonl 参照实现的 `install(ctx)` 契约，非框架隐含
