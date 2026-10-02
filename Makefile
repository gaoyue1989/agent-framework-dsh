# OAF 平行运行时（agent-framework-dsh）——基于 dsh 插件体系，M0 探针
#
# 常用目标：make install / make dump-config / make run / make test / make smoke

SHELL := /bin/bash
DSH_HOME_DIR ?= .dsh-home

export DSH_HOME = $(CURDIR)/$(DSH_HOME_DIR)

.PHONY: install dump-config run test test-ask-deny smoke clean

install:
	pnpm install
	mkdir -p $(DSH_HOME_DIR)
	rm -rf $(DSH_HOME_DIR)/profiles
	ln -s $(CURDIR)/profiles $(DSH_HOME_DIR)/profiles

# 打印合成后的插件树（组合验证，不启动）
dump-config:
	./node_modules/.bin/dsh --profile oaf-web --dump-config

# 本地启动（需 LLM_* / CHECKPOINT_* 环境变量，见 scripts/boot.mjs 头注）
run:
	node scripts/boot.mjs

test:
	node --test "test/*.test.mjs"

# M0 冒烟 e2e：自起 mock LLM（复用 agent-framework 录制回放件）+ 运行时 + 24 项断言
smoke:
	node e2e/smoke.mjs

# ask 工具安全降级确定性验证（假 MCP server + 自制回放夹具，本地零集群依赖）
test-ask-deny:
	node e2e/ask-deny.test.mjs

clean:
	rm -rf $(DSH_HOME_DIR)/logs $(DSH_HOME_DIR)/storage
