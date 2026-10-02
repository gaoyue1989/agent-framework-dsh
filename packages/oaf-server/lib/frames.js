/**
 * AF SSE 帧序列化器 —— dsh 事件/活流帧 → agent-framework 前端契约帧（api-frontend-sse.md §9 词表）。
 *
 * 纯函数 + 每 turn 状态机，无 IO；单测钉帧序（设计 R3：以帧表为规范写快照测试）。
 *
 * 映射决策（M0）：
 * - TEXT、THINKING、TOOL_CALL_DELTA 来自 `agent/assistant-stream` 活流帧（§5.1 第 6 步）
 * - AGENT_START、MODEL_CALL、TOOL_CALL（START、END）、TOOL_RESULT、AGENT_END 来自 `session/event` 结算事件
 * - tool/call 结算事件在活流未发过该 callId 时补发 START+END（参数完整、无 DELTA）；
 *   冷恢复/纯回放路径即由此覆盖（AF「HITL 恢复不重放 TOOL_CALL_*」的缺陷在事件溯源模型下不存在）
 * - replyId 派生为 r{turn}；blockId 每 turn 计数 b1..bn（与 AF 帧字段对齐，§4.3）
 *
 * @module @oaf/oaf-server/frames
 */

/** 创建一个 turn 的序列化状态机（每 turn 一个；turn 事件间共享由调用方保证）。 */
export function createTurnSerializer({ sessionId, turn, agentName = 'assistant' }) {
  return {
    sessionId,
    turn,
    replyId: `r${turn}`,
    agentName,
    blockCounter: 0,
    /** 活流块索引 → { kind, blockId, callId? }（block-start 开、block-end 关）。 */
    openBlocks: new Map(),
    /** 已从活流发过 START 的 toolCallId（结算 tool/call 据此去重）。 */
    liveToolCalls: new Set(),
    /** 已发过 TOOL_CALL_END 的 toolCallId（块收口与结算兜底只发一次）。 */
    endedToolCalls: new Set(),
    /** callId → 工具名（tool/call 结算或活流 delta 首帧登记；tool/result 取名用）。 */
    toolNames: new Map(),
  };
}

/** 思维/文本块类型 → AF 帧前缀。 */
function blockPrefix(blockType) {
  if (blockType === 'reasoning') return 'THINKING_BLOCK';
  if (blockType === 'text') return 'TEXT_BLOCK';
  return null; // 其余块类型（toolCall 等）不走 *_BLOCK_* 帧
}

/**
 * 活流帧 → AF 帧（0..n 帧）。frame 为 dsh `agent/assistant-stream` 的 frame。
 */
export function liveStreamFrames(s, frame) {
  if (frame.type !== 'chunk') return [];
  const c = frame.chunk;
  const replyId = s.replyId;
  switch (c.type) {
    case 'block-start': {
      const prefix = blockPrefix(c.blockType);
      if (!prefix) return [];
      const blockId = `b${++s.blockCounter}`;
      s.openBlocks.set(c.index, { kind: c.blockType, blockId });
      return [{ type: `${prefix}_START`, replyId, blockId }];
    }
    case 'text-delta': {
      // 无 block-start 的裸 delta（部分网关形态）：懒开块
      let open = findOpenByKind(s, 'text');
      const frames = [];
      if (!open) {
        const blockId = `b${++s.blockCounter}`;
        open = { kind: 'text', blockId, orphan: true };
        s.openBlocks.set(`text-${blockId}`, open);
        frames.push({ type: 'TEXT_BLOCK_START', replyId, blockId });
      }
      frames.push({ type: 'TEXT_BLOCK_DELTA', delta: c.text, replyId, blockId: open.blockId });
      return frames;
    }
    case 'reasoning-delta': {
      let open = findOpenByKind(s, 'reasoning');
      const frames = [];
      if (!open) {
        const blockId = `b${++s.blockCounter}`;
        open = { kind: 'reasoning', blockId, orphan: true };
        s.openBlocks.set(`reasoning-${blockId}`, open);
        frames.push({ type: 'THINKING_BLOCK_START', replyId, blockId });
      }
      frames.push({ type: 'THINKING_BLOCK_DELTA', delta: c.text, replyId, blockId: open.blockId });
      return frames;
    }
    case 'tool-call-delta': {
      const frames = [];
      if (!s.liveToolCalls.has(c.id)) {
        s.liveToolCalls.add(c.id);
        if (c.name) s.toolNames.set(c.id, c.name);
        frames.push({
          type: 'TOOL_CALL_START',
          toolName: c.name ?? s.toolNames.get(c.id) ?? 'tool',
          toolCallId: c.id,
          replyId,
        });
      } else if (c.name && !s.toolNames.has(c.id)) {
        s.toolNames.set(c.id, c.name);
      }
      // chunk.index 即块索引：登记 toolCall 块，block-end 据此发对应 callId 的 END
      if (!s.openBlocks.has(c.index)) s.openBlocks.set(c.index, { kind: 'toolCall', callId: c.id });
      frames.push({ type: 'TOOL_CALL_DELTA', delta: c.argumentsDelta, toolCallId: c.id, toolCallName: s.toolNames.get(c.id) ?? c.name });
      return frames;
    }
    case 'block-end': {
      const open = s.openBlocks.get(c.index);
      if (!open) return [];
      s.openBlocks.delete(c.index);
      const prefix = blockPrefix(open.kind);
      if (prefix) return [{ type: `${prefix}_END`, replyId, blockId: open.blockId }];
      // toolCall 块收口 → TOOL_CALL_END（同 callId 只发一次）
      const callId = open.callId;
      if (callId && !s.endedToolCalls.has(callId)) {
        s.endedToolCalls.add(callId);
        return [{ type: 'TOOL_CALL_END', toolCallId: callId, toolCallName: s.toolNames.get(callId) }];
      }
      return [];
    }
    default:
      return []; // usage / finish / start 帧不映射（MODEL_CALL_END 由结算 assistant/message 发）
  }
}

function findOpenByKind(s, kind) {
  for (const open of s.openBlocks.values()) if (open.kind === kind) return open;
  return undefined;
}

/**
 * 结算事件 → AF 帧（0..n 帧）。event 为 dsh `session/event` 的 event（{type, seq, time, data}）。
 * 返回的帧不带 seq/id——由调用方（事件镜像）统一编 seq 并补 data.id。
 */
export function settledEventFrames(s, event) {
  const d = event.data ?? {};
  const replyId = s.replyId;
  switch (event.type) {
    case 'turn/start':
      return [{
        type: 'AGENT_START',
        replyId,
        sessionId: s.sessionId,
        name: s.agentName,
        role: 'assistant',
      }];
    case 'step/start':
      return [{ type: 'MODEL_CALL_START', replyId }];
    case 'assistant/message': {
      // usage 随 assistant/message 一起留存（无独立 usage 事件）
      const u = d.usage;
      if (!u) return [];
      return [{
        type: 'MODEL_CALL_END',
        replyId,
        inputTokens: u.inputTokens ?? 0,
        outputTokens: u.outputTokens ?? 0,
        totalTokens: u.totalTokens ?? (u.inputTokens ?? 0) + (u.outputTokens ?? 0),
      }];
    }
    case 'tool/call': {
      s.toolNames.set(d.callId, d.name);
      if (s.liveToolCalls.has(d.callId)) {
        // 活流已发 START；若块收口未发 END（无 block-end 的网关形态）在此兜底一次
        if (!s.endedToolCalls.has(d.callId)) {
          s.endedToolCalls.add(d.callId);
          return [{ type: 'TOOL_CALL_END', toolCallId: d.callId, toolCallName: d.name }];
        }
        return [];
      }
      // 结算兜底路径（冷恢复/无活流）：START+END 成对、参数完整
      s.endedToolCalls.add(d.callId);
      return [
        { type: 'TOOL_CALL_START', toolName: d.name, toolCallId: d.callId, replyId },
        { type: 'TOOL_CALL_END', toolCallId: d.callId, toolCallName: d.name },
      ];
    }
    case 'tool/result': {
      const callId = d.message?.toolCallId;
      const toolName = s.toolNames.get(callId) ?? 'tool';
      const text = textOfBlocks(d.message?.content);
      return [
        { type: 'TOOL_RESULT_START', toolCallId: callId, toolCallName: toolName, replyId },
        ...(text ? [{ type: 'TOOL_RESULT_TEXT_DELTA', delta: text, toolCallId: callId, toolCallName: toolName, replyId }] : []),
        {
          type: 'TOOL_RESULT_END',
          state: d.message?.isError ? 'ERROR' : 'SUCCESS',
          toolCallId: callId,
          toolCallName: toolName,
          replyId,
        },
      ];
    }
    case 'turn/end':
      return [{ type: 'AGENT_END', replyId }];
    default:
      return []; // user/message / request/header / system/message 等不出 AF 帧
  }
}

/** content 块数组 → 文本（text 块拼接；其余块忽略）。 */
export function textOfBlocks(content) {
  if (!Array.isArray(content)) return '';
  return content
    .filter((b) => b?.type === 'text' && typeof b.text === 'string')
    .map((b) => b.text)
    .join('');
}

/**
 * 控制帧（session_created/waiting/done/error/interrupted）——无 seq、不进事件镜像
 * （api-frontend-sse.md §9.9：控制帧没有 id: 行）。
 */
export const controlFrames = {
  sessionCreated: (sessionId) => ({ type: 'session_created', session_id: sessionId }),
  waiting: () => ({ type: 'waiting' }),
  done: () => ({ type: 'done' }),
  error: (message) => ({ type: 'error', error: String(message) }),
  interrupted: () => ({ type: 'interrupted', reason: 'turn_interrupted' }),
};
