/**
 * 帧序列化器单测——以 agent-framework/docs/api-frontend-sse.md §9 帧词表为规范钉快照
 * （设计 R3：帧序列化回归以 AF 帧表为规范）。
 *
 * 运行：node --test test/
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import {
  createTurnSerializer,
  settledEventFrames,
  liveStreamFrames,
  controlFrames,
  textOfBlocks,
} from '@oaf/oaf-server/frames';

function serializer(turn = 1) {
  return createTurnSerializer({ sessionId: 's1', turn, agentName: 'oaf-dsh-agent' });
}

/** 断言帧序列的类型序列（忽略其余字段）。 */
function assertTypes(actual, expected, message) {
  assert.deepEqual(
    actual.map((f) => f.type),
    expected,
    message,
  );
}

test('turn/start → AGENT_START（AF 生命周期帧字段）', () => {
  const s = serializer(1);
  const frames = settledEventFrames(s, { type: 'turn/start', seq: 0, time: 0, data: { turn: 1 } });
  assert.deepEqual(frames, [{
    type: 'AGENT_START',
    replyId: 'r1',
    sessionId: 's1',
    name: 'oaf-dsh-agent',
    role: 'assistant',
  }]);
});

test('活流文本块：block-start/delta/end → TEXT_BLOCK_START/DELTA/END', () => {
  const s = serializer();
  const frames = [
    ...liveStreamFrames(s, { type: 'chunk', chunk: { type: 'block-start', index: 0, blockType: 'text' } }),
    ...liveStreamFrames(s, { type: 'chunk', chunk: { type: 'text-delta', index: 0, text: '你' } }),
    ...liveStreamFrames(s, { type: 'chunk', chunk: { type: 'text-delta', index: 0, text: '好' } }),
    ...liveStreamFrames(s, { type: 'chunk', chunk: { type: 'block-end', index: 0, block: { type: 'text', text: '你好' } } }),
  ];
  assertTypes(frames, ['TEXT_BLOCK_START', 'TEXT_BLOCK_DELTA', 'TEXT_BLOCK_DELTA', 'TEXT_BLOCK_END']);
  assert.equal(frames[1].delta, '你');
  assert.equal(frames[2].blockId, frames[1].blockId, 'delta 归属同一 blockId');
  assert.match(frames[0].blockId, /^b\d+$/);
});

test('活流思维块 → THINKING_BLOCK_*（可折叠语义同文本）', () => {
  const s = serializer();
  const frames = [
    ...liveStreamFrames(s, { type: 'chunk', chunk: { type: 'block-start', index: 0, blockType: 'reasoning' } }),
    ...liveStreamFrames(s, { type: 'chunk', chunk: { type: 'reasoning-delta', index: 0, text: '想' } }),
    ...liveStreamFrames(s, { type: 'chunk', chunk: { type: 'block-end', index: 0, block: { type: 'reasoning' } } }),
  ];
  assertTypes(frames, ['THINKING_BLOCK_START', 'THINKING_BLOCK_DELTA', 'THINKING_BLOCK_END']);
});

test('活流工具调用：首个 tool-call-delta 发 TOOL_CALL_START，block-end 发 TOOL_CALL_END', () => {
  const s = serializer();
  const frames = [
    ...liveStreamFrames(s, { type: 'chunk', chunk: { type: 'tool-call-delta', index: 1, id: 'call-1', name: 'echo', argumentsDelta: '{"te' } }),
    ...liveStreamFrames(s, { type: 'chunk', chunk: { type: 'tool-call-delta', index: 1, id: 'call-1', argumentsDelta: 'xt":"hi"}' } }),
    ...liveStreamFrames(s, { type: 'chunk', chunk: { type: 'block-end', index: 1, block: { type: 'toolCall' } } }),
  ];
  assertTypes(frames, ['TOOL_CALL_START', 'TOOL_CALL_DELTA', 'TOOL_CALL_DELTA', 'TOOL_CALL_END']);
  assert.equal(frames[0].toolCallId, 'call-1');
  assert.equal(frames[3].toolCallName, 'echo');
});

test('结算 tool/call 在活流已发 START 时只兜底一次 END；无活流时 START+END 成对', () => {
  // 路径 A：活流已发 START（块收口也发了 END）→ 结算事件零帧
  const s1 = serializer();
  liveStreamFrames(s1, { type: 'chunk', chunk: { type: 'tool-call-delta', index: 0, id: 'c1', name: 'echo', argumentsDelta: '{}' } });
  liveStreamFrames(s1, { type: 'chunk', chunk: { type: 'block-end', index: 0, block: {} } });
  assert.deepEqual(settledEventFrames(s1, { type: 'tool/call', seq: 0, time: 0, data: { callId: 'c1', name: 'echo', arguments: '{}' } }), []);

  // 路径 B：无活流（冷恢复）→ 结算兜底 START+END
  const s2 = serializer();
  const frames = settledEventFrames(s2, { type: 'tool/call', seq: 0, time: 0, data: { callId: 'c2', name: 'write_file', arguments: '{"p":1}' } });
  assertTypes(frames, ['TOOL_CALL_START', 'TOOL_CALL_END']);
  assert.equal(frames[0].toolName, 'write_file');
});

test('结算 tool/result → TOOL_RESULT_START/TEXT_DELTA/END（isError → ERROR 态）', () => {
  const s = serializer();
  settledEventFrames(s, { type: 'tool/call', seq: 0, time: 0, data: { callId: 'c1', name: 'echo', arguments: '{}' } });
  const ok = settledEventFrames(s, {
    type: 'tool/result', seq: 0, time: 0,
    data: { message: { role: 'tool', toolCallId: 'c1', content: [{ type: 'text', text: 'done' }] } },
  });
  assertTypes(ok, ['TOOL_RESULT_START', 'TOOL_RESULT_TEXT_DELTA', 'TOOL_RESULT_END']);
  assert.equal(ok[2].state, 'SUCCESS');
  assert.equal(ok[1].delta, 'done');

  const bad = settledEventFrames(s, {
    type: 'tool/result', seq: 0, time: 0,
    data: { message: { role: 'tool', toolCallId: 'c1', isError: true, content: [{ type: 'text', text: 'boom' }] } },
  });
  assert.equal(bad.find((f) => f.type === 'TOOL_RESULT_END').state, 'ERROR');
});

test('assistant/message usage → MODEL_CALL_END（AF token 三字段）', () => {
  const s = serializer();
  const frames = settledEventFrames(s, {
    type: 'assistant/message', seq: 0, time: 0,
    data: { turn: 1, step: 1, message: { role: 'assistant', content: [] }, usage: { inputTokens: 10, outputTokens: 8, totalTokens: 18 } },
  });
  assert.deepEqual(frames, [{ type: 'MODEL_CALL_END', replyId: 'r1', inputTokens: 10, outputTokens: 8, totalTokens: 18 }]);
});

test('turn/end → AGENT_END；非映射事件（user/message 等）零帧', () => {
  const s = serializer();
  assertTypes(settledEventFrames(s, { type: 'turn/end', seq: 0, time: 0, data: { turn: 1, reason: { kind: 'completed' } } }), ['AGENT_END']);
  assert.deepEqual(settledEventFrames(s, { type: 'user/message', seq: 0, time: 0, data: {} }), []);
  assert.deepEqual(settledEventFrames(s, { type: 'request/header', seq: 0, time: 0, data: {} }), []);
});

test('控制帧形状（§9.9：无 id、session_id/waiting/done/error）', () => {
  assert.deepEqual(controlFrames.sessionCreated('sid-1'), { type: 'session_created', session_id: 'sid-1' });
  assert.deepEqual(controlFrames.waiting(), { type: 'waiting' });
  assert.deepEqual(controlFrames.done(), { type: 'done' });
  assert.deepEqual(controlFrames.error('boom'), { type: 'error', error: 'boom' });
});

test('textOfBlocks 只拼 text 块', () => {
  assert.equal(textOfBlocks([{ type: 'text', text: 'a' }, { type: 'image' }, { type: 'text', text: 'b' }]), 'ab');
  assert.equal(textOfBlocks(undefined), '');
});

test('A.1 完整时序快照：普通对话（无工具）帧类型序列与 AF 文档一致', () => {
  const s = serializer();
  const types = [];
  const push = (frames) => types.push(...frames.map((f) => f.type));

  push(settledEventFrames(s, { type: 'turn/start', seq: 1, time: 0, data: { turn: 1 } }));
  push(settledEventFrames(s, { type: 'step/start', seq: 2, time: 0, data: { turn: 1, step: 1 } }));
  push(liveStreamFrames(s, { type: 'chunk', chunk: { type: 'block-start', index: 0, blockType: 'text' } }));
  push(liveStreamFrames(s, { type: 'chunk', chunk: { type: 'text-delta', index: 0, text: '你' } }));
  push(liveStreamFrames(s, { type: 'chunk', chunk: { type: 'text-delta', index: 0, text: '好！' } }));
  push(liveStreamFrames(s, { type: 'chunk', chunk: { type: 'block-end', index: 0, block: {} } }));
  push(settledEventFrames(s, { type: 'assistant/message', seq: 3, time: 0, data: { turn: 1, step: 1, message: { role: 'assistant', content: [] }, usage: { inputTokens: 10, outputTokens: 8, totalTokens: 18 } } }));
  push(settledEventFrames(s, { type: 'turn/end', seq: 4, time: 0, data: { turn: 1, reason: { kind: 'completed' } } }));

  assert.deepEqual(types, [
    'AGENT_START',
    'MODEL_CALL_START',
    'TEXT_BLOCK_START',
    'TEXT_BLOCK_DELTA',
    'TEXT_BLOCK_DELTA',
    'TEXT_BLOCK_END',
    'MODEL_CALL_END',
    'AGENT_END',
  ]);
});
