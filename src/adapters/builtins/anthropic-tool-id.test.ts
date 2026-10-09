import { expect, it } from 'vitest';
import { anthropicToolId } from './anthropic-tool-id';
import { formatAnthropicMessagesResponse } from './source/formatters';
import { anthropicMessagesTargetAdapter } from './target/anthropic-messages';
import { anthropicMessagesSourceAdapter } from './source/anthropic-messages';
import { createOptimisticOpenAIChatStreamRelay, relayOptimisticOpenAIChatStreamToolCalls } from '../../gateway/streaming-conversion';

it('preserves valid IDs and maps distinct invalid IDs deterministically', () => {
  expect(anthropicToolId('toolu_valid-1')).toBe('toolu_valid-1');
  expect(anthropicToolId('Bash:0')).toMatch(/^[A-Za-z0-9_-]+$/);
  expect(anthropicToolId('Bash:0')).not.toBe(anthropicToolId('Bash/0'));
  expect(anthropicToolId('Bash:0')).toBe(anthropicToolId('Bash:0'));
});

it('repairs old poisoned Anthropic histories on the native passthrough path without mutating signed blocks', () => {
  const thinking = { type: 'thinking', thinking: 'opaque', signature: 'keep-exactly' };
  const body = { model: 'native', messages: [
    { role: 'assistant', content: [thinking, { type: 'tool_use', id: 'functions.Bash:0', name: 'Bash', input: {} }] },
    { role: 'user', content: [{ type: 'tool_result', tool_use_id: 'functions.Bash:0', content: 'ok', is_error: false }] }
  ] };
  const built = anthropicMessagesSourceAdapter.buildPassthroughRequest({ body, request: { headers: {} }, config: { anthropicApiKey: 'test', anthropicBaseUrl: 'https://example.test' } } as any);
  expect(built.ok).toBe(true);
  if (!built.ok) return;
  const messages = (built.value.body as any).messages;
  expect(messages[0].content[0]).toEqual(thinking);
  expect(messages[0].content[1].id).toMatch(/^[A-Za-z0-9_-]+$/);
  expect(messages[1].content[0].tool_use_id).toBe(messages[0].content[1].id);
  expect(body.messages[0].content[1]).toHaveProperty('id', 'functions.Bash:0');
});

it('sanitizes JSON and streamed Anthropic tool calls and pairs replayed results', () => {
  const call = { type: 'function_call', id: 'Bash:0', call_id: 'Bash:0', name: 'Bash', arguments: '{}', status: 'completed' } as const;
  const response = { id: 'message', model: 'model', object: 'response', status: 'completed', output: [call], usage: { input_tokens: 1, output_tokens: 1, total_tokens: 2 } } as any;
  const formatted = formatAnthropicMessagesResponse(response) as any;
  const id = formatted.content[0].id;
  expect(id).toBe(anthropicToolId('Bash:0'));
  const relay = createOptimisticOpenAIChatStreamRelay({ adapterKey: 'anthropic_messages' } as any, { model: 'model', input: 'hello' });
  expect(relay).toBeTruthy();
  const frames = relayOptimisticOpenAIChatStreamToolCalls(relay!, [call]).join('');
  expect(frames).toContain(`"id":"${id}"`);
  expect(frames).not.toContain('Bash:0');
  const built = anthropicMessagesTargetAdapter.buildRequestFromStandard({
    request: { headers: {} } as any,
    standardRequest: { model: 'model', input: [
      { type: 'message', role: 'assistant', content: [{ type: 'tool_use', id: 'Bash:0', name: 'Bash', input: {} }] },
      { type: 'message', role: 'user', content: [{ type: 'tool_result', tool_use_id: 'Bash:0', content: 'ok' }] }
    ] },
    config: { anthropicApiKey: 'test', anthropicBaseUrl: 'https://example.test' } as any
  });
  expect(built.ok).toBe(true);
  if (built.ok) {
    const body = built.value.body as any;
    expect(body.messages[0].content[0].id).toBe(id);
    expect(body.messages[1].content[0].tool_use_id).toBe(id);
  }
});
