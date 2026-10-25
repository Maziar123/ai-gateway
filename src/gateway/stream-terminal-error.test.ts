import { describe, expect, it } from 'vitest';
import {
  buildStreamTerminalErrorFrames,
  createSseTerminalEventDetector,
  guardStreamTerminalError
} from './stream-terminal-error';

const encoder = new TextEncoder();

function createEventStream(chunks: string[], errorAfter?: Error): Response {
  let index = 0;
  const stream = new ReadableStream<Uint8Array>({
    pull(controller) {
      if (index < chunks.length) {
        controller.enqueue(encoder.encode(chunks[index]));
        index += 1;
        return;
      }
      if (errorAfter) {
        controller.error(errorAfter);
        return;
      }
      controller.close();
    }
  });

  return new Response(stream, {
    headers: { 'content-type': 'text/event-stream' },
    status: 200
  });
}

describe('buildStreamTerminalErrorFrames', () => {
  it('emits an Anthropic error event carrying a typed api error', () => {
    const frames = buildStreamTerminalErrorFrames({
      adapterKey: 'anthropic_messages',
      message: 'Upstream stream failed.'
    });

    expect(frames).toHaveLength(1);
    expect(frames[0]).toBe(
      'event: error\ndata: {"error":{"code":"upstream_stream_error","message":"Upstream stream failed.","type":"api_error"},"type":"error"}\n\n'
    );
  });

  it('emits an OpenAI chat error chunk followed by the done sentinel', () => {
    const frames = buildStreamTerminalErrorFrames({
      adapterKey: 'openai_chat',
      message: 'Upstream stream failed.'
    });

    expect(frames).toHaveLength(2);
    expect(frames[0].startsWith('data: {')).toBe(true);
    expect(frames[0]).not.toContain('event:');
    expect(frames[1]).toBe('data: [DONE]\n\n');
  });

  it('emits a failed Responses event followed by the done sentinel', () => {
    const frames = buildStreamTerminalErrorFrames({
      adapterKey: 'openai_responses',
      message: 'Upstream stream failed.'
    });

    expect(frames).toHaveLength(2);
    expect(frames[0].startsWith('event: response.failed\n')).toBe(true);
    expect(frames[0]).toContain('"status":"failed"');
    expect(frames[1]).toBe('data: [DONE]\n\n');
  });

  it('returns no frame for adapters without an in-band error event', () => {
    expect(
      buildStreamTerminalErrorFrames({
        adapterKey: 'gemini_generate_content',
        message: 'Upstream stream failed.'
      })
    ).toEqual([]);
  });
});

describe('createSseTerminalEventDetector', () => {
  it('detects a terminal event split across chunks', () => {
    const detector = createSseTerminalEventDetector();
    detector.push(encoder.encode('event: message_'));
    expect(detector.hasTerminalEvent()).toBe(false);
    detector.push(encoder.encode('stop\ndata: {"type":"message_stop"}\n\n'));

    expect(detector.hasTerminalEvent()).toBe(true);
  });

  it('detects the OpenAI done sentinel', () => {
    const detector = createSseTerminalEventDetector();
    detector.push(encoder.encode('data: [DONE]\n\n'));

    expect(detector.hasTerminalEvent()).toBe(true);
  });

  it('ignores the word error inside streamed text', () => {
    const detector = createSseTerminalEventDetector();
    const delta = JSON.stringify({ choices: [{ delta: { content: '{"type":"error"}' } }] });
    detector.push(encoder.encode(`data: ${delta}\n\n`));

    expect(detector.hasTerminalEvent()).toBe(false);
  });
});

describe('guardStreamTerminalError', () => {
  it('ends a truncated Anthropic stream with one terminal error frame', async () => {
    const guarded = guardStreamTerminalError(
      createEventStream(
        ['event: content_block_delta\ndata: {"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"live"}}\n\n'],
        new Error('terminated')
      ),
      { adapterKey: 'anthropic_messages', message: 'Upstream stream failed.' }
    );

    const body = await guarded.text();

    expect(body).toContain('"type":"thinking_delta"');
    expect(body.endsWith('event: error\ndata: {"error":{"code":"upstream_stream_error","message":"Upstream stream failed.","type":"api_error"},"type":"error"}\n\n')).toBe(true);
  });

  it('never writes a second terminal event', async () => {
    const guarded = guardStreamTerminalError(
      createEventStream(
        ['event: message_stop\ndata: {"type":"message_stop"}\n\n'],
        new Error('terminated')
      ),
      { adapterKey: 'anthropic_messages', message: 'Upstream stream failed.' }
    );

    await expect(guarded.text()).rejects.toThrow('terminated');
  });

  it('stays out of the way of non event-stream responses', () => {
    const response = new Response('{"ok":true}', {
      headers: { 'content-type': 'application/json' }
    });

    expect(guardStreamTerminalError(response, { adapterKey: 'anthropic_messages' })).toBe(response);
  });

  it('stays out of the way of adapters without a terminal error frame', () => {
    const response = createEventStream(['data: {"candidates":[]}\n\n']);

    expect(guardStreamTerminalError(response, { adapterKey: 'gemini_generate_content' })).toBe(
      response
    );
  });

  it('does not mask the failure after the client aborted', async () => {
    const controller = new AbortController();
    controller.abort();
    const guarded = guardStreamTerminalError(
      createEventStream(['data: {"choices":[]}\n\n'], new Error('aborted')),
      { adapterKey: 'openai_chat', message: 'Upstream stream failed.', signal: controller.signal }
    );

    await expect(guarded.text()).rejects.toThrow('aborted');
  });
});
