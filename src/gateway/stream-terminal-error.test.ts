import { describe, expect, it } from 'vitest';
import {
  buildStreamTerminalErrorFrames,
  createSseTerminalEventDetector,
  guardStreamTerminalError
} from './stream-terminal-error';

const encoder = new TextEncoder();

function createEventStream(chunks: string[], errorAfter?: unknown): Response {
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

function transportError(code: string, message = 'transport detail that must not leak'): Error {
  const error = new Error(message);
  (error as Error & { code?: string }).code = code;
  return error;
}

const truncatedThinkingChunk =
  'event: content_block_delta\ndata: {"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"live"}}\n\n';

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

describe('guardStreamTerminalError transport identity', () => {
  it('names a recognised socket code without copying its message', async () => {
    const guarded = guardStreamTerminalError(
      createEventStream([truncatedThinkingChunk], transportError('ECONNRESET')),
      { adapterKey: 'anthropic_messages', message: 'Upstream stream failed.' }
    );

    const body = await guarded.text();

    expect(body.endsWith(
      'event: error\ndata: {"error":{"code":"upstream_stream_error","message":"Upstream stream failed. (transport: ECONNRESET)","type":"api_error"},"type":"error"}\n\n'
    )).toBe(true);
    expect(body).not.toContain('transport detail that must not leak');
  });

  it('finds the code nested in a cause chain', async () => {
    const inner = transportError('UND_ERR_SOCKET', 'socket closed while reading');
    const middle = new Error('fetch failed', { cause: inner });
    const outer = new Error('upstream request failed', { cause: middle });

    const guarded = guardStreamTerminalError(
      createEventStream([truncatedThinkingChunk], outer),
      { adapterKey: 'anthropic_messages', message: 'Upstream stream failed.' }
    );

    const body = await guarded.text();

    expect(body).toContain('"message":"Upstream stream failed. (transport: UND_ERR_SOCKET)"');
    for (const leaked of ['socket closed while reading', 'fetch failed', 'upstream request failed']) {
      expect(body).not.toContain(leaked);
    }
  });

  it('keeps the OpenAI chat framing when the failure is identified', async () => {
    const guarded = guardStreamTerminalError(
      createEventStream(['data: {"choices":[]}\n\n'], transportError('ETIMEDOUT')),
      { adapterKey: 'openai_chat', message: 'Upstream stream failed.' }
    );

    const body = await guarded.text();

    expect(body.endsWith(
      'data: {"error":{"code":"upstream_stream_error","message":"Upstream stream failed. (transport: ETIMEDOUT)","type":"api_error"}}\n\ndata: [DONE]\n\n'
    )).toBe(true);
  });

  it('keeps the failed Responses framing when the failure is identified', async () => {
    const guarded = guardStreamTerminalError(
      createEventStream(['data: {"type":"response.created"}\n\n'], transportError('EPIPE')),
      { adapterKey: 'openai_responses', message: 'Upstream stream failed.' }
    );

    const body = await guarded.text();

    expect(body.endsWith(
      'event: response.failed\ndata: {"response":{"error":{"code":"upstream_stream_error","message":"Upstream stream failed. (transport: EPIPE)","type":"api_error"},"object":"response","status":"failed"},"type":"response.failed"}\n\ndata: [DONE]\n\n'
    )).toBe(true);
  });

  it('falls back to the default message plus the code when none is configured', async () => {
    const guarded = guardStreamTerminalError(
      createEventStream([truncatedThinkingChunk], transportError('EAI_AGAIN')),
      { adapterKey: 'anthropic_messages' }
    );

    const body = await guarded.text();

    expect(body).toContain(
      '"message":"Upstream stream failed before the response completed. (transport: EAI_AGAIN)"'
    );
  });

  it('refuses an unknown code and a private message', async () => {
    const guarded = guardStreamTerminalError(
      createEventStream(
        [truncatedThinkingChunk],
        transportError('ERR_SOMETHING_PRIVATE', 'secret upstream body at https://internal/route')
      ),
      { adapterKey: 'anthropic_messages', message: 'Upstream stream failed.' }
    );

    const body = await guarded.text();

    expect(body.endsWith(
      'event: error\ndata: {"error":{"code":"upstream_stream_error","message":"Upstream stream failed.","type":"api_error"},"type":"error"}\n\n'
    )).toBe(true);
    expect(body).not.toContain('ERR_SOMETHING_PRIVATE');
    expect(body).not.toContain('secret upstream body');
    expect(body).not.toContain('internal/route');
  });

  it('ignores a code carried in the message when the error has none', async () => {
    const guarded = guardStreamTerminalError(
      createEventStream([truncatedThinkingChunk], new Error('read ECONNRESET on socket')),
      { adapterKey: 'anthropic_messages', message: 'Upstream stream failed.' }
    );

    const body = await guarded.text();

    expect(body.endsWith(
      'event: error\ndata: {"error":{"code":"upstream_stream_error","message":"Upstream stream failed.","type":"api_error"},"type":"error"}\n\n'
    )).toBe(true);
    expect(body).not.toContain('(transport:');
  });

  it('survives a circular cause chain without reporting a code', async () => {
    const first = new Error('first');
    const second = new Error('second', { cause: first });
    (first as Error & { cause?: unknown }).cause = second;

    const guarded = guardStreamTerminalError(
      createEventStream([truncatedThinkingChunk], first),
      { adapterKey: 'anthropic_messages', message: 'Upstream stream failed.' }
    );

    const body = await guarded.text();

    expect(body).toContain('"message":"Upstream stream failed."');
    expect(body).not.toContain('(transport:');
  });

  it('accepts a recognised code on the thrown error itself when the cause is unrelated', async () => {
    const failure = transportError('ECONNREFUSED', 'connect ECONNREFUSED 10.0.0.1:443');

    const guarded = guardStreamTerminalError(
      createEventStream([truncatedThinkingChunk], failure),
      { adapterKey: 'anthropic_messages' }
    );

    const body = await guarded.text();

    expect(body).toContain('(transport: ECONNREFUSED)');
    expect(body).not.toContain('10.0.0.1');
  });
});

describe('guardStreamTerminalError frame accounting', () => {
  it('writes no extra frame once a terminal event already arrived', async () => {
    const guarded = guardStreamTerminalError(
      createEventStream(
        ['event: message_stop\ndata: {"type":"message_stop"}\n\n'],
        transportError('ECONNRESET')
      ),
      { adapterKey: 'anthropic_messages', message: 'Upstream stream failed.' }
    );

    await expect(guarded.text()).rejects.toThrow('transport detail that must not leak');
  });

  it('writes no frame after the client aborted, even with a recognised code', async () => {
    const controller = new AbortController();
    controller.abort();
    const guarded = guardStreamTerminalError(
      createEventStream(['data: {"choices":[]}\n\n'], transportError('ECONNRESET')),
      { adapterKey: 'openai_chat', message: 'Upstream stream failed.', signal: controller.signal }
    );

    await expect(guarded.text()).rejects.toThrow('transport detail that must not leak');
  });

  it('keeps the identity passthrough for an unknown adapter', () => {
    const response = createEventStream(['data: {"candidates":[]}\n\n']);

    expect(guardStreamTerminalError(response, { adapterKey: 'gemini_generate_content' })).toBe(
      response
    );
  });
});
