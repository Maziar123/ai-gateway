import { describe, expect, it } from 'vitest';
import {
  createOptimisticOpenAIChatStreamRelay,
  relayOptimisticOpenAIChatStreamTurn,
  type OptimisticOpenAIChatStreamTurnResult
} from './streaming-conversion';

const encoder = new TextEncoder();

function createHangingSseResponse(frames: string[]): Response {
  return new Response(
    new ReadableStream<Uint8Array>({
      start(controller) {
        for (const frame of frames) {
          controller.enqueue(encoder.encode(frame));
        }
      }
    }),
    {
      headers: {
        'content-type': 'text/event-stream'
      }
    }
  );
}

function chatChunk(
  delta: Record<string, unknown>,
  finishReason: string | null = null,
  usage?: Record<string, unknown>
): string {
  return `data: ${JSON.stringify({
    id: 'chatcmpl_test',
    object: 'chat.completion.chunk',
    created: 1,
    model: 'test-model',
    choices: [{ index: 0, delta, finish_reason: finishReason }],
    ...(usage ? { usage } : {})
  })}\n\n`;
}

function usageChunk(usage: Record<string, unknown>): string {
  return `data: ${JSON.stringify({
    id: 'chatcmpl_test',
    object: 'chat.completion.chunk',
    created: 1,
    model: 'test-model',
    choices: [],
    usage
  })}\n\n`;
}

async function consumeHangingTurn(frames: string[], timeoutMs: number) {
  const relay = createOptimisticOpenAIChatStreamRelay({ adapterKey: 'anthropic_messages' });
  if (!relay) {
    throw new Error('Expected an Anthropic Messages optimistic relay.');
  }

  const result: OptimisticOpenAIChatStreamTurnResult = {};
  const abortController = new AbortController();
  let timedOut = false;
  const timeout = setTimeout(() => {
    timedOut = true;
    abortController.abort(new Error('Timed out waiting for the optimistic stream turn.'));
  }, timeoutMs);

  try {
    const downstreamFrames: string[] = [];
    for await (const frame of relayOptimisticOpenAIChatStreamTurn(
      createHangingSseResponse(frames),
      relay,
      result,
      abortController.signal
    )) {
      downstreamFrames.push(frame);
    }
    if (timedOut) {
      throw abortController.signal.reason;
    }
    return { downstreamFrames, result };
  } finally {
    clearTimeout(timeout);
  }
}

describe('relayOptimisticOpenAIChatStreamTurn', () => {
  it('finishes on [DONE] even when the upstream transport remains open', async () => {
    const { downstreamFrames, result } = await consumeHangingTurn(
      [
        chatChunk({ role: 'assistant', content: 'hello' }),
        chatChunk({}, 'stop'),
        'data: [DONE]\n\n'
      ],
      250
    );

    expect(downstreamFrames.join('')).toContain('hello');
    expect(result.upstreamPayload).toMatchObject({
      choices: [
        {
          finish_reason: 'stop',
          message: { content: 'hello' }
        }
      ]
    });
  });

  it('collects trailing usage after finish_reason and then releases a hanging transport', async () => {
    const upstreamUsage = {
      prompt_tokens: 12,
      completion_tokens: 3,
      total_tokens: 15
    };
    const { result } = await consumeHangingTurn(
      [chatChunk({ content: 'done' }), chatChunk({}, 'stop'), usageChunk(upstreamUsage)],
      250
    );

    expect(result.upstreamPayload).toMatchObject({
      choices: [{ finish_reason: 'stop' }],
      usage: {
        input_tokens: 12,
        output_tokens: 3,
        total_tokens: 15
      }
    });
  });

  it('uses the finish grace period when no usage or [DONE] event follows', async () => {
    const { result } = await consumeHangingTurn(
      [chatChunk({ content: 'done' }), chatChunk({}, 'stop')],
      1_500
    );

    expect(result.upstreamPayload).toMatchObject({
      choices: [{ finish_reason: 'stop', message: { content: 'done' } }]
    });
  });
});
