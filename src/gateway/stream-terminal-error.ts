const defaultTerminalErrorMessage = 'Upstream stream failed before the response completed.';
const upstreamStreamErrorCode = 'upstream_stream_error';
const defaultTerminalErrorType = 'api_error';

const terminalSseEventNames: Record<string, true> = {
  error: true,
  message_stop: true,
  'response.completed': true,
  'response.error': true,
  'response.failed': true,
  'response.incomplete': true
};

// A quoted `type` can only appear at the JSON structure level: text carried by a
// model is escaped, so a delta that merely contains the word cannot match here.
const terminalSsePayloadPattern =
  /"type"\s*:\s*"(?:error|message_stop|response\.(?:completed|error|failed|incomplete))"/;

export interface StreamTerminalErrorFrameInput {
  adapterKey: string;
  code?: string;
  errorType?: string;
  message?: string;
}

export interface SseTerminalEventDetector {
  hasTerminalEvent(): boolean;
  push(chunk: Uint8Array): void;
}

export interface StreamTerminalErrorGuardInput {
  adapterKey: string;
  message?: string;
  signal?: AbortSignal;
}

/**
 * Frames a downstream client can classify as a failed stream, in the client's own
 * protocol. An empty result means the adapter has no in-band error event, so the
 * caller must keep failing the stream the other way (socket close).
 */
export function buildStreamTerminalErrorFrames(input: StreamTerminalErrorFrameInput): string[] {
  const message = input.message ?? defaultTerminalErrorMessage;
  const code = input.code ?? upstreamStreamErrorCode;
  const errorType = input.errorType ?? defaultTerminalErrorType;
  const error = { code, message, type: errorType };

  switch (input.adapterKey) {
    case 'anthropic_messages':
      return [encodeSseFrame('error', { error, type: 'error' })];
    case 'openai_chat':
      return [encodeSseFrame(undefined, { error }), encodeSseFrame(undefined, '[DONE]')];
    case 'openai_responses':
      return [
        encodeSseFrame('response.failed', {
          response: { error, object: 'response', status: 'failed' },
          type: 'response.failed'
        }),
        encodeSseFrame(undefined, '[DONE]')
      ];
    default:
      return [];
  }
}

export function createSseTerminalEventDetector(): SseTerminalEventDetector {
  const decoder = new TextDecoder();
  let buffer = '';
  let terminalEventSeen = false;

  return {
    hasTerminalEvent: () => terminalEventSeen,
    push(chunk: Uint8Array): void {
      if (terminalEventSeen) {
        return;
      }

      buffer += decoder.decode(chunk, { stream: true });
      let newlineIndex = buffer.indexOf('\n');
      while (newlineIndex >= 0) {
        const line = buffer.slice(0, newlineIndex);
        buffer = buffer.slice(newlineIndex + 1);
        if (isTerminalSseLine(line)) {
          terminalEventSeen = true;
          return;
        }
        newlineIndex = buffer.indexOf('\n');
      }
    }
  };
}

/**
 * Wraps an event-stream response so a body that dies mid-stream ends with one
 * terminal error frame instead of a bare EOF. The client cannot tell a truncated
 * frame from a completed answer otherwise, and never retries.
 */
export function guardStreamTerminalError(
  response: Response,
  input: StreamTerminalErrorGuardInput
): Response {
  const frames = response.body && isEventStreamContentType(response.headers.get('content-type'))
    ? buildStreamTerminalErrorFrames({ adapterKey: input.adapterKey, message: input.message })
    : [];

  if (!response.body || frames.length === 0) {
    return response;
  }

  const reader = response.body.getReader();
  const state: TerminalErrorPumpState = {
    cancelled: false,
    detector: createSseTerminalEventDetector(),
    encoder: new TextEncoder(),
    frames,
    isClientAbort: () => Boolean(input.signal?.aborted)
  };
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      void pumpStreamWithTerminalError(reader, controller, state);
    },
    cancel(reason) {
      state.cancelled = true;
      return reader.cancel(reason);
    }
  });

  const headers = new Headers(response.headers);
  // The terminal frame changes the body length, so the upstream value cannot stand.
  headers.delete('content-length');

  return new Response(stream, {
    headers,
    status: response.status,
    statusText: response.statusText
  });
}

interface TerminalErrorPumpState {
  cancelled: boolean;
  detector: SseTerminalEventDetector;
  encoder: TextEncoder;
  frames: string[];
  isClientAbort: () => boolean;
}

async function pumpStreamWithTerminalError(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  controller: ReadableStreamDefaultController<Uint8Array>,
  state: TerminalErrorPumpState
): Promise<void> {
  try {
    while (true) {
      const chunk = await reader.read();
      if (chunk.done) {
        break;
      }
      state.detector.push(chunk.value);
      controller.enqueue(chunk.value);
    }
    controller.close();
  } catch (error) {
    if (state.cancelled) {
      return;
    }
    // A terminal event already reached the client: a second one would be a
    // protocol violation, so let the stream fail as before. The same holds once
    // the client is gone - there is nobody left to read a frame.
    if (state.isClientAbort() || state.detector.hasTerminalEvent()) {
      controller.error(error);
      return;
    }
    for (const frame of state.frames) {
      controller.enqueue(state.encoder.encode(frame));
    }
    controller.close();
  }
}

function isTerminalSseLine(rawLine: string): boolean {
  const line = rawLine.endsWith('\r') ? rawLine.slice(0, -1) : rawLine;
  if (line.startsWith('event:')) {
    return terminalSseEventNames[line.slice('event:'.length).trim()] === true;
  }
  if (line.startsWith('data:')) {
    const data = line.slice('data:'.length).trim();
    return data === '[DONE]' || terminalSsePayloadPattern.test(data);
  }
  return false;
}

function isEventStreamContentType(contentType: string | null): boolean {
  return Boolean(contentType && contentType.toLowerCase().includes('text/event-stream'));
}

function encodeSseFrame(eventName: string | undefined, data: unknown): string {
  const payload = typeof data === 'string' ? data : JSON.stringify(data);
  const dataLines = payload
    .split('\n')
    .map((line) => `data: ${line}`)
    .join('\n');
  return eventName ? `event: ${eventName}\n${dataLines}\n\n` : `${dataLines}\n\n`;
}
