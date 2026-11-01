const defaultTerminalErrorMessage = 'Upstream stream failed before the response completed.';
const upstreamStreamErrorCode = 'upstream_stream_error';
const defaultTerminalErrorType = 'api_error';

// Transport failures worth naming back to the client: the client cannot discover
// these from a truncated body, and none of them carries request or credential
// material. The set is closed on purpose - anything else stays generic, so no
// provider message, URL or error object can reach the client through this path.
const knownTransportCodes: Record<string, true> = {
  EAI_AGAIN: true,
  ECONNABORTED: true,
  ECONNREFUSED: true,
  ECONNRESET: true,
  EHOSTUNREACH: true,
  ENETUNREACH: true,
  EPIPE: true,
  ETIMEDOUT: true,
  UND_ERR_BODY_TIMEOUT: true,
  UND_ERR_CLOSED: true,
  UND_ERR_CONNECT_TIMEOUT: true,
  UND_ERR_HEADERS_TIMEOUT: true,
  UND_ERR_SOCKET: true
};

const maxTransportCauseDepth = 4;

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
 * Adapters that can express a failed stream in-band. Checked before building
 * anything so a caller can decide support without allocating a frame.
 */
function supportsTerminalErrorFrame(adapterKey: string): boolean {
  return (
    adapterKey === 'anthropic_messages' ||
    adapterKey === 'openai_chat' ||
    adapterKey === 'openai_responses'
  );
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
  const body = response.body;
  if (
    !body ||
    !isEventStreamContentType(response.headers.get('content-type')) ||
    // An adapter with no in-band error event has nothing to end the stream with,
    // so the response must pass through untouched.
    !supportsTerminalErrorFrame(input.adapterKey)
  ) {
    return response;
  }

  const reader = body.getReader();
  const state: TerminalErrorPumpState = {
    cancelled: false,
    detector: createSseTerminalEventDetector(),
    encoder: new TextEncoder(),
    // Built only when the stream actually fails, because only then is the
    // transport failure known - and only then can its identity be reported.
    buildFrames: (cause) =>
      buildStreamTerminalErrorFrames({
        adapterKey: input.adapterKey,
        message: terminalErrorMessageFor(input.message, cause)
      }),
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
  buildFrames: (cause: unknown) => string[];
  isClientAbort: () => boolean;
}

/**
 * The configured message stays authoritative; a recognised transport code is
 * appended so the client learns why the body stopped without the server ever
 * copying a provider message into the frame.
 */
function terminalErrorMessageFor(configured: string | undefined, cause: unknown): string {
  const code = readTransportCode(cause);
  if (!code) {
    return configured ?? defaultTerminalErrorMessage;
  }

  const base = configured ?? defaultTerminalErrorMessage;
  // A configured message that already names the code is a caller's deliberate
  // wording; appending again would read as a doubled diagnostic.
  return base.includes(code) ? base : `${base} (transport: ${code})`;
}

/**
 * Walks a bounded `code`/`cause` chain and returns the first closed-set transport
 * code. Nothing else about the error is read: no message, no URL, no object.
 */
function readTransportCode(error: unknown): string | undefined {
  let current: unknown = error;

  for (let depth = 0; depth < maxTransportCauseDepth; depth += 1) {
    if (!current || typeof current !== 'object') {
      return undefined;
    }

    const record = current as { cause?: unknown; code?: unknown };
    const code = record.code;
    if (typeof code === 'string' && knownTransportCodes[code] === true) {
      return code;
    }

    // Only an explicitly closed set is ever returned, so an unknown code on this
    // record must not stop the walk - the real cause may be one level deeper.
    current = record.cause;
  }

  return undefined;
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
    const frames = state.buildFrames(error);
    if (frames.length === 0) {
      controller.error(error);
      return;
    }
    for (const frame of frames) {
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
