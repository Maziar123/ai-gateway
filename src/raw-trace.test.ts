import { mkdtemp, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, expect, it, vi } from 'vitest';
import { closeRawTraceManager, enqueueRawTraceCapture, initializeRawTraceManager } from './raw-trace';
import { publishJsonEventToExternalSink } from './external-event-sink';

vi.mock('./external-event-sink', () => ({ publishJsonEventToExternalSink: vi.fn() }));
afterEach(async () => { await closeRawTraceManager(); vi.clearAllMocks(); });

it('#1867 bounds retained request bodies while the trace sink is unavailable', async () => {
  const spoolDir = await mkdtemp(join(tmpdir(), 'gateway-trace-capacity-'));
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  vi.mocked(publishJsonEventToExternalSink).mockImplementation(async () => {
    await gate;
    throw new Error('host unavailable');
  });
  const warn = vi.fn();
  try {
    await initializeRawTraceManager({
      enabled: true, mode: 'body_full', spoolDir, maxPartBytes: 1024,
      uploaderConcurrency: 2, maxAttempts: 1, baseDelayMs: 1,
      sync: { enabled: true, transport: 'http', endpoint: 'http://unused', headers: {}, timeoutMs: 10,
        maxAttempts: 1, baseDelayMs: 1, maxDelayMs: 1, requireAck: true, apiKeyHeader: 'x-test' },
    }, { warn });
    for (let index = 0; index < 20; index += 1) {
      enqueueRawTraceCapture({
        requestId: `capture-${index}`, method: 'POST', url: '/v1/messages',
        parts: [{ partType: 'client_request', content: { index } }],
      });
    }
    // Two active uploads and at most four queued captures retain bodies.
    expect(warn.mock.calls.filter(([payload]) => payload.reason === 'queue_capacity')).toHaveLength(14);
  } finally {
    release();
    await closeRawTraceManager();
    const bundles = await readdir(spoolDir);
    await rm(spoolDir, { recursive: true, force: true });
    expect(bundles.length).toBeLessThanOrEqual(6);
  }
});

it('#1867 releases capacity after the final upload failure', async () => {
  const spoolDir = await mkdtemp(join(tmpdir(), 'gateway-trace-release-'));
  const warn = vi.fn();
  vi.mocked(publishJsonEventToExternalSink).mockRejectedValue(new Error('host unavailable'));
  try {
    await initializeRawTraceManager({
      enabled: true, mode: 'body_full', spoolDir, maxPartBytes: 1024,
      uploaderConcurrency: 1, maxAttempts: 1, baseDelayMs: 1,
      sync: { enabled: true, transport: 'http', endpoint: 'http://unused', headers: {}, timeoutMs: 10,
        maxAttempts: 1, baseDelayMs: 1, maxDelayMs: 1, requireAck: true, apiKeyHeader: 'x-test' },
    }, { warn });
    for (let index = 0; index < 8; index += 1) {
      enqueueRawTraceCapture({ requestId: `retry-${index}`, method: 'POST', url: '/v1/messages',
        parts: [{ partType: 'client_request', content: 'hello' }] });
      await vi.waitFor(() => expect(warn).toHaveBeenCalledTimes(index + 1));
    }
    expect(warn.mock.calls.every(([payload]) => payload.reason !== 'queue_capacity')).toBe(true);
    expect(await readdir(spoolDir)).toHaveLength(8);
  } finally {
    await closeRawTraceManager();
    await rm(spoolDir, { recursive: true, force: true });
  }
});
