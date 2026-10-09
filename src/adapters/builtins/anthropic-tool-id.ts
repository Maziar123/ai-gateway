import { createHash, randomUUID } from 'node:crypto';

/** Keep calls and results paired, including when replaying a different protocol's history. */
export function anthropicToolId(value: string | undefined): string {
  if (!value) return `toolu_${randomUUID().replace(/-/g, '')}`;
  if (/^[A-Za-z0-9_-]+$/.test(value)) return value;
  return `toolu_ccr_${createHash('sha256').update(value).digest('hex').slice(0, 48)}`;
}
