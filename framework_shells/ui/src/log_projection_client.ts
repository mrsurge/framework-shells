export type WindowAction = 'tail' | 'older' | 'newer' | 'current';
export interface RawReference { generation: string; byte_start: number; byte_end: number }
export interface ProjectedRecord {
  text: string;
  raw: RawReference;
  diagnostic: string | null;
  omissions: { pointer: string; original_type: string; serialized_bytes: number }[];
}
export interface LogWindow {
  generation: string; start: number; end: number; total: number;
  at_start: boolean; at_tail: boolean; pending_bytes: number;
  records: ProjectedRecord[];
}

function endpoint(shell: string, operation: string, params: URLSearchParams): string {
  const path = window.location.pathname;
  const prefix = path.slice(0, path.lastIndexOf('/fws'));
  return `${prefix}/api/framework_shells/logs/${encodeURIComponent(shell)}/${operation}?${params}`;
}

export async function loadWindow(shell: string, stream: string, action: WindowAction,
  previous?: LogWindow, resync = false): Promise<LogWindow> {
  const cursor = action === 'newer' ? previous?.end : previous?.start;
  const params = new URLSearchParams({ stream, action, count: '1000', shift: '250', current: String(cursor ?? 0) });
  if (previous) params.set('generation', previous.generation);
  const response = await fetch(endpoint(shell, 'window', params));
  if (response.status === 409 && !resync) return loadWindow(shell, stream, 'tail', undefined, true);
  if (!response.ok) throw new Error(`Log window: ${response.status} ${await response.text()}`);
  const body = await response.json() as { data: LogWindow };
  if (!body.data || !Array.isArray(body.data.records) || body.data.records.length > 1000) {
    throw new Error('Invalid log window response');
  }
  return body.data;
}

export async function loadRawPage(shell: string, stream: string, raw: RawReference, offset: number): Promise<{hex: string; next_offset: number; eof: boolean}> {
  const params = new URLSearchParams({stream, generation: raw.generation,
    byte_start: String(raw.byte_start), byte_end: String(raw.byte_end), offset: String(offset), limit: '65536'});
  const response = await fetch(endpoint(shell, 'raw', params));
  if (!response.ok) throw new Error(`Original bytes: ${response.status} ${await response.text()}`);
  return (await response.json() as {data: {hex: string; next_offset: number; eof: boolean}}).data;
}
