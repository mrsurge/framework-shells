export type WindowAction = 'tail' | 'older' | 'newer' | 'current';
export const LOG_WINDOW_RECORDS = 200;
export const LOG_WINDOW_SHIFT = 50;
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

export function viewportScroll(view: LogWindow, top: number, height: number, viewport: number,
  delta: number, following: boolean, busy: boolean): {following: boolean; action: WindowAction | null} {
  const bottom = Math.max(0, height - top - viewport);
  if (bottom >= 12) following = false;
  else if (view.at_tail && delta > 0) return {following: true, action: 'tail'};
  if (busy || following || Math.abs(delta) < 1) return {following, action: null};
  const runway = Math.max(160, viewport);
  const action = delta < 0 && !view.at_start && top < runway ? 'older'
    : delta > 0 && !view.at_tail && bottom < runway ? 'newer' : null;
  return {following, action};
}

// Adjacent backend pages form an overlapping viewport; never retain full history.
export function slideWindow(previous: LogWindow, page: LogWindow, action: WindowAction): LogWindow {
  if (previous.generation !== page.generation || (action !== 'older' && action !== 'newer')) return page;
  if (action === 'older' ? page.end !== previous.start : page.start < previous.start || page.start > previous.end) return page;
  let records = action === 'older' ? [...page.records, ...previous.records] : [...previous.records.slice(0, page.start - previous.start), ...page.records];
  records = action === 'older' ? records.slice(0, LOG_WINDOW_RECORDS) : records.slice(-LOG_WINDOW_RECORDS);
  const start = action === 'older' ? page.start : page.end - records.length;
  const end = start + records.length;
  return {...page, records, start, end, at_start: start === 0, at_tail: end === page.total};
}

function endpoint(shell: string, operation: string, params: URLSearchParams): string {
  const path = window.location.pathname;
  const prefix = path.slice(0, path.lastIndexOf('/fws'));
  return `${prefix}/api/framework_shells/logs/${encodeURIComponent(shell)}/${operation}?${params}`;
}

export async function loadWindow(shell: string, stream: string, action: WindowAction,
  previous?: LogWindow, resync = false): Promise<LogWindow> {
  // Refresh the boundary record: a text log's previous final line may have grown.
  const newer = action === 'newer' && previous !== undefined;
  const cursor = newer ? Math.max(previous.start, previous.end - 1) : previous?.start;
  const params = new URLSearchParams({ stream, action: newer ? 'current' : action,
    count: String(newer ? LOG_WINDOW_SHIFT + 1 : LOG_WINDOW_RECORDS), shift: String(LOG_WINDOW_SHIFT), current: String(cursor ?? 0) });
  if (previous) params.set('generation', previous.generation);
  const response = await fetch(endpoint(shell, 'window', params));
  if (response.status === 409 && !resync) return loadWindow(shell, stream, 'tail', undefined, true);
  if (!response.ok) throw new Error(`Log window: ${response.status} ${await response.text()}`);
  const body = await response.json() as { data: LogWindow };
  if (!body.data || !Array.isArray(body.data.records) || body.data.records.length > 1000) {
    throw new Error('Invalid log window response');
  }
  return previous ? slideWindow(previous, body.data, action) : body.data;
}

export async function loadRawPage(shell: string, stream: string, raw: RawReference, offset: number): Promise<{hex: string; next_offset: number; eof: boolean}> {
  const params = new URLSearchParams({stream, generation: raw.generation,
    byte_start: String(raw.byte_start), byte_end: String(raw.byte_end), offset: String(offset), limit: '65536'});
  const response = await fetch(endpoint(shell, 'raw', params));
  if (!response.ok) throw new Error(`Original bytes: ${response.status} ${await response.text()}`);
  return (await response.json() as {data: {hex: string; next_offset: number; eof: boolean}}).data;
}
