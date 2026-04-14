export type JsonRpcVersion = '2.0';
export type LogStreamName = 'stdout' | 'stderr';

export interface JsonRpcNotification<M extends string, P> {
  jsonrpc: JsonRpcVersion;
  method: M;
  params: P;
}

export interface DashboardConnectParams {
  view: 'html';
}

export interface LogsConnectParams {
  shell_id: string;
}

export interface DashboardSnapshotParams {
  html: string;
}

export interface LogsInitialParams {
  shell_id: string;
  stdout: string;
  stderr: string;
}

export interface LogsChunkParams {
  shell_id: string;
  stream: LogStreamName;
  chunk: string;
}

export interface LogsResetParams {
  shell_id: string;
  stream: LogStreamName;
}

export interface ErrorParams {
  message: string;
  code?: string;
  shell_id?: string;
}

export interface ClientNotificationMap {
  'fws.dashboard.connect': DashboardConnectParams;
  'fws.logs.connect': LogsConnectParams;
}

export interface ServerNotificationMap {
  'fws.dashboard.snapshot': DashboardSnapshotParams;
  'fws.logs.initial': LogsInitialParams;
  'fws.logs.chunk': LogsChunkParams;
  'fws.logs.reset': LogsResetParams;
  'fws.error': ErrorParams;
}

export type ClientMethod = keyof ClientNotificationMap;
export type ServerMethod = keyof ServerNotificationMap;

export type ClientNotificationFor<M extends ClientMethod> = JsonRpcNotification<M, ClientNotificationMap[M]>;
export type ServerNotificationFor<M extends ServerMethod> = JsonRpcNotification<M, ServerNotificationMap[M]>;

export type ClientNotification = {
  [M in ClientMethod]: ClientNotificationFor<M>;
}[ClientMethod];

export type ServerNotification = {
  [M in ServerMethod]: ServerNotificationFor<M>;
}[ServerMethod];

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function isLogStreamName(value: unknown): value is LogStreamName {
  return value === 'stdout' || value === 'stderr';
}

function parseJsonRpcNotification(raw: string): { method: string; params: Record<string, unknown> } | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }
  if (!isRecord(parsed) || parsed.jsonrpc !== '2.0' || typeof parsed.method !== 'string' || !isRecord(parsed.params)) {
    return null;
  }
  return {
    method: parsed.method,
    params: parsed.params,
  };
}

export function stringifyClientNotification<M extends ClientMethod>(method: M, params: ClientNotificationMap[M]): string {
  const payload: ClientNotificationFor<M> = {
    jsonrpc: '2.0',
    method,
    params,
  };
  return JSON.stringify(payload);
}

export function parseServerNotification(raw: string): ServerNotification | null {
  const parsed = parseJsonRpcNotification(raw);
  if (!parsed) {
    return null;
  }

  switch (parsed.method) {
    case 'fws.dashboard.snapshot':
      if (typeof parsed.params.html === 'string') {
        return {
          jsonrpc: '2.0',
          method: parsed.method,
          params: { html: parsed.params.html },
        };
      }
      return null;
    case 'fws.logs.initial':
      if (
        typeof parsed.params.shell_id === 'string' &&
        typeof parsed.params.stdout === 'string' &&
        typeof parsed.params.stderr === 'string'
      ) {
        return {
          jsonrpc: '2.0',
          method: parsed.method,
          params: {
            shell_id: parsed.params.shell_id,
            stdout: parsed.params.stdout,
            stderr: parsed.params.stderr,
          },
        };
      }
      return null;
    case 'fws.logs.chunk':
      if (
        typeof parsed.params.shell_id === 'string' &&
        isLogStreamName(parsed.params.stream) &&
        typeof parsed.params.chunk === 'string'
      ) {
        return {
          jsonrpc: '2.0',
          method: parsed.method,
          params: {
            shell_id: parsed.params.shell_id,
            stream: parsed.params.stream,
            chunk: parsed.params.chunk,
          },
        };
      }
      return null;
    case 'fws.logs.reset':
      if (typeof parsed.params.shell_id === 'string' && isLogStreamName(parsed.params.stream)) {
        return {
          jsonrpc: '2.0',
          method: parsed.method,
          params: {
            shell_id: parsed.params.shell_id,
            stream: parsed.params.stream,
          },
        };
      }
      return null;
    case 'fws.error':
      if (typeof parsed.params.message === 'string') {
        const result: ErrorParams = { message: parsed.params.message };
        if (typeof parsed.params.code === 'string') {
          result.code = parsed.params.code;
        }
        if (typeof parsed.params.shell_id === 'string') {
          result.shell_id = parsed.params.shell_id;
        }
        return {
          jsonrpc: '2.0',
          method: parsed.method,
          params: result,
        };
      }
      return null;
    default:
      return null;
  }
}

export function parseServerNotificationData(raw: unknown): ServerNotification | null {
  if (typeof raw !== 'string') {
    return null;
  }
  return parseServerNotification(raw);
}
