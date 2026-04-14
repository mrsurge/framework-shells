export type JsonRpcVersion = '2.0';
export type LogStreamName = 'stdout' | 'stderr';
export type ShutdownScope = 'tree' | 'shells';

export interface JsonRpcNotification<M extends string, P> {
  jsonrpc: JsonRpcVersion;
  method: M;
  params: P;
}

export interface JsonRpcRequest<M extends string, P> extends JsonRpcNotification<M, P> {
  id: string;
}

export interface JsonRpcSuccessResponse<I extends string, R> {
  jsonrpc: JsonRpcVersion;
  id: I;
  result: R;
}

export interface JsonRpcErrorData {
  code?: string;
  shell_id?: string;
}

export interface JsonRpcErrorResponse<I extends string | null = string | null> {
  jsonrpc: JsonRpcVersion;
  id: I;
  error: {
    code: number;
    message: string;
    data?: JsonRpcErrorData;
  };
}

export interface DashboardOpenParams {
  view: 'html';
}

export interface LogsOpenParams {
  shell_id: string;
}

export interface EmptyParams {}

export interface ShellActionParams {
  shell_id: string;
}

export interface PidActionParams {
  pid: number;
}

export interface AppActionParams {
  app_id: string;
}

export interface ShutdownParams {
  scope: ShutdownScope;
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

export interface ClientRequestMap {
  'fws.dashboard.open': DashboardOpenParams;
  'fws.logs.open': LogsOpenParams;
  'fws.dashboard.refresh': EmptyParams;
  'fws.logs.truncate': EmptyParams;
  'fws.exited.purge': EmptyParams;
  'fws.shell.terminate': ShellActionParams;
  'fws.shell.purge': ShellActionParams;
  'fws.pid.terminate': PidActionParams;
  'fws.app.shutdown': AppActionParams;
  'fws.shutdown': ShutdownParams;
}

export interface RequestResultMap {
  'fws.dashboard.open': { accepted: true };
  'fws.logs.open': { accepted: true; shell_id: string };
  'fws.dashboard.refresh': { ok: true };
  'fws.logs.truncate': { ok: true };
  'fws.exited.purge': { ok: true };
  'fws.shell.terminate': { ok: true };
  'fws.shell.purge': { ok: true };
  'fws.pid.terminate': { ok: true };
  'fws.app.shutdown': { ok: true };
  'fws.shutdown': { ok: true };
}

export interface ServerNotificationMap {
  'fws.dashboard.snapshot': DashboardSnapshotParams;
  'fws.logs.initial': LogsInitialParams;
  'fws.logs.chunk': LogsChunkParams;
  'fws.logs.reset': LogsResetParams;
  'fws.error': ErrorParams;
}

export type ClientRequestMethod = keyof ClientRequestMap;
export type ServerNotificationMethod = keyof ServerNotificationMap;

export type ClientRequestFor<M extends ClientRequestMethod> = JsonRpcRequest<M, ClientRequestMap[M]>;
export type ServerSuccessResponseFor<M extends ClientRequestMethod> = JsonRpcSuccessResponse<string, RequestResultMap[M]>;
export type ServerNotificationFor<M extends ServerNotificationMethod> = JsonRpcNotification<M, ServerNotificationMap[M]>;

export type ClientRequest = {
  [M in ClientRequestMethod]: ClientRequestFor<M>;
}[ClientRequestMethod];

export type ServerSuccessResponse = {
  [M in ClientRequestMethod]: ServerSuccessResponseFor<M>;
}[ClientRequestMethod];

export type ServerNotification = {
  [M in ServerNotificationMethod]: ServerNotificationFor<M>;
}[ServerNotificationMethod];

export type IncomingJsonRpcMessage = ServerSuccessResponse | ServerNotification | JsonRpcErrorResponse;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function isLogStreamName(value: unknown): value is LogStreamName {
  return value === 'stdout' || value === 'stderr';
}

function isJsonRpcVersion(value: unknown): value is JsonRpcVersion {
  return value === '2.0';
}

function parseJsonRpcObject(raw: string): Record<string, unknown> | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }
  if (!isRecord(parsed) || !isJsonRpcVersion(parsed.jsonrpc)) {
    return null;
  }
  return parsed;
}

export function stringifyClientRequest<M extends ClientRequestMethod>(
  method: M,
  id: string,
  params: ClientRequestMap[M],
): string {
  const payload: ClientRequestFor<M> = {
    jsonrpc: '2.0',
    id,
    method,
    params,
  };
  return JSON.stringify(payload);
}

export function frameJsonRpcLine(payload: string): string {
  return payload.endsWith('\n') ? payload : `${payload}\n`;
}

export function consumeJsonlChunk(
  buffer: string,
  chunk: unknown,
): { lines: string[]; buffer: string } {
  if (typeof chunk !== 'string' || chunk.length === 0) {
    return { lines: [], buffer };
  }
  const combined = buffer + chunk;
  const parts = combined.split('\n');
  const nextBuffer = parts.pop() ?? '';
  const lines = parts.map((line) => line.trim()).filter((line) => line.length > 0);
  return { lines, buffer: nextBuffer };
}

export function parseIncomingJsonRpcMessage(raw: string): IncomingJsonRpcMessage | null {
  const parsed = parseJsonRpcObject(raw);
  if (!parsed) {
    return null;
  }

  if (typeof parsed.id === 'string' && isRecord(parsed.result)) {
    const result = parsed.result;
    if (result.accepted === true) {
      if (typeof result.shell_id === 'string') {
        return {
          jsonrpc: '2.0',
          id: parsed.id,
          result: { accepted: true, shell_id: result.shell_id },
        };
      }
      return {
        jsonrpc: '2.0',
        id: parsed.id,
        result: { accepted: true },
      };
    }
    if (result.ok === true) {
      return {
        jsonrpc: '2.0',
        id: parsed.id,
        result: { ok: true },
      };
    }
    return null;
  }

  if ((typeof parsed.id === 'string' || parsed.id === null) && isRecord(parsed.error)) {
    const error = parsed.error;
    if (typeof error.code !== 'number' || typeof error.message !== 'string') {
      return null;
    }
    const response: JsonRpcErrorResponse = {
      jsonrpc: '2.0',
      id: typeof parsed.id === 'string' ? parsed.id : null,
      error: {
        code: error.code,
        message: error.message,
      },
    };
    if (isRecord(error.data)) {
      const data: JsonRpcErrorData = {};
      if (typeof error.data.code === 'string') {
        data.code = error.data.code;
      }
      if (typeof error.data.shell_id === 'string') {
        data.shell_id = error.data.shell_id;
      }
      if (Object.keys(data).length > 0) {
        response.error.data = data;
      }
    }
    return response;
  }

  if (typeof parsed.method !== 'string' || !isRecord(parsed.params)) {
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
