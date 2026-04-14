export type JsonRpcVersion = '2.0';
export type JsonScalar = null | boolean | number | string;
export type JsonValue = JsonScalar | JsonValue[] | { [key: string]: JsonValue };

export interface JsonRpcNotification<M extends string, P> {
  jsonrpc: JsonRpcVersion;
  method: M;
  params: P;
}

export interface TerminalConnectParams {
  cols?: number;
  rows?: number;
}

export interface TerminalInputParams {
  data_b64: string;
}

export interface TerminalResizeParams {
  cols: number;
  rows: number;
}

export interface TerminalDestroyParams {}

export interface TerminalPingParams {
  nonce?: JsonValue;
}

export interface TerminalClientNotificationMap {
  'terminal.connect': TerminalConnectParams;
  'terminal.input': TerminalInputParams;
  'terminal.resize': TerminalResizeParams;
  'terminal.destroy': TerminalDestroyParams;
  'terminal.ping': TerminalPingParams;
}

export type TerminalClientMethod = keyof TerminalClientNotificationMap;

export type TerminalClientNotificationFor<M extends TerminalClientMethod> = JsonRpcNotification<
  M,
  TerminalClientNotificationMap[M]
>;

export type TerminalClientNotification = {
  [M in TerminalClientMethod]: TerminalClientNotificationFor<M>;
}[TerminalClientMethod];

export interface TerminalReadyEventFrame {
  type: 'ready';
  ts: number;
  pid: number;
  shell: string[];
  cwd: string;
}

export interface TerminalDataEventFrame {
  type: 'data';
  seq: number;
  ts: number;
  data_b64: string;
}

export interface TerminalPongEventFrame {
  type: 'pong';
  nonce: JsonValue | null;
}

export interface TerminalClosedEventFrame {
  type: 'closed';
  seq: number;
  ts: number;
  exit_code: number | null;
  reason: string;
}

export type TerminalServerEventFrame =
  | TerminalReadyEventFrame
  | TerminalDataEventFrame
  | TerminalPongEventFrame
  | TerminalClosedEventFrame;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((part) => typeof part === 'string');
}

export function stringifyTerminalClientNotification<M extends TerminalClientMethod>(
  method: M,
  params: TerminalClientNotificationMap[M],
): string {
  const payload: TerminalClientNotificationFor<M> = {
    jsonrpc: '2.0',
    method,
    params,
  };
  return JSON.stringify(payload);
}

export function parseTerminalServerEvent(raw: string): TerminalServerEventFrame | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }
  if (!isRecord(parsed) || typeof parsed.type !== 'string') {
    return null;
  }

  switch (parsed.type) {
    case 'ready':
      if (
        typeof parsed.ts === 'number' &&
        typeof parsed.pid === 'number' &&
        isStringArray(parsed.shell) &&
        typeof parsed.cwd === 'string'
      ) {
        return {
          type: 'ready',
          ts: parsed.ts,
          pid: parsed.pid,
          shell: parsed.shell,
          cwd: parsed.cwd,
        };
      }
      return null;
    case 'data':
      if (
        typeof parsed.seq === 'number' &&
        typeof parsed.ts === 'number' &&
        typeof parsed.data_b64 === 'string'
      ) {
        return {
          type: 'data',
          seq: parsed.seq,
          ts: parsed.ts,
          data_b64: parsed.data_b64,
        };
      }
      return null;
    case 'pong':
      return {
        type: 'pong',
        nonce: (parsed.nonce as JsonValue | null | undefined) ?? null,
      };
    case 'closed':
      if (
        typeof parsed.seq === 'number' &&
        typeof parsed.ts === 'number' &&
        (typeof parsed.exit_code === 'number' || parsed.exit_code === null) &&
        typeof parsed.reason === 'string'
      ) {
        return {
          type: 'closed',
          seq: parsed.seq,
          ts: parsed.ts,
          exit_code: parsed.exit_code,
          reason: parsed.reason,
        };
      }
      return null;
    default:
      return null;
  }
}
