export type JsonRpcVersion = '2.0';
export type LogStreamName = 'stdout' | 'stderr';
export type ShutdownScope = 'tree' | 'shells';
export type ShellNotificationMethod =
  | 'fws.shell.created'
  | 'fws.shell.spawned'
  | 'fws.shell.updated'
  | 'fws.shell.exited';

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

export interface LogsCloseParams {
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

export interface DashboardShellStats {
  alive?: boolean;
  uptime?: number | null;
  cpu_percent?: number;
  memory_rss?: number;
}

export interface DashboardShellCapabilities {
  backend?: string;
  stdin_write?: boolean;
  stdin_eof?: boolean;
  stdout_subscribe?: boolean;
  stdout_subscribe_bytes?: boolean;
  stderr_subscribe?: boolean;
  resize?: boolean;
  reattach?: boolean;
}

export interface DashboardPipeRuntime {
  engine?: string;
  active?: boolean;
  phase?: string;
}

export interface DashboardShellPayload {
  id?: string;
  spec_id?: string | null;
  command?: string[];
  label?: string | null;
  subgroups?: string[];
  ui?: Record<string, unknown>;
  cwd?: string;
  pid?: number | null;
  status?: string;
  created_at?: number;
  updated_at?: number;
  autostart?: boolean;
  stdout_log?: string;
  stderr_log?: string;
  exit_code?: number | null;
  env_keys?: string[];
  run_id?: string | null;
  launcher_pid?: number | null;
  adopted?: boolean;
  backend?: string;
  uses_pty?: boolean;
  uses_pipes?: boolean;
  uses_dtach?: boolean;
  pty_mode?: string;
  runtime_id?: string | null;
  app_id?: string | null;
  parent_shell_id?: string | null;
  is_app_worker?: boolean;
  stats?: DashboardShellStats;
  capabilities?: DashboardShellCapabilities;
  pipe_runtime?: DashboardPipeRuntime;
}

export interface DashboardProcessPayload {
  pid?: number;
  parent_pid?: number | null;
  type?: string;
  label?: string | null;
  shell_id?: string | null;
  metadata?: Record<string, unknown>;
}

export interface DashboardStatePayload {
  shells: DashboardShellPayload[];
  processes: DashboardProcessPayload[];
}

export interface ShellEventParams {
  shell: DashboardShellPayload;
}

export interface ShellRemovedParams {
  shell_id: string;
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
  'fws.logs.close': LogsCloseParams;
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
  'fws.dashboard.open': { accepted: true; state: DashboardStatePayload };
  'fws.logs.open': { accepted: true; shell_id: string };
  'fws.logs.close': { ok: true };
  'fws.dashboard.refresh': { ok: true; state: DashboardStatePayload };
  'fws.logs.truncate': { ok: true };
  'fws.exited.purge': { ok: true };
  'fws.shell.terminate': { ok: true };
  'fws.shell.purge': { ok: true };
  'fws.pid.terminate': { ok: true };
  'fws.app.shutdown': { ok: true };
  'fws.shutdown': { ok: true };
}

export interface ServerNotificationMap {
  'fws.shell.created': ShellEventParams;
  'fws.shell.spawned': ShellEventParams;
  'fws.shell.updated': ShellEventParams;
  'fws.shell.exited': ShellEventParams;
  'fws.shell.removed': ShellRemovedParams;
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

function asString(value: unknown): string | undefined {
  return typeof value === 'string' ? value : undefined;
}

function asNullableString(value: unknown): string | null | undefined {
  if (value === null) {
    return null;
  }
  return typeof value === 'string' ? value : undefined;
}

function asNumber(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isFinite(value) ? value : undefined;
}

function asNullableNumber(value: unknown): number | null | undefined {
  if (value === null) {
    return null;
  }
  return asNumber(value);
}

function asBoolean(value: unknown): boolean | undefined {
  return typeof value === 'boolean' ? value : undefined;
}

function asStringArray(value: unknown): string[] | undefined {
  if (!Array.isArray(value)) {
    return undefined;
  }
  const result: string[] = [];
  for (const item of value) {
    if (typeof item === 'string') {
      result.push(item);
    }
  }
  return result;
}

function asObjectRecord(value: unknown): Record<string, unknown> | undefined {
  return isRecord(value) ? value : undefined;
}

function coerceDashboardShellStats(value: unknown): DashboardShellStats | undefined {
  const record = asObjectRecord(value);
  if (!record) {
    return undefined;
  }
  const result: DashboardShellStats = {};
  const alive = asBoolean(record.alive);
  if (alive !== undefined) {
    result.alive = alive;
  }
  const uptime = asNullableNumber(record.uptime);
  if (uptime !== undefined) {
    result.uptime = uptime;
  }
  const cpuPercent = asNumber(record.cpu_percent);
  if (cpuPercent !== undefined) {
    result.cpu_percent = cpuPercent;
  }
  const memoryRss = asNumber(record.memory_rss);
  if (memoryRss !== undefined) {
    result.memory_rss = memoryRss;
  }
  return result;
}

function coerceDashboardShellCapabilities(value: unknown): DashboardShellCapabilities | undefined {
  const record = asObjectRecord(value);
  if (!record) {
    return undefined;
  }
  const result: DashboardShellCapabilities = {};
  const backend = asString(record.backend);
  if (backend !== undefined) {
    result.backend = backend;
  }
  const stdinWrite = asBoolean(record.stdin_write);
  if (stdinWrite !== undefined) {
    result.stdin_write = stdinWrite;
  }
  const stdinEof = asBoolean(record.stdin_eof);
  if (stdinEof !== undefined) {
    result.stdin_eof = stdinEof;
  }
  const stdoutSubscribe = asBoolean(record.stdout_subscribe);
  if (stdoutSubscribe !== undefined) {
    result.stdout_subscribe = stdoutSubscribe;
  }
  const stdoutSubscribeBytes = asBoolean(record.stdout_subscribe_bytes);
  if (stdoutSubscribeBytes !== undefined) {
    result.stdout_subscribe_bytes = stdoutSubscribeBytes;
  }
  const stderrSubscribe = asBoolean(record.stderr_subscribe);
  if (stderrSubscribe !== undefined) {
    result.stderr_subscribe = stderrSubscribe;
  }
  const resize = asBoolean(record.resize);
  if (resize !== undefined) {
    result.resize = resize;
  }
  const reattach = asBoolean(record.reattach);
  if (reattach !== undefined) {
    result.reattach = reattach;
  }
  return result;
}

function coerceDashboardPipeRuntime(value: unknown): DashboardPipeRuntime | undefined {
  const record = asObjectRecord(value);
  if (!record) {
    return undefined;
  }
  const result: DashboardPipeRuntime = {};
  const engine = asString(record.engine);
  if (engine !== undefined) {
    result.engine = engine;
  }
  const active = asBoolean(record.active);
  if (active !== undefined) {
    result.active = active;
  }
  const phase = asString(record.phase);
  if (phase !== undefined) {
    result.phase = phase;
  }
  return result;
}

function coerceDashboardShellPayload(value: unknown): DashboardShellPayload | null {
  const record = asObjectRecord(value);
  if (!record) {
    return null;
  }
  const result: DashboardShellPayload = {};
  const id = asString(record.id);
  if (id !== undefined) {
    result.id = id;
  }
  const specId = asNullableString(record.spec_id);
  if (specId !== undefined) {
    result.spec_id = specId;
  }
  const command = asStringArray(record.command);
  if (command !== undefined) {
    result.command = command;
  }
  const label = asNullableString(record.label);
  if (label !== undefined) {
    result.label = label;
  }
  const subgroups = asStringArray(record.subgroups);
  if (subgroups !== undefined) {
    result.subgroups = subgroups;
  }
  const ui = asObjectRecord(record.ui);
  if (ui !== undefined) {
    result.ui = ui;
  }
  const cwd = asString(record.cwd);
  if (cwd !== undefined) {
    result.cwd = cwd;
  }
  const pid = asNullableNumber(record.pid);
  if (pid !== undefined) {
    result.pid = pid;
  }
  const status = asString(record.status);
  if (status !== undefined) {
    result.status = status;
  }
  const createdAt = asNumber(record.created_at);
  if (createdAt !== undefined) {
    result.created_at = createdAt;
  }
  const updatedAt = asNumber(record.updated_at);
  if (updatedAt !== undefined) {
    result.updated_at = updatedAt;
  }
  const autostart = asBoolean(record.autostart);
  if (autostart !== undefined) {
    result.autostart = autostart;
  }
  const stdoutLog = asString(record.stdout_log);
  if (stdoutLog !== undefined) {
    result.stdout_log = stdoutLog;
  }
  const stderrLog = asString(record.stderr_log);
  if (stderrLog !== undefined) {
    result.stderr_log = stderrLog;
  }
  const exitCode = asNullableNumber(record.exit_code);
  if (exitCode !== undefined) {
    result.exit_code = exitCode;
  }
  const envKeys = asStringArray(record.env_keys);
  if (envKeys !== undefined) {
    result.env_keys = envKeys;
  }
  const runId = asNullableString(record.run_id);
  if (runId !== undefined) {
    result.run_id = runId;
  }
  const launcherPid = asNullableNumber(record.launcher_pid);
  if (launcherPid !== undefined) {
    result.launcher_pid = launcherPid;
  }
  const adopted = asBoolean(record.adopted);
  if (adopted !== undefined) {
    result.adopted = adopted;
  }
  const backend = asString(record.backend);
  if (backend !== undefined) {
    result.backend = backend;
  }
  const usesPty = asBoolean(record.uses_pty);
  if (usesPty !== undefined) {
    result.uses_pty = usesPty;
  }
  const usesPipes = asBoolean(record.uses_pipes);
  if (usesPipes !== undefined) {
    result.uses_pipes = usesPipes;
  }
  const usesDtach = asBoolean(record.uses_dtach);
  if (usesDtach !== undefined) {
    result.uses_dtach = usesDtach;
  }
  const ptyMode = asString(record.pty_mode);
  if (ptyMode !== undefined) {
    result.pty_mode = ptyMode;
  }
  const runtimeId = asNullableString(record.runtime_id);
  if (runtimeId !== undefined) {
    result.runtime_id = runtimeId;
  }
  const appId = asNullableString(record.app_id);
  if (appId !== undefined) {
    result.app_id = appId;
  }
  const parentShellId = asNullableString(record.parent_shell_id);
  if (parentShellId !== undefined) {
    result.parent_shell_id = parentShellId;
  }
  const isAppWorker = asBoolean(record.is_app_worker);
  if (isAppWorker !== undefined) {
    result.is_app_worker = isAppWorker;
  }
  const stats = coerceDashboardShellStats(record.stats);
  if (stats !== undefined) {
    result.stats = stats;
  }
  const capabilities = coerceDashboardShellCapabilities(record.capabilities);
  if (capabilities !== undefined) {
    result.capabilities = capabilities;
  }
  const pipeRuntime = coerceDashboardPipeRuntime(record.pipe_runtime);
  if (pipeRuntime !== undefined) {
    result.pipe_runtime = pipeRuntime;
  }
  return result;
}

function coerceDashboardProcessPayload(value: unknown): DashboardProcessPayload | null {
  const record = asObjectRecord(value);
  if (!record) {
    return null;
  }
  const result: DashboardProcessPayload = {};
  const pid = asNumber(record.pid);
  if (pid !== undefined) {
    result.pid = pid;
  }
  const parentPid = asNullableNumber(record.parent_pid);
  if (parentPid !== undefined) {
    result.parent_pid = parentPid;
  }
  const type = asString(record.type);
  if (type !== undefined) {
    result.type = type;
  }
  const label = asNullableString(record.label);
  if (label !== undefined) {
    result.label = label;
  }
  const shellId = asNullableString(record.shell_id);
  if (shellId !== undefined) {
    result.shell_id = shellId;
  }
  const metadata = asObjectRecord(record.metadata);
  if (metadata !== undefined) {
    result.metadata = metadata;
  }
  return result;
}

function coerceDashboardStatePayload(value: unknown): DashboardStatePayload | null {
  const record = asObjectRecord(value);
  if (!record || !Array.isArray(record.shells) || !Array.isArray(record.processes)) {
    return null;
  }
  const shells: DashboardShellPayload[] = [];
  for (const shell of record.shells) {
    const parsed = coerceDashboardShellPayload(shell);
    if (parsed) {
      shells.push(parsed);
    }
  }
  const processes: DashboardProcessPayload[] = [];
  for (const process of record.processes) {
    const parsed = coerceDashboardProcessPayload(process);
    if (parsed) {
      processes.push(parsed);
    }
  }
  return { shells, processes };
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
  return JSON.stringify(buildClientRequest(method, id, params));
}

export function buildClientRequest<M extends ClientRequestMethod>(
  method: M,
  id: string,
  params: ClientRequestMap[M],
): ClientRequestFor<M> {
  return {
    jsonrpc: '2.0',
    id,
    method,
    params,
  };
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

function coerceIncomingJsonRpcObject(parsed: unknown): IncomingJsonRpcMessage | null {
  if (!isRecord(parsed)) {
    return null;
  }

  if (!isJsonRpcVersion(parsed.jsonrpc)) {
    return null;
  }

  const parsedId = parsed.id;
  const parsedMethod = parsed.method;
  const parsedResult = parsed.result;
  const parsedError = parsed.error;
  const parsedParams = parsed.params;

  if (typeof parsedId === 'string' && isRecord(parsedResult)) {
    const result = parsedResult;
    if (result.accepted === true) {
      const state = coerceDashboardStatePayload(result.state);
      if (state) {
        return {
          jsonrpc: '2.0',
          id: parsedId,
          result: { accepted: true, state },
        };
      }
      if (typeof result.shell_id === 'string') {
        return {
          jsonrpc: '2.0',
          id: parsedId,
          result: { accepted: true, shell_id: result.shell_id },
        };
      }
      return null;
    }
    if (result.ok === true) {
      const state = coerceDashboardStatePayload(result.state);
      if (state) {
        return {
          jsonrpc: '2.0',
          id: parsedId,
          result: { ok: true, state },
        };
      }
      return {
        jsonrpc: '2.0',
        id: parsedId,
        result: { ok: true },
      };
    }
    return null;
  }

  if ((typeof parsedId === 'string' || parsedId === null) && isRecord(parsedError)) {
    const error = parsedError;
    if (typeof error.code !== 'number' || typeof error.message !== 'string') {
      return null;
    }
    const response: JsonRpcErrorResponse = {
      jsonrpc: '2.0',
      id: typeof parsedId === 'string' ? parsedId : null,
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

  if (typeof parsedMethod !== 'string' || !isRecord(parsedParams)) {
    return null;
  }

  switch (parsedMethod) {
    case 'fws.shell.created':
    case 'fws.shell.spawned':
    case 'fws.shell.updated':
    case 'fws.shell.exited': {
      const shell = coerceDashboardShellPayload(parsedParams.shell);
      if (!shell) {
        return null;
      }
      return {
        jsonrpc: '2.0',
        method: parsedMethod,
        params: { shell },
      };
    }
    case 'fws.shell.removed':
      if (typeof parsedParams.shell_id === 'string') {
        return {
          jsonrpc: '2.0',
          method: parsedMethod,
          params: { shell_id: parsedParams.shell_id },
        };
      }
      return null;
    case 'fws.logs.initial':
      if (
        typeof parsedParams.shell_id === 'string' &&
        typeof parsedParams.stdout === 'string' &&
        typeof parsedParams.stderr === 'string'
      ) {
        return {
          jsonrpc: '2.0',
          method: parsedMethod,
          params: {
            shell_id: parsedParams.shell_id,
            stdout: parsedParams.stdout,
            stderr: parsedParams.stderr,
          },
        };
      }
      return null;
    case 'fws.logs.chunk':
      if (
        typeof parsedParams.shell_id === 'string' &&
        isLogStreamName(parsedParams.stream) &&
        typeof parsedParams.chunk === 'string'
      ) {
        return {
          jsonrpc: '2.0',
          method: parsedMethod,
          params: {
            shell_id: parsedParams.shell_id,
            stream: parsedParams.stream,
            chunk: parsedParams.chunk,
          },
        };
      }
      return null;
    case 'fws.logs.reset':
      if (typeof parsedParams.shell_id === 'string' && isLogStreamName(parsedParams.stream)) {
        return {
          jsonrpc: '2.0',
          method: parsedMethod,
          params: {
            shell_id: parsedParams.shell_id,
            stream: parsedParams.stream,
          },
        };
      }
      return null;
    case 'fws.error':
      if (typeof parsedParams.message === 'string') {
        const result: ErrorParams = { message: parsedParams.message };
        if (typeof parsedParams.code === 'string') {
          result.code = parsedParams.code;
        }
        if (typeof parsedParams.shell_id === 'string') {
          result.shell_id = parsedParams.shell_id;
        }
        return {
          jsonrpc: '2.0',
          method: parsedMethod,
          params: result,
        };
      }
      return null;
    default:
      return null;
  }
}

export function coerceIncomingJsonRpcMessage(value: unknown): IncomingJsonRpcMessage | null {
  return coerceIncomingJsonRpcObject(value);
}

export function parseIncomingJsonRpcMessage(raw: string): IncomingJsonRpcMessage | null {
  const parsed = parseJsonRpcObject(raw);
  if (!parsed) {
    return null;
  }
  return coerceIncomingJsonRpcObject(parsed);
}
