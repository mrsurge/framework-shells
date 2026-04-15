type ConsoleLevel = 'log' | 'info' | 'warn' | 'error' | 'debug';

interface SocketIoSocket {
  connected: boolean;
  emit(event: string, payload: unknown): void;
  on(event: string, handler: (payload: unknown) => void): void;
  disconnect?: () => void;
}

interface SocketIoConnectOptions {
  path: string;
  transports: string[];
  query: Record<string, string>;
}

type SocketIoFactory = (namespace: string, options: SocketIoConnectOptions) => SocketIoSocket;

export interface ConsoleBridgeHandle {
  socket: SocketIoSocket;
  workerId: string;
  destroy: () => void;
}

interface ConsoleBridgeOptions {
  socket?: SocketIoSocket;
  workerId?: string;
  workerLabel?: string;
  uniquePerWindow?: boolean;
  socketPath?: string;
  namespace?: string;
  appId?: string;
  source?: string;
  socketIoScriptPath?: string;
}

interface WindowWithSocketIo extends Window {
  io?: SocketIoFactory;
  __fwsConsoleBridge?: ConsoleBridgeHandle;
}

const CONSOLE_LEVELS: ConsoleLevel[] = ['log', 'info', 'warn', 'error', 'debug'];
const DEFAULT_SOCKET_IO_SCRIPT_PATH = '/static/vendor/socket.io.min.js';
const DEFAULT_NAMESPACE = '/te2_console';
const DEFAULT_SOCKET_PATH = '/te2_console_ws/socket.io';
const DEFAULT_APP_ID = 'file_editor_cm6';
const DEFAULT_SOURCE = 'console_bridge';

let bridgeActive = false;
let bridgeSocket: SocketIoSocket | null = null;
let bridgeWorkerId: string | null = null;
let bridgeWorkerLabel: string | null = null;
let fwsConsoleBridgePromise: Promise<ConsoleBridgeHandle | null> | null = null;

const originalConsole: Partial<Record<ConsoleLevel, (...args: unknown[]) => void>> = {};

function getSocketIoFactory(): SocketIoFactory | null {
  const io = (window as WindowWithSocketIo).io;
  return typeof io === 'function' ? io : null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function isEvalRequest(value: unknown): value is { reqId: string; code: string } {
  if (!isRecord(value)) {
    return false;
  }
  return typeof value.reqId === 'string' && typeof value.code === 'string';
}

function loadScript(src: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const script = document.createElement('script');
    script.src = src;
    script.async = true;
    script.onload = () => resolve();
    script.onerror = (event) => {
      script.remove();
      reject(event);
    };
    document.head.appendChild(script);
  });
}

async function ensureSocketIoClient(scriptPath: string): Promise<void> {
  if (getSocketIoFactory()) {
    return;
  }
  await loadScript(scriptPath);
  if (!getSocketIoFactory()) {
    throw new Error('Failed to load Socket.IO client');
  }
}

function safeSerialize(value: unknown): string {
  const seen = new WeakSet<object>();
  return JSON.stringify(value, (_key, nextValue) => {
    if (typeof nextValue === 'bigint') {
      return `BigInt(${nextValue.toString()})`;
    }
    if (nextValue instanceof Error) {
      return { name: nextValue.name, message: nextValue.message, stack: nextValue.stack };
    }
    if (typeof nextValue === 'object' && nextValue !== null) {
      if (seen.has(nextValue)) {
        return '[Circular]';
      }
      seen.add(nextValue);
    }
    return nextValue;
  });
}

function serializeArg(value: unknown): unknown {
  try {
    return JSON.parse(safeSerialize(value));
  } catch {
    return String(value);
  }
}

function randomWorkerSuffix(): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID().split('-')[0] || Math.random().toString(36).slice(2, 10);
  }
  return Math.random().toString(36).slice(2, 10);
}

function sanitizeWorkerLabel(value: unknown): string {
  const raw = String(value ?? '').trim();
  const normalized = raw.replace(/[^a-zA-Z0-9._:-]+/g, '_').replace(/^_+|_+$/g, '');
  return normalized || 'worker';
}

function perWindowWorkerId(label: string): string {
  const base = sanitizeWorkerLabel(label);
  const storageKey = `te2.consoleBridge.workerId:${base}`;
  try {
    const existing = window.sessionStorage.getItem(storageKey);
    if (existing && existing.trim()) {
      return existing.trim();
    }
    const created = `${base}:${randomWorkerSuffix()}`;
    window.sessionStorage.setItem(storageKey, created);
    return created;
  } catch {
    return `${base}:${randomWorkerSuffix()}`;
  }
}

function emitLog(level: ConsoleLevel, rawArgs: unknown[]): void {
  if (!bridgeSocket || !bridgeSocket.connected || !bridgeWorkerId || !bridgeWorkerLabel) {
    return;
  }
  bridgeSocket.emit('console:log', {
    workerId: bridgeWorkerId,
    workerLabel: bridgeWorkerLabel,
    level,
    ts: Date.now(),
    args: rawArgs.map(serializeArg),
  });
}

function patchConsole(): void {
  const consoleRef = console as unknown as Record<ConsoleLevel, (...args: unknown[]) => void>;
  for (const level of CONSOLE_LEVELS) {
    originalConsole[level] = consoleRef[level].bind(console);
    consoleRef[level] = (...args: unknown[]) => {
      try {
        emitLog(level, args);
      } catch {
        // Never break caller logging.
      }
      const original = originalConsole[level];
      if (original) {
        original(...args);
      }
    };
  }
}

function hookErrors(): void {
  window.addEventListener('error', (event: ErrorEvent) => {
    emitLog('error', [event.message, event.filename, event.lineno, event.colno, event.error ?? null]);
  });
  window.addEventListener('unhandledrejection', (event: PromiseRejectionEvent) => {
    emitLog('error', ['UnhandledRejection', event.reason]);
  });
}

function hookEval(): void {
  if (!bridgeSocket) {
    return;
  }
  bridgeSocket.on('console:eval', async (payload: unknown) => {
    if (!isEvalRequest(payload) || !bridgeSocket || !bridgeWorkerId) {
      return;
    }
    try {
      let result: unknown;
      try {
        result = (0, eval)(payload.code);
      } catch (error) {
        if (error instanceof SyntaxError) {
          result = (0, eval)(`(${payload.code})`);
        } else {
          throw error;
        }
      }
      const resolved = await Promise.resolve(result);
      bridgeSocket.emit('console:evalResult', {
        workerId: bridgeWorkerId,
        reqId: payload.reqId,
        ok: true,
        value: serializeArg(resolved),
      });
    } catch (error) {
      bridgeSocket.emit('console:evalResult', {
        workerId: bridgeWorkerId,
        reqId: payload.reqId,
        ok: false,
        error: serializeArg(error),
      });
    }
  });
}

export async function initConsoleBridge(opts: ConsoleBridgeOptions = {}): Promise<ConsoleBridgeHandle | null> {
  if (bridgeActive && bridgeSocket && bridgeWorkerId) {
    return { socket: bridgeSocket, workerId: bridgeWorkerId, destroy: destroyConsoleBridge };
  }

  bridgeWorkerLabel = sanitizeWorkerLabel(opts.workerLabel || opts.workerId || 'worker');
  if (opts.uniquePerWindow) {
    bridgeWorkerId = perWindowWorkerId(bridgeWorkerLabel);
  } else if (typeof opts.workerId === 'string' && opts.workerId.trim()) {
    bridgeWorkerId = opts.workerId.trim();
  } else if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    bridgeWorkerId = crypto.randomUUID();
  } else {
    bridgeWorkerId = `w_${Math.random().toString(36).slice(2, 10)}`;
  }

  if (opts.socket) {
    bridgeSocket = opts.socket;
  } else {
    await ensureSocketIoClient(opts.socketIoScriptPath || DEFAULT_SOCKET_IO_SCRIPT_PATH);
    const io = getSocketIoFactory();
    if (!io || !bridgeWorkerId) {
      console.warn('[console_bridge] window.io not available - bridge not started');
      return null;
    }
    bridgeSocket = io(opts.namespace || DEFAULT_NAMESPACE, {
      path: opts.socketPath || DEFAULT_SOCKET_PATH,
      transports: ['websocket'],
      query: {
        app_id: opts.appId || DEFAULT_APP_ID,
        source: opts.source || DEFAULT_SOURCE,
        workerId: bridgeWorkerId,
        workerLabel: bridgeWorkerLabel,
      },
    });
  }

  const register = (): void => {
    if (!bridgeSocket || !bridgeWorkerId || !bridgeWorkerLabel) {
      return;
    }
    bridgeSocket.emit('console:register', {
      workerId: bridgeWorkerId,
      workerLabel: bridgeWorkerLabel,
      role: 'worker',
    });
  };

  bridgeSocket.on('connect', () => {
    register();
  });
  if (bridgeSocket.connected) {
    register();
  }

  patchConsole();
  hookErrors();
  hookEval();
  bridgeActive = true;

  return bridgeSocket && bridgeWorkerId
    ? { socket: bridgeSocket, workerId: bridgeWorkerId, destroy: destroyConsoleBridge }
    : null;
}

export function destroyConsoleBridge(): void {
  if (!bridgeActive) {
    return;
  }
  const consoleRef = console as unknown as Record<ConsoleLevel, (...args: unknown[]) => void>;
  for (const level of CONSOLE_LEVELS) {
    const original = originalConsole[level];
    if (original) {
      consoleRef[level] = original;
    }
  }
  if (bridgeSocket?.disconnect) {
    try {
      bridgeSocket.disconnect();
    } catch {
      // Best effort cleanup only.
    }
  }
  bridgeSocket = null;
  bridgeWorkerId = null;
  bridgeWorkerLabel = null;
  bridgeActive = false;
}

export function initFwsConsoleBridge(): Promise<ConsoleBridgeHandle | null> {
  if (fwsConsoleBridgePromise) {
    return fwsConsoleBridgePromise;
  }
  fwsConsoleBridgePromise = (async () => {
    try {
      const bridge = await initConsoleBridge({
        workerLabel: 'framework_shells',
        uniquePerWindow: true,
        source: 'fws_console_bridge',
      });
      if (bridge) {
        (window as WindowWithSocketIo).__fwsConsoleBridge = bridge;
        console.info('[fws] console bridge ready', bridge.workerId);
      }
      return bridge;
    } catch (error) {
      console.warn('[fws] failed to init console bridge', error);
      return null;
    }
  })();
  return fwsConsoleBridgePromise;
}
