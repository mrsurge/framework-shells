interface SocketIoConnectOptions {
  auth?: Record<string, string>;
  path: string;
  query?: Record<string, string>;
  transports: string[];
}

export interface SocketIoSocket {
  connected: boolean;
  disconnect?: () => void;
  emit(event: string, payload?: unknown, ack?: (payload: unknown) => void): void;
  off?(event: string, handler?: (...args: unknown[]) => void): void;
  on(event: string, handler: (...args: unknown[]) => void): void;
}

type SocketIoFactory = (namespace: string, options: SocketIoConnectOptions) => SocketIoSocket;

interface WindowWithSocketIo extends Window {
  io?: SocketIoFactory;
}

const DEFAULT_SOCKET_IO_SCRIPT_PATH = '/static/vendor/socket.io.min.js';

function getSocketIoFactory(): SocketIoFactory | null {
  const io = (window as WindowWithSocketIo).io;
  return typeof io === 'function' ? io : null;
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

export async function ensureSocketIoClient(scriptPath = DEFAULT_SOCKET_IO_SCRIPT_PATH): Promise<void> {
  if (getSocketIoFactory()) {
    return;
  }
  await loadScript(scriptPath);
  if (!getSocketIoFactory()) {
    throw new Error('Failed to load Socket.IO client');
  }
}

export async function connectSocketIo(
  namespace: string,
  options: {
    auth?: Record<string, string>;
    path: string;
    query?: Record<string, string>;
    socketIoScriptPath?: string;
    transports?: string[];
  },
): Promise<SocketIoSocket> {
  await ensureSocketIoClient(options.socketIoScriptPath || DEFAULT_SOCKET_IO_SCRIPT_PATH);
  const io = getSocketIoFactory();
  if (!io) {
    throw new Error('Socket.IO client factory unavailable');
  }
  const connectOptions: SocketIoConnectOptions = {
    path: options.path,
    transports: options.transports ?? ['websocket'],
  };
  if (options.auth) {
    connectOptions.auth = options.auth;
  }
  if (options.query) {
    connectOptions.query = options.query;
  }
  return io(namespace, connectOptions);
}
