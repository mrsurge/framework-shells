export interface ReconnectingWebSocketOptions {
  maxRetries?: number;
  reconnectInterval?: number;
  maxReconnectInterval?: number;
  reconnectDecay?: number;
  debug?: boolean;
  protocols?: string | string[];
}

type WebSocketSendData = string | ArrayBufferLike | Blob | ArrayBufferView;

type ReconnectingWebSocketError = Event | Error;

type ReconnectHandler = (attempt: number, delayMs: number) => void;

const DEFAULT_OPTIONS: Required<ReconnectingWebSocketOptions> = {
  maxRetries: Number.POSITIVE_INFINITY,
  reconnectInterval: 1000,
  maxReconnectInterval: 30000,
  reconnectDecay: 1.5,
  debug: false,
  protocols: [],
};

export class ReconnectingWebSocket {
  private readonly url: string;
  private readonly options: Required<ReconnectingWebSocketOptions>;
  private ws: WebSocket | null = null;
  private reconnectAttempts = 0;
  private reconnectTimeout: number | null = null;
  private readonly messageQueue: WebSocketSendData[] = [];
  private forcedClose = false;

  readyState: number = WebSocket.CONNECTING;

  onopen: ((event: Event) => void) | null = null;
  onmessage: ((event: MessageEvent) => void) | null = null;
  onerror: ((error: ReconnectingWebSocketError) => void) | null = null;
  onclose: ((event: CloseEvent) => void) | null = null;
  onreconnect: ReconnectHandler | null = null;

  constructor(url: string, options: ReconnectingWebSocketOptions = {}) {
    this.url = url;
    this.options = {
      maxRetries: options.maxRetries ?? DEFAULT_OPTIONS.maxRetries,
      reconnectInterval: options.reconnectInterval ?? DEFAULT_OPTIONS.reconnectInterval,
      maxReconnectInterval: options.maxReconnectInterval ?? DEFAULT_OPTIONS.maxReconnectInterval,
      reconnectDecay: options.reconnectDecay ?? DEFAULT_OPTIONS.reconnectDecay,
      debug: options.debug ?? DEFAULT_OPTIONS.debug,
      protocols: options.protocols ?? DEFAULT_OPTIONS.protocols,
    };

    this.connect();
  }

  private log(...args: unknown[]): void {
    if (this.options.debug) {
      console.log('[ReconnectingWebSocket]', ...args);
    }
  }

  private connect(): void {
    if (this.forcedClose) {
      this.log('Connection blocked: forcedClose = true');
      return;
    }

    this.log(`Connecting to ${this.url}...`);

    try {
      this.ws = new WebSocket(this.url, this.options.protocols);
      this.readyState = WebSocket.CONNECTING;

      this.ws.onopen = (event: Event) => {
        this.log('Connected successfully');
        this.readyState = WebSocket.OPEN;
        this.reconnectAttempts = 0;

        while (this.messageQueue.length > 0) {
          const message = this.messageQueue.shift();
          if (message === undefined || this.ws === null) {
            continue;
          }
          this.log('Sending queued message:', message);
          this.ws.send(message);
        }

        this.onopen?.(event);
      };

      this.ws.onmessage = (event: MessageEvent) => {
        this.onmessage?.(event);
      };

      this.ws.onerror = (event: Event) => {
        this.log('WebSocket error:', event);
        this.onerror?.(event);
      };

      this.ws.onclose = (event: CloseEvent) => {
        this.log('Connection closed:', event.code, event.reason);
        this.readyState = WebSocket.CLOSED;
        this.onclose?.(event);

        if (!this.forcedClose) {
          this.scheduleReconnect();
        }
      };
    } catch (error: unknown) {
      this.log('Connection error:', error);
      this.readyState = WebSocket.CLOSED;
      this.onerror?.(error instanceof Error ? error : new Error(String(error)));

      if (!this.forcedClose) {
        this.scheduleReconnect();
      }
    }
  }

  private scheduleReconnect(): void {
    if (this.reconnectAttempts >= this.options.maxRetries) {
      this.log(`Max reconnection attempts (${this.options.maxRetries}) reached`);
      return;
    }

    this.reconnectAttempts += 1;
    const delay = Math.min(
      this.options.reconnectInterval * Math.pow(this.options.reconnectDecay, this.reconnectAttempts - 1),
      this.options.maxReconnectInterval,
    );

    this.log(`Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts}/${this.options.maxRetries})...`);
    this.onreconnect?.(this.reconnectAttempts, delay);

    this.reconnectTimeout = window.setTimeout(() => {
      this.connect();
    }, delay);
  }

  send(data: WebSocketSendData): void {
    if (this.ws !== null && this.ws.readyState === WebSocket.OPEN) {
      this.log('Sending message:', data);
      this.ws.send(data);
      return;
    }

    this.log('Queueing message (not connected):', data);
    this.messageQueue.push(data);
  }

  close(code = 1000, reason = 'Normal closure'): void {
    this.log('Manually closing connection');
    this.forcedClose = true;

    if (this.reconnectTimeout !== null) {
      window.clearTimeout(this.reconnectTimeout);
      this.reconnectTimeout = null;
    }

    this.ws?.close(code, reason);
    this.readyState = WebSocket.CLOSED;
  }

  reconnect(): void {
    this.log('Manual reconnect requested');
    this.forcedClose = false;
    this.reconnectAttempts = 0;

    if (this.reconnectTimeout !== null) {
      window.clearTimeout(this.reconnectTimeout);
      this.reconnectTimeout = null;
    }

    this.ws?.close();
    this.connect();
  }

  get bufferedAmount(): number {
    return this.ws?.bufferedAmount ?? 0;
  }

  get extensions(): string {
    return this.ws?.extensions ?? '';
  }

  get protocol(): string {
    return this.ws?.protocol ?? '';
  }

  get binaryType(): BinaryType {
    return this.ws?.binaryType ?? 'blob';
  }

  set binaryType(type: BinaryType) {
    if (this.ws !== null) {
      this.ws.binaryType = type;
    }
  }
}

export default ReconnectingWebSocket;
