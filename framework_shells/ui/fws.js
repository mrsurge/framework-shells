"use strict";
(() => {
  // framework_shells/ui/src/socketio_client.ts
  var DEFAULT_SOCKET_IO_SCRIPT_PATH = "/static/vendor/socket.io.min.js";
  function getSocketIoFactory() {
    const io = window.io;
    return typeof io === "function" ? io : null;
  }
  function loadScript(src) {
    return new Promise((resolve, reject) => {
      const script = document.createElement("script");
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
  async function ensureSocketIoClient(scriptPath = DEFAULT_SOCKET_IO_SCRIPT_PATH) {
    if (getSocketIoFactory()) {
      return;
    }
    await loadScript(scriptPath);
    if (!getSocketIoFactory()) {
      throw new Error("Failed to load Socket.IO client");
    }
  }
  async function connectSocketIo(namespace, options) {
    await ensureSocketIoClient(options.socketIoScriptPath || DEFAULT_SOCKET_IO_SCRIPT_PATH);
    const io = getSocketIoFactory();
    if (!io) {
      throw new Error("Socket.IO client factory unavailable");
    }
    const connectOptions = {
      path: options.path,
      transports: options.transports ?? ["websocket"]
    };
    if (options.auth) {
      connectOptions.auth = options.auth;
    }
    if (options.query) {
      connectOptions.query = options.query;
    }
    return io(namespace, connectOptions);
  }

  // framework_shells/ui/src/te2_console_bridge.ts
  var CONSOLE_LEVELS = ["log", "info", "warn", "error", "debug"];
  var DEFAULT_SOCKET_IO_SCRIPT_PATH2 = "/static/vendor/socket.io.min.js";
  var DEFAULT_NAMESPACE = "/te2_console";
  var DEFAULT_SOCKET_PATH = "/te2_console_ws/socket.io";
  var DEFAULT_APP_ID = "file_editor_cm6";
  var DEFAULT_SOURCE = "console_bridge";
  var bridgeActive = false;
  var bridgeSocket = null;
  var bridgeWorkerId = null;
  var bridgeWorkerLabel = null;
  var fwsConsoleBridgePromise = null;
  var originalConsole = {};
  function getSocketIoFactory2() {
    const io = window.io;
    return typeof io === "function" ? io : null;
  }
  function isRecord(value) {
    return typeof value === "object" && value !== null;
  }
  function isEvalRequest(value) {
    if (!isRecord(value)) {
      return false;
    }
    return typeof value.reqId === "string" && typeof value.code === "string";
  }
  function loadScript2(src) {
    return new Promise((resolve, reject) => {
      const script = document.createElement("script");
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
  async function ensureSocketIoClient2(scriptPath) {
    if (getSocketIoFactory2()) {
      return;
    }
    await loadScript2(scriptPath);
    if (!getSocketIoFactory2()) {
      throw new Error("Failed to load Socket.IO client");
    }
  }
  function safeSerialize(value) {
    const seen = /* @__PURE__ */ new WeakSet();
    return JSON.stringify(value, (_key, nextValue) => {
      if (typeof nextValue === "bigint") {
        return `BigInt(${nextValue.toString()})`;
      }
      if (nextValue instanceof Error) {
        return { name: nextValue.name, message: nextValue.message, stack: nextValue.stack };
      }
      if (typeof nextValue === "object" && nextValue !== null) {
        if (seen.has(nextValue)) {
          return "[Circular]";
        }
        seen.add(nextValue);
      }
      return nextValue;
    });
  }
  function serializeArg(value) {
    try {
      return JSON.parse(safeSerialize(value));
    } catch {
      return String(value);
    }
  }
  function randomWorkerSuffix() {
    if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
      return crypto.randomUUID().split("-")[0] || Math.random().toString(36).slice(2, 10);
    }
    return Math.random().toString(36).slice(2, 10);
  }
  function sanitizeWorkerLabel(value) {
    const raw = String(value ?? "").trim();
    const normalized = raw.replace(/[^a-zA-Z0-9._:-]+/g, "_").replace(/^_+|_+$/g, "");
    return normalized || "worker";
  }
  function perWindowWorkerId(label) {
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
  function emitLog(level, rawArgs) {
    if (!bridgeSocket || !bridgeSocket.connected || !bridgeWorkerId || !bridgeWorkerLabel) {
      return;
    }
    bridgeSocket.emit("console:log", {
      workerId: bridgeWorkerId,
      workerLabel: bridgeWorkerLabel,
      level,
      ts: Date.now(),
      args: rawArgs.map(serializeArg)
    });
  }
  function patchConsole() {
    const consoleRef = console;
    for (const level of CONSOLE_LEVELS) {
      originalConsole[level] = consoleRef[level].bind(console);
      consoleRef[level] = (...args) => {
        try {
          emitLog(level, args);
        } catch {
        }
        const original = originalConsole[level];
        if (original) {
          original(...args);
        }
      };
    }
  }
  function hookErrors() {
    window.addEventListener("error", (event) => {
      emitLog("error", [event.message, event.filename, event.lineno, event.colno, event.error ?? null]);
    });
    window.addEventListener("unhandledrejection", (event) => {
      emitLog("error", ["UnhandledRejection", event.reason]);
    });
  }
  function hookEval() {
    if (!bridgeSocket) {
      return;
    }
    bridgeSocket.on("console:eval", async (payload) => {
      if (!isEvalRequest(payload) || !bridgeSocket || !bridgeWorkerId) {
        return;
      }
      try {
        let result;
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
        bridgeSocket.emit("console:evalResult", {
          workerId: bridgeWorkerId,
          reqId: payload.reqId,
          ok: true,
          value: serializeArg(resolved)
        });
      } catch (error) {
        bridgeSocket.emit("console:evalResult", {
          workerId: bridgeWorkerId,
          reqId: payload.reqId,
          ok: false,
          error: serializeArg(error)
        });
      }
    });
  }
  async function initConsoleBridge(opts = {}) {
    if (bridgeActive && bridgeSocket && bridgeWorkerId) {
      return { socket: bridgeSocket, workerId: bridgeWorkerId, destroy: destroyConsoleBridge };
    }
    bridgeWorkerLabel = sanitizeWorkerLabel(opts.workerLabel || opts.workerId || "worker");
    if (opts.uniquePerWindow) {
      bridgeWorkerId = perWindowWorkerId(bridgeWorkerLabel);
    } else if (typeof opts.workerId === "string" && opts.workerId.trim()) {
      bridgeWorkerId = opts.workerId.trim();
    } else if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
      bridgeWorkerId = crypto.randomUUID();
    } else {
      bridgeWorkerId = `w_${Math.random().toString(36).slice(2, 10)}`;
    }
    if (opts.socket) {
      bridgeSocket = opts.socket;
    } else {
      await ensureSocketIoClient2(opts.socketIoScriptPath || DEFAULT_SOCKET_IO_SCRIPT_PATH2);
      const io = getSocketIoFactory2();
      if (!io || !bridgeWorkerId) {
        console.warn("[console_bridge] window.io not available - bridge not started");
        return null;
      }
      bridgeSocket = io(opts.namespace || DEFAULT_NAMESPACE, {
        path: opts.socketPath || DEFAULT_SOCKET_PATH,
        transports: ["websocket"],
        query: {
          app_id: opts.appId || DEFAULT_APP_ID,
          source: opts.source || DEFAULT_SOURCE,
          workerId: bridgeWorkerId,
          workerLabel: bridgeWorkerLabel
        }
      });
    }
    const register = () => {
      if (!bridgeSocket || !bridgeWorkerId || !bridgeWorkerLabel) {
        return;
      }
      bridgeSocket.emit("console:register", {
        workerId: bridgeWorkerId,
        workerLabel: bridgeWorkerLabel,
        role: "worker"
      });
    };
    bridgeSocket.on("connect", () => {
      register();
    });
    if (bridgeSocket.connected) {
      register();
    }
    patchConsole();
    hookErrors();
    hookEval();
    bridgeActive = true;
    return bridgeSocket && bridgeWorkerId ? { socket: bridgeSocket, workerId: bridgeWorkerId, destroy: destroyConsoleBridge } : null;
  }
  function destroyConsoleBridge() {
    if (!bridgeActive) {
      return;
    }
    const consoleRef = console;
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
      }
    }
    bridgeSocket = null;
    bridgeWorkerId = null;
    bridgeWorkerLabel = null;
    bridgeActive = false;
  }
  function initFwsConsoleBridge() {
    if (fwsConsoleBridgePromise) {
      return fwsConsoleBridgePromise;
    }
    fwsConsoleBridgePromise = (async () => {
      try {
        const bridge = await initConsoleBridge({
          workerLabel: "framework_shells",
          uniquePerWindow: true,
          source: "fws_console_bridge"
        });
        if (bridge) {
          window.__fwsConsoleBridge = bridge;
          console.info("[fws] console bridge ready", bridge.workerId);
        }
        return bridge;
      } catch (error) {
        console.warn("[fws] failed to init console bridge", error);
        return null;
      }
    })();
    return fwsConsoleBridgePromise;
  }

  // framework_shells/ui/src/ansi_json_log_renderer.ts
  var MAX_JSON_FRAGMENT_CHARS = 2e5;
  var DEFAULT_FG = "#c9d1d9";
  var DEFAULT_BG = "#0d1117";
  var ANSI_16_COLORS = {
    0: "#484f58",
    1: "#ff7b72",
    2: "#7ee787",
    3: "#d29922",
    4: "#79c0ff",
    5: "#d2a8ff",
    6: "#76e3ea",
    7: "#c9d1d9",
    8: "#6e7681",
    9: "#ffa198",
    10: "#56d364",
    11: "#e3b341",
    12: "#a5d6ff",
    13: "#d2a8ff",
    14: "#39c5cf",
    15: "#f0f6fc"
  };
  function createDefaultAnsiStyle() {
    return {
      bold: false,
      dim: false,
      italic: false,
      underline: false,
      inverse: false
    };
  }
  function cloneAnsiStyle(style) {
    const clone = createDefaultAnsiStyle();
    if (style.fg) {
      clone.fg = style.fg;
    }
    if (style.bg) {
      clone.bg = style.bg;
    }
    clone.bold = style.bold;
    clone.dim = style.dim;
    clone.italic = style.italic;
    clone.underline = style.underline;
    clone.inverse = style.inverse;
    return clone;
  }
  function advanceAnsiStyle(text, initialStyle = createDefaultAnsiStyle()) {
    return parseAnsiSegments(text, initialStyle).style;
  }
  function renderLogLine(text, initialStyle = createDefaultAnsiStyle(), options = {}) {
    const parsed = parseAnsiSegments(text, initialStyle);
    const fragment = document.createDocumentFragment();
    for (const segment of parsed.segments) {
      if (segment.type === "control") {
        appendControlMarker(fragment, segment.marker, segment.kind, segment.style);
      } else {
        appendStyledText(fragment, segment.text, segment.style, options);
      }
    }
    return { fragment, finalStyle: parsed.style };
  }
  function parseAnsiSegments(text, initialStyle) {
    const segments = [];
    let style = cloneAnsiStyle(initialStyle);
    let buffer = "";
    const flush = () => {
      if (!buffer) {
        return;
      }
      segments.push({ type: "text", text: buffer, style: cloneAnsiStyle(style) });
      buffer = "";
    };
    for (let index = 0; index < text.length; index += 1) {
      const code = text.charCodeAt(index);
      if (code === 27) {
        const csi = parseCsiSequence(text, index);
        if (csi && csi.final === "m") {
          flush();
          style = applySgrParams(style, csi.params);
          index = csi.end;
          continue;
        }
        flush();
        segments.push({ type: "control", marker: "[ESC]", kind: "esc", style: cloneAnsiStyle(style) });
        continue;
      }
      if (code === 8) {
        flush();
        segments.push({ type: "control", marker: "[BS]", kind: "backspace", style: cloneAnsiStyle(style) });
        continue;
      }
      if (code === 127) {
        flush();
        segments.push({ type: "control", marker: "[DEL]", kind: "delete", style: cloneAnsiStyle(style) });
        continue;
      }
      if (code === 13) {
        flush();
        segments.push({ type: "control", marker: "[CR]", kind: "carriage-return", style: cloneAnsiStyle(style) });
        continue;
      }
      if (code < 32 && code !== 9) {
        flush();
        segments.push({ type: "control", marker: `[0x${code.toString(16).padStart(2, "0")}]`, kind: "control", style: cloneAnsiStyle(style) });
        continue;
      }
      buffer += text[index] ?? "";
    }
    flush();
    return { segments, style };
  }
  function parseCsiSequence(text, start) {
    if (text[start] !== "\x1B" || text[start + 1] !== "[") {
      return null;
    }
    for (let index = start + 2; index < text.length && index < start + 80; index += 1) {
      const code = text.charCodeAt(index);
      if (code >= 64 && code <= 126) {
        const rawParams = text.slice(start + 2, index);
        const params = rawParams.length === 0 ? [0] : rawParams.split(";").map((part) => {
          const parsed = Number.parseInt(part || "0", 10);
          return Number.isFinite(parsed) ? parsed : 0;
        });
        return { params, final: text[index] ?? "", end: index };
      }
    }
    return null;
  }
  function applySgrParams(inputStyle, params) {
    const style = cloneAnsiStyle(inputStyle);
    const effectiveParams = params.length > 0 ? params : [0];
    for (let index = 0; index < effectiveParams.length; index += 1) {
      const code = effectiveParams[index] ?? 0;
      if (code === 0) {
        return createDefaultAnsiStyle();
      }
      if (code === 1) {
        style.bold = true;
        continue;
      }
      if (code === 2) {
        style.dim = true;
        continue;
      }
      if (code === 3) {
        style.italic = true;
        continue;
      }
      if (code === 4) {
        style.underline = true;
        continue;
      }
      if (code === 7) {
        style.inverse = true;
        continue;
      }
      if (code === 22) {
        style.bold = false;
        style.dim = false;
        continue;
      }
      if (code === 23) {
        style.italic = false;
        continue;
      }
      if (code === 24) {
        style.underline = false;
        continue;
      }
      if (code === 27) {
        style.inverse = false;
        continue;
      }
      if (code === 39) {
        delete style.fg;
        continue;
      }
      if (code === 49) {
        delete style.bg;
        continue;
      }
      if (code >= 30 && code <= 37) {
        setColor(style, "fg", ansi16Color(code - 30));
        continue;
      }
      if (code >= 40 && code <= 47) {
        setColor(style, "bg", ansi16Color(code - 40));
        continue;
      }
      if (code >= 90 && code <= 97) {
        setColor(style, "fg", ansi16Color(code - 90 + 8));
        continue;
      }
      if (code >= 100 && code <= 107) {
        setColor(style, "bg", ansi16Color(code - 100 + 8));
        continue;
      }
      if (code === 38 || code === 48) {
        const target = code === 38 ? "fg" : "bg";
        const mode = effectiveParams[index + 1];
        if (mode === 5) {
          const color = effectiveParams[index + 2];
          if (typeof color === "number") {
            setColor(style, target, ansi256Color(color));
            index += 2;
          }
          continue;
        }
        if (mode === 2) {
          const red = effectiveParams[index + 2];
          const green = effectiveParams[index + 3];
          const blue = effectiveParams[index + 4];
          if (isByte(red) && isByte(green) && isByte(blue)) {
            setColor(style, target, `rgb(${red}, ${green}, ${blue})`);
            index += 4;
          }
          continue;
        }
      }
    }
    return style;
  }
  function isByte(value) {
    return typeof value === "number" && Number.isInteger(value) && value >= 0 && value <= 255;
  }
  function setColor(style, target, color) {
    if (!color) {
      return;
    }
    if (target === "fg") {
      style.fg = color;
    } else {
      style.bg = color;
    }
  }
  function ansi16Color(index) {
    return ANSI_16_COLORS[index] ?? null;
  }
  function ansi256Color(index) {
    if (!Number.isInteger(index) || index < 0 || index > 255) {
      return null;
    }
    if (index < 16) {
      return ansi16Color(index);
    }
    if (index >= 232) {
      const level = 8 + (index - 232) * 10;
      return `rgb(${level}, ${level}, ${level})`;
    }
    const offset = index - 16;
    const red = Math.floor(offset / 36);
    const green = Math.floor(offset % 36 / 6);
    const blue = offset % 6;
    return `rgb(${ansiCubeLevel(red)}, ${ansiCubeLevel(green)}, ${ansiCubeLevel(blue)})`;
  }
  function ansiCubeLevel(value) {
    return value === 0 ? 0 : 55 + value * 40;
  }
  function appendControlMarker(parent, marker, kind, style) {
    const node = document.createElement("span");
    node.className = `ansi-control ansi-control-${kind}`;
    applyAnsiStyle(node, style);
    node.textContent = marker;
    parent.appendChild(node);
  }
  function appendStyledText(parent, text, style, options) {
    if (!text) {
      return;
    }
    const target = hasVisibleAnsiStyle(style) ? document.createElement("span") : parent;
    if (target instanceof HTMLElement) {
      target.className = "ansi-segment";
      applyAnsiStyle(target, style);
    }
    appendJsonHighlightedText(target, text, options);
    if (target !== parent) {
      parent.appendChild(target);
    }
  }
  function hasVisibleAnsiStyle(style) {
    return Boolean(style.fg || style.bg || style.bold || style.dim || style.italic || style.underline || style.inverse);
  }
  function applyAnsiStyle(node, style) {
    let fg = style.fg;
    let bg = style.bg;
    if (style.inverse) {
      const nextFg = bg || DEFAULT_BG;
      bg = fg || DEFAULT_FG;
      fg = nextFg;
    }
    if (fg) {
      node.style.color = fg;
    }
    if (bg) {
      node.style.backgroundColor = bg;
    }
    if (style.bold) {
      node.classList.add("ansi-bold");
    }
    if (style.dim) {
      node.classList.add("ansi-dim");
    }
    if (style.italic) {
      node.classList.add("ansi-italic");
    }
    if (style.underline) {
      node.classList.add("ansi-underline");
    }
  }
  function appendJsonHighlightedText(parent, text, options) {
    const fragments = findJsonFragments(text);
    if (fragments.length === 0) {
      appendHighlightedText(parent, text, options.highlight);
      return;
    }
    let cursor = 0;
    for (const fragment of fragments) {
      if (fragment.start > cursor) {
        appendHighlightedText(parent, text.slice(cursor, fragment.start), options.highlight);
      }
      if (options.prettyJson) {
        appendPrettyJsonBlock(parent, fragment.raw, options);
      } else {
        appendJsonTokens(parent, fragment.raw, options);
      }
      cursor = fragment.end;
    }
    if (cursor < text.length) {
      appendHighlightedText(parent, text.slice(cursor), options.highlight);
    }
  }
  function appendPrettyJsonBlock(parent, raw, options) {
    const node = document.createElement("span");
    node.className = "json-pretty-block";
    try {
      appendJsonTokens(node, JSON.stringify(JSON.parse(raw), null, 2), options);
    } catch {
      appendJsonTokens(node, raw, options);
    }
    parent.appendChild(node);
  }
  function findJsonFragments(text) {
    const fragments = [];
    let start = -1;
    let stack = [];
    let inString = false;
    let escaped = false;
    for (let index = 0; index < text.length; index += 1) {
      const ch = text[index] ?? "";
      if (start < 0) {
        if (ch === "{" || ch === "[") {
          start = index;
          stack = [ch];
          inString = false;
          escaped = false;
        }
        continue;
      }
      if (inString) {
        if (escaped) {
          escaped = false;
          continue;
        }
        if (ch === "\\") {
          escaped = true;
          continue;
        }
        if (ch === '"') {
          inString = false;
        }
        continue;
      }
      if (ch === '"') {
        inString = true;
        continue;
      }
      if (ch === "{" || ch === "[") {
        stack.push(ch);
        continue;
      }
      if (ch !== "}" && ch !== "]") {
        continue;
      }
      const opener = stack[stack.length - 1];
      if (opener === "{" && ch !== "}" || opener === "[" && ch !== "]") {
        start = -1;
        stack = [];
        continue;
      }
      stack.pop();
      if (stack.length > 0) {
        continue;
      }
      const end = index + 1;
      const raw = text.slice(start, end);
      if (raw.length <= MAX_JSON_FRAGMENT_CHARS && isValidJson(raw)) {
        fragments.push({ start, end, raw });
      }
      start = -1;
      stack = [];
    }
    return fragments;
  }
  function isValidJson(raw) {
    try {
      JSON.parse(raw);
      return true;
    } catch {
      return false;
    }
  }
  function appendJsonTokens(parent, raw, options) {
    let index = 0;
    while (index < raw.length) {
      const ch = raw[index] ?? "";
      if (isWhitespace(ch)) {
        const next = scanWhile(raw, index, isWhitespace);
        appendHighlightedText(parent, raw.slice(index, next), options.highlight);
        index = next;
        continue;
      }
      if (ch === '"') {
        const end = scanStringEnd(raw, index);
        const after = skipWhitespace(raw, end);
        appendToken(parent, raw.slice(index, end), raw[after] === ":" ? "key" : "string", options);
        index = end;
        continue;
      }
      if (isNumberStart(ch)) {
        const end = scanJsonNumberEnd(raw, index);
        appendToken(parent, raw.slice(index, end), "number", options);
        index = end;
        continue;
      }
      if (raw.startsWith("true", index)) {
        appendToken(parent, "true", "boolean", options);
        index += 4;
        continue;
      }
      if (raw.startsWith("false", index)) {
        appendToken(parent, "false", "boolean", options);
        index += 5;
        continue;
      }
      if (raw.startsWith("null", index)) {
        appendToken(parent, "null", "null", options);
        index += 4;
        continue;
      }
      appendToken(parent, ch, "punctuation", options);
      index += 1;
    }
  }
  function appendToken(parent, text, kind, options) {
    const node = document.createElement("span");
    node.className = `json-token json-token-${kind}`;
    appendHighlightedText(node, text, options.highlight);
    parent.appendChild(node);
  }
  function appendHighlightedText(parent, text, highlight) {
    if (!text) {
      return;
    }
    if (!highlight) {
      parent.appendChild(document.createTextNode(text));
      return;
    }
    if (highlight.kind === "line") {
      appendHighlightNode(parent, text);
      return;
    }
    const flags = highlight.flags.includes("g") ? highlight.flags : `${highlight.flags}g`;
    let pattern;
    try {
      pattern = new RegExp(highlight.source, flags);
    } catch {
      parent.appendChild(document.createTextNode(text));
      return;
    }
    let cursor = 0;
    for (const match of text.matchAll(pattern)) {
      const index = match.index;
      const value = match[0] ?? "";
      if (index === void 0 || value.length === 0) {
        continue;
      }
      if (index > cursor) {
        parent.appendChild(document.createTextNode(text.slice(cursor, index)));
      }
      appendHighlightNode(parent, value);
      cursor = index + value.length;
    }
    if (cursor < text.length) {
      parent.appendChild(document.createTextNode(text.slice(cursor)));
    }
  }
  function appendHighlightNode(parent, text) {
    const node = document.createElement("mark");
    node.className = "log-filter-match";
    node.textContent = text;
    parent.appendChild(node);
  }
  function scanStringEnd(raw, start) {
    let escaped = false;
    for (let index = start + 1; index < raw.length; index += 1) {
      const ch = raw[index] ?? "";
      if (escaped) {
        escaped = false;
        continue;
      }
      if (ch === "\\") {
        escaped = true;
        continue;
      }
      if (ch === '"') {
        return index + 1;
      }
    }
    return raw.length;
  }
  function scanJsonNumberEnd(raw, start) {
    let index = start;
    while (index < raw.length && /[-+0-9.eE]/.test(raw[index] ?? "")) {
      index += 1;
    }
    return index;
  }
  function scanWhile(raw, start, predicate) {
    let index = start;
    while (index < raw.length && predicate(raw[index] ?? "")) {
      index += 1;
    }
    return index;
  }
  function skipWhitespace(raw, start) {
    return scanWhile(raw, start, isWhitespace);
  }
  function isWhitespace(ch) {
    return ch === " " || ch === "	" || ch === "\n" || ch === "\r";
  }
  function isNumberStart(ch) {
    return ch === "-" || ch >= "0" && ch <= "9";
  }

  // framework_shells/ui/src/protocol.ts
  function isRecord2(value) {
    return typeof value === "object" && value !== null;
  }
  function isLogStreamName(value) {
    return value === "stdout" || value === "stderr";
  }
  function isJsonRpcVersion(value) {
    return value === "2.0";
  }
  function asString(value) {
    return typeof value === "string" ? value : void 0;
  }
  function asNullableString(value) {
    if (value === null) {
      return null;
    }
    return typeof value === "string" ? value : void 0;
  }
  function asNumber(value) {
    return typeof value === "number" && Number.isFinite(value) ? value : void 0;
  }
  function asNullableNumber(value) {
    if (value === null) {
      return null;
    }
    return asNumber(value);
  }
  function asBoolean(value) {
    return typeof value === "boolean" ? value : void 0;
  }
  function asStringArray(value) {
    if (!Array.isArray(value)) {
      return void 0;
    }
    const result = [];
    for (const item of value) {
      if (typeof item === "string") {
        result.push(item);
      }
    }
    return result;
  }
  function asObjectRecord(value) {
    return isRecord2(value) ? value : void 0;
  }
  function coerceDashboardShellStats(value) {
    const record = asObjectRecord(value);
    if (!record) {
      return void 0;
    }
    const result = {};
    const alive = asBoolean(record.alive);
    if (alive !== void 0) {
      result.alive = alive;
    }
    const uptime = asNullableNumber(record.uptime);
    if (uptime !== void 0) {
      result.uptime = uptime;
    }
    const cpuPercent = asNumber(record.cpu_percent);
    if (cpuPercent !== void 0) {
      result.cpu_percent = cpuPercent;
    }
    const memoryRss = asNumber(record.memory_rss);
    if (memoryRss !== void 0) {
      result.memory_rss = memoryRss;
    }
    return result;
  }
  function coerceDashboardShellCapabilities(value) {
    const record = asObjectRecord(value);
    if (!record) {
      return void 0;
    }
    const result = {};
    const backend = asString(record.backend);
    if (backend !== void 0) {
      result.backend = backend;
    }
    const stdinWrite = asBoolean(record.stdin_write);
    if (stdinWrite !== void 0) {
      result.stdin_write = stdinWrite;
    }
    const stdinEof = asBoolean(record.stdin_eof);
    if (stdinEof !== void 0) {
      result.stdin_eof = stdinEof;
    }
    const stdoutSubscribe = asBoolean(record.stdout_subscribe);
    if (stdoutSubscribe !== void 0) {
      result.stdout_subscribe = stdoutSubscribe;
    }
    const stdoutSubscribeBytes = asBoolean(record.stdout_subscribe_bytes);
    if (stdoutSubscribeBytes !== void 0) {
      result.stdout_subscribe_bytes = stdoutSubscribeBytes;
    }
    const stderrSubscribe = asBoolean(record.stderr_subscribe);
    if (stderrSubscribe !== void 0) {
      result.stderr_subscribe = stderrSubscribe;
    }
    const resize = asBoolean(record.resize);
    if (resize !== void 0) {
      result.resize = resize;
    }
    const reattach = asBoolean(record.reattach);
    if (reattach !== void 0) {
      result.reattach = reattach;
    }
    return result;
  }
  function coerceDashboardPipeRuntime(value) {
    const record = asObjectRecord(value);
    if (!record) {
      return void 0;
    }
    const result = {};
    const engine = asString(record.engine);
    if (engine !== void 0) {
      result.engine = engine;
    }
    const active = asBoolean(record.active);
    if (active !== void 0) {
      result.active = active;
    }
    const phase = asString(record.phase);
    if (phase !== void 0) {
      result.phase = phase;
    }
    return result;
  }
  function coerceDashboardShellPayload(value) {
    const record = asObjectRecord(value);
    if (!record) {
      return null;
    }
    const result = {};
    const id = asString(record.id);
    if (id !== void 0) {
      result.id = id;
    }
    const specId = asNullableString(record.spec_id);
    if (specId !== void 0) {
      result.spec_id = specId;
    }
    const command = asStringArray(record.command);
    if (command !== void 0) {
      result.command = command;
    }
    const label = asNullableString(record.label);
    if (label !== void 0) {
      result.label = label;
    }
    const subgroups = asStringArray(record.subgroups);
    if (subgroups !== void 0) {
      result.subgroups = subgroups;
    }
    const ui = asObjectRecord(record.ui);
    if (ui !== void 0) {
      result.ui = ui;
    }
    const debug = asObjectRecord(record.debug);
    if (debug !== void 0) {
      result.debug = debug;
    }
    const cwd = asString(record.cwd);
    if (cwd !== void 0) {
      result.cwd = cwd;
    }
    const pid = asNullableNumber(record.pid);
    if (pid !== void 0) {
      result.pid = pid;
    }
    const status = asString(record.status);
    if (status !== void 0) {
      result.status = status;
    }
    const createdAt = asNumber(record.created_at);
    if (createdAt !== void 0) {
      result.created_at = createdAt;
    }
    const updatedAt = asNumber(record.updated_at);
    if (updatedAt !== void 0) {
      result.updated_at = updatedAt;
    }
    const autostart = asBoolean(record.autostart);
    if (autostart !== void 0) {
      result.autostart = autostart;
    }
    const stdoutLog = asString(record.stdout_log);
    if (stdoutLog !== void 0) {
      result.stdout_log = stdoutLog;
    }
    const stderrLog = asString(record.stderr_log);
    if (stderrLog !== void 0) {
      result.stderr_log = stderrLog;
    }
    const ioMetadataLog = asNullableString(record.io_metadata_log);
    if (ioMetadataLog !== void 0) {
      result.io_metadata_log = ioMetadataLog;
    }
    const exitCode = asNullableNumber(record.exit_code);
    if (exitCode !== void 0) {
      result.exit_code = exitCode;
    }
    const envKeys = asStringArray(record.env_keys);
    if (envKeys !== void 0) {
      result.env_keys = envKeys;
    }
    const runId = asNullableString(record.run_id);
    if (runId !== void 0) {
      result.run_id = runId;
    }
    const launcherPid = asNullableNumber(record.launcher_pid);
    if (launcherPid !== void 0) {
      result.launcher_pid = launcherPid;
    }
    const adopted = asBoolean(record.adopted);
    if (adopted !== void 0) {
      result.adopted = adopted;
    }
    const backend = asString(record.backend);
    if (backend !== void 0) {
      result.backend = backend;
    }
    const usesPty = asBoolean(record.uses_pty);
    if (usesPty !== void 0) {
      result.uses_pty = usesPty;
    }
    const usesPipes = asBoolean(record.uses_pipes);
    if (usesPipes !== void 0) {
      result.uses_pipes = usesPipes;
    }
    const usesDtach = asBoolean(record.uses_dtach);
    if (usesDtach !== void 0) {
      result.uses_dtach = usesDtach;
    }
    const ptyMode = asString(record.pty_mode);
    if (ptyMode !== void 0) {
      result.pty_mode = ptyMode;
    }
    const runtimeId = asNullableString(record.runtime_id);
    if (runtimeId !== void 0) {
      result.runtime_id = runtimeId;
    }
    const appId = asNullableString(record.app_id);
    if (appId !== void 0) {
      result.app_id = appId;
    }
    const parentShellId = asNullableString(record.parent_shell_id);
    if (parentShellId !== void 0) {
      result.parent_shell_id = parentShellId;
    }
    const isAppWorker = asBoolean(record.is_app_worker);
    if (isAppWorker !== void 0) {
      result.is_app_worker = isAppWorker;
    }
    const stats = coerceDashboardShellStats(record.stats);
    if (stats !== void 0) {
      result.stats = stats;
    }
    const capabilities = coerceDashboardShellCapabilities(record.capabilities);
    if (capabilities !== void 0) {
      result.capabilities = capabilities;
    }
    const pipeRuntime = coerceDashboardPipeRuntime(record.pipe_runtime);
    if (pipeRuntime !== void 0) {
      result.pipe_runtime = pipeRuntime;
    }
    return result;
  }
  function coerceDashboardProcessPayload(value) {
    const record = asObjectRecord(value);
    if (!record) {
      return null;
    }
    const result = {};
    const pid = asNumber(record.pid);
    if (pid !== void 0) {
      result.pid = pid;
    }
    const parentPid = asNullableNumber(record.parent_pid);
    if (parentPid !== void 0) {
      result.parent_pid = parentPid;
    }
    const type = asString(record.type);
    if (type !== void 0) {
      result.type = type;
    }
    const label = asNullableString(record.label);
    if (label !== void 0) {
      result.label = label;
    }
    const shellId = asNullableString(record.shell_id);
    if (shellId !== void 0) {
      result.shell_id = shellId;
    }
    const metadata = asObjectRecord(record.metadata);
    if (metadata !== void 0) {
      result.metadata = metadata;
    }
    return result;
  }
  function coerceDashboardStatePayload(value) {
    const record = asObjectRecord(value);
    if (!record || !Array.isArray(record.shells) || !Array.isArray(record.processes)) {
      return null;
    }
    const shells = [];
    for (const shell of record.shells) {
      const parsed = coerceDashboardShellPayload(shell);
      if (parsed) {
        shells.push(parsed);
      }
    }
    const processes = [];
    for (const process of record.processes) {
      const parsed = coerceDashboardProcessPayload(process);
      if (parsed) {
        processes.push(parsed);
      }
    }
    return { shells, processes };
  }
  function coerceIoMetadataRecord(value) {
    const record = asObjectRecord(value);
    if (!record) {
      return null;
    }
    const shellId = asString(record.shell_id);
    const kind = asString(record.kind);
    const stream = asString(record.stream);
    if (!shellId || !["output", "stdin_write", "stdin_eof"].includes(kind ?? "")) {
      return null;
    }
    if (!stream || !["stdout", "stderr", "stdin"].includes(stream)) {
      return null;
    }
    const result = {
      shell_id: shellId,
      kind,
      stream
    };
    const schema = asString(record.schema);
    if (schema !== void 0) {
      result.schema = schema;
    }
    const ts = asNumber(record.ts);
    if (ts !== void 0) {
      result.ts = ts;
    }
    const source = asString(record.source);
    if (source !== void 0) {
      result.source = source;
    }
    const backend = asString(record.backend);
    if (backend !== void 0) {
      result.backend = backend;
    }
    const byteStart = asNumber(record.byte_start);
    if (byteStart !== void 0) {
      result.byte_start = byteStart;
    }
    const byteEnd = asNumber(record.byte_end);
    if (byteEnd !== void 0) {
      result.byte_end = byteEnd;
    }
    const byteCount = asNumber(record.byte_count);
    if (byteCount !== void 0) {
      result.byte_count = byteCount;
    }
    const appendNewline = asBoolean(record.append_newline);
    if (appendNewline !== void 0) {
      result.append_newline = appendNewline;
    }
    const newlineAppended = asBoolean(record.newline_appended);
    if (newlineAppended !== void 0) {
      result.newline_appended = newlineAppended;
    }
    const preview = asString(record.preview);
    if (preview !== void 0) {
      result.preview = preview;
    }
    const text = asString(record.text);
    if (text !== void 0) {
      result.text = text;
    }
    const previewTruncated = asBoolean(record.preview_truncated);
    if (previewTruncated !== void 0) {
      result.preview_truncated = previewTruncated;
    }
    const sha256 = asString(record.sha256);
    if (sha256 !== void 0) {
      result.sha256 = sha256;
    }
    return result;
  }
  function buildClientRequest(method, id, params) {
    return {
      jsonrpc: "2.0",
      id,
      method,
      params
    };
  }
  function coerceIncomingJsonRpcObject(parsed) {
    if (!isRecord2(parsed)) {
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
    if (typeof parsedId === "string" && isRecord2(parsedResult)) {
      const result = parsedResult;
      if (result.accepted === true) {
        const state = coerceDashboardStatePayload(result.state);
        if (state) {
          return {
            jsonrpc: "2.0",
            id: parsedId,
            result: { accepted: true, state }
          };
        }
        if (typeof result.shell_id === "string") {
          return {
            jsonrpc: "2.0",
            id: parsedId,
            result: { accepted: true, shell_id: result.shell_id }
          };
        }
        return null;
      }
      if (result.ok === true) {
        const state = coerceDashboardStatePayload(result.state);
        if (state) {
          return {
            jsonrpc: "2.0",
            id: parsedId,
            result: { ok: true, state }
          };
        }
        return {
          jsonrpc: "2.0",
          id: parsedId,
          result: { ok: true }
        };
      }
      return null;
    }
    if ((typeof parsedId === "string" || parsedId === null) && isRecord2(parsedError)) {
      const error = parsedError;
      if (typeof error.code !== "number" || typeof error.message !== "string") {
        return null;
      }
      const response = {
        jsonrpc: "2.0",
        id: typeof parsedId === "string" ? parsedId : null,
        error: {
          code: error.code,
          message: error.message
        }
      };
      if (isRecord2(error.data)) {
        const data = {};
        if (typeof error.data.code === "string") {
          data.code = error.data.code;
        }
        if (typeof error.data.shell_id === "string") {
          data.shell_id = error.data.shell_id;
        }
        if (Object.keys(data).length > 0) {
          response.error.data = data;
        }
      }
      return response;
    }
    if (typeof parsedMethod !== "string" || !isRecord2(parsedParams)) {
      return null;
    }
    switch (parsedMethod) {
      case "fws.shell.created":
      case "fws.shell.spawned":
      case "fws.shell.updated":
      case "fws.shell.exited": {
        const shell = coerceDashboardShellPayload(parsedParams.shell);
        if (!shell) {
          return null;
        }
        return {
          jsonrpc: "2.0",
          method: parsedMethod,
          params: { shell }
        };
      }
      case "fws.shell.removed":
        if (typeof parsedParams.shell_id === "string") {
          return {
            jsonrpc: "2.0",
            method: parsedMethod,
            params: { shell_id: parsedParams.shell_id }
          };
        }
        return null;
      case "fws.logs.initial":
        if (typeof parsedParams.shell_id === "string" && typeof parsedParams.stdout === "string" && typeof parsedParams.stderr === "string") {
          const ioMetadata = [];
          if (Array.isArray(parsedParams.io_metadata)) {
            for (const item of parsedParams.io_metadata) {
              const record = coerceIoMetadataRecord(item);
              if (record) {
                ioMetadata.push(record);
              }
            }
          }
          return {
            jsonrpc: "2.0",
            method: parsedMethod,
            params: {
              shell_id: parsedParams.shell_id,
              stdout: parsedParams.stdout,
              stderr: parsedParams.stderr,
              io_metadata: ioMetadata
            }
          };
        }
        return null;
      case "fws.logs.chunk":
        if (typeof parsedParams.shell_id === "string" && isLogStreamName(parsedParams.stream) && typeof parsedParams.chunk === "string") {
          return {
            jsonrpc: "2.0",
            method: parsedMethod,
            params: {
              shell_id: parsedParams.shell_id,
              stream: parsedParams.stream,
              chunk: parsedParams.chunk
            }
          };
        }
        return null;
      case "fws.logs.io_metadata": {
        const record = coerceIoMetadataRecord(parsedParams.record);
        if (typeof parsedParams.shell_id === "string" && record) {
          return {
            jsonrpc: "2.0",
            method: parsedMethod,
            params: {
              shell_id: parsedParams.shell_id,
              record
            }
          };
        }
        return null;
      }
      case "fws.logs.reset":
        if (typeof parsedParams.shell_id === "string" && isLogStreamName(parsedParams.stream)) {
          return {
            jsonrpc: "2.0",
            method: parsedMethod,
            params: {
              shell_id: parsedParams.shell_id,
              stream: parsedParams.stream
            }
          };
        }
        return null;
      case "fws.error":
        if (typeof parsedParams.message === "string") {
          const result = { message: parsedParams.message };
          if (typeof parsedParams.code === "string") {
            result.code = parsedParams.code;
          }
          if (typeof parsedParams.shell_id === "string") {
            result.shell_id = parsedParams.shell_id;
          }
          return {
            jsonrpc: "2.0",
            method: parsedMethod,
            params: result
          };
        }
        return null;
      default:
        return null;
    }
  }
  function coerceIncomingJsonRpcMessage(value) {
    return coerceIncomingJsonRpcObject(value);
  }

  // framework_shells/ui/src/fws.ts
  var LOG_STREAMS = ["stdout", "stderr"];
  var EXITED_EXPANDED_KEY = "fws.exited.expanded";
  var GROUP_EXPANDED_KEY = "fws.group.expanded";
  var LOG_RENDER_OPTIONS_KEY = "fws.log.render.options";
  var EXITED_PAGE_SIZE = 50;
  var CSS_COLOR_RE = /^[#()0-9a-zA-Z.,%\s-]+$/;
  var FWS_SOCKETIO_NAMESPACE = "/fws";
  var FWS_SOCKETIO_PATH = "/fws_ws/socket.io";
  function isRecord3(value) {
    return typeof value === "object" && value !== null;
  }
  function getElementById(id) {
    const element = document.getElementById(id);
    if (element === null) {
      return null;
    }
    return element;
  }
  function isElement(target) {
    return target instanceof Element;
  }
  function normalizeStoredBoolean(value) {
    return value === true || value === 1 || value === "1";
  }
  function parseStoredGroupExpanded(raw) {
    if (!raw) {
      return {};
    }
    try {
      const parsed = JSON.parse(raw);
      if (!isRecord3(parsed)) {
        return {};
      }
      const result = {};
      for (const [key, value] of Object.entries(parsed)) {
        result[key] = normalizeStoredBoolean(value);
      }
      return result;
    } catch {
      return {};
    }
  }
  function normalizeStreamRenderOptions(value) {
    if (!isRecord3(value)) {
      return { prettyJson: false };
    }
    return { prettyJson: normalizeStoredBoolean(value.prettyJson) };
  }
  function parseStoredLogRenderOptions(raw) {
    if (!raw) {
      return {};
    }
    try {
      const parsed = JSON.parse(raw);
      if (!isRecord3(parsed)) {
        return {};
      }
      const result = {};
      for (const [shellId, value] of Object.entries(parsed)) {
        if (!isRecord3(value)) {
          continue;
        }
        const shellOptions = {};
        if ("stdout" in value) {
          shellOptions.stdout = normalizeStreamRenderOptions(value.stdout);
        }
        if ("stderr" in value) {
          shellOptions.stderr = normalizeStreamRenderOptions(value.stderr);
        }
        if ("ioOverlay" in value) {
          shellOptions.ioOverlay = normalizeStoredBoolean(value.ioOverlay);
        }
        if (shellOptions.stdout || shellOptions.stderr || shellOptions.ioOverlay !== void 0) {
          result[shellId] = shellOptions;
        }
      }
      return result;
    } catch {
      return {};
    }
  }
  function getStoredStreamRenderOptions(store, shellId, stream) {
    return store[shellId]?.[stream] ?? { prettyJson: false };
  }
  function makeStreamState(containerId) {
    return {
      container: getElementById(containerId),
      entries: [],
      partial: "",
      pendingCount: 0,
      ansiStyle: createDefaultAnsiStyle(),
      prettyJson: false
    };
  }
  function escapeHtml(value) {
    return String(value ?? "").split("&").join("&amp;").split("<").join("&lt;").split(">").join("&gt;").split('"').join("&quot;").split("'").join("&#39;");
  }
  function fmtBytes(value) {
    if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
      return "0";
    }
    const mib = value / (1024 * 1024);
    if (mib >= 1024) {
      return `${(mib / 1024).toFixed(1)} GiB`;
    }
    return `${Math.round(mib)} MiB`;
  }
  function fmtCpu(value) {
    if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
      return "-";
    }
    return `${value.toFixed(1)}%`;
  }
  function shellBackend(info) {
    let backend = "";
    if (typeof info.backend === "string" && info.backend) {
      backend = info.backend;
    } else if (info.uses_dtach) {
      backend = "dtach";
    } else if (info.uses_pipes) {
      backend = "pipe";
    } else if (info.uses_pty) {
      backend = "pty";
    } else {
      backend = "proc";
    }
    const engine = info.pipe_runtime?.engine;
    if (backend === "pipe" && engine === "native-pipe") {
      return "pipe:native-pipe";
    }
    if (backend === "pipe" && engine === "native-terminal-pipe") {
      return "pipe:native-terminal-pipe";
    }
    if (backend === "pipe" && engine === "python-terminal-pipe") {
      return "pipe:python-terminal-pipe";
    }
    return backend;
  }
  function isShellLive(info) {
    if (info.status !== "running") {
      return false;
    }
    if (typeof info.pid !== "number" || info.pid <= 0) {
      return false;
    }
    if (info.stats?.alive === false) {
      return false;
    }
    return true;
  }
  function safeCssValue(value) {
    const text = String(value ?? "").trim();
    if (!text || !CSS_COLOR_RE.test(text)) {
      return "";
    }
    return text;
  }
  function globMatches(pattern, value) {
    const escaped = pattern.replace(/[.+^${}()|[\]\\]/g, "\\$&");
    const normalized = escaped.split("*").join(".*").split("?").join(".");
    try {
      return new RegExp(`^${normalized}$`).test(value);
    } catch {
      return false;
    }
  }
  function collectSubgroupStyles(shells) {
    const merged = {};
    for (const shell of shells) {
      if (!isRecord3(shell.ui)) {
        continue;
      }
      const raw = shell.ui.subgroup_styles ?? shell.ui.subgroupStyles;
      if (!isRecord3(raw)) {
        continue;
      }
      for (const [key, styleValue] of Object.entries(raw)) {
        if (!isRecord3(styleValue)) {
          continue;
        }
        const normalized = {};
        const bg = safeCssValue(styleValue.bg ?? styleValue.background);
        const border = safeCssValue(styleValue.border ?? styleValue.border_color ?? styleValue.borderColor);
        const color = safeCssValue(styleValue.color ?? styleValue.fg ?? styleValue.foreground);
        if (bg) {
          normalized.bg = bg;
        }
        if (border) {
          normalized.border = border;
        }
        if (color) {
          normalized.color = color;
        }
        if (Object.keys(normalized).length > 0) {
          merged[key] = normalized;
        }
      }
    }
    return merged;
  }
  function subgroupStyleFor(name, styles) {
    if (!name) {
      return {};
    }
    if (styles[name]) {
      return styles[name];
    }
    let bestKey = null;
    for (const pattern of Object.keys(styles)) {
      if (pattern === name) {
        bestKey = pattern;
        break;
      }
      if ((pattern.includes("*") || pattern.includes("?")) && globMatches(pattern, name)) {
        if (bestKey === null || pattern.length > bestKey.length) {
          bestKey = pattern;
        }
      }
    }
    return bestKey ? styles[bestKey] ?? {} : {};
  }
  function cardStyleForSubgroups(subgroups, styles) {
    if (subgroups.length === 0) {
      return {};
    }
    const preferred = subgroups.slice(1).concat(subgroups.slice(0, 1));
    for (const subgroup of preferred) {
      const style = subgroupStyleFor(subgroup, styles);
      if (Object.keys(style).length > 0) {
        return style;
      }
    }
    return {};
  }
  function renderSubgroupPills(subgroups, styles) {
    const pills = [];
    for (const subgroup of subgroups) {
      const name = subgroup.trim();
      if (!name) {
        continue;
      }
      const style = subgroupStyleFor(name, styles);
      const cssBits = [];
      if (style.bg) {
        cssBits.push(`background: ${style.bg};`);
      }
      if (style.border) {
        cssBits.push(`border-color: ${style.border};`);
      }
      if (style.color) {
        cssBits.push(`color: ${style.color};`);
      }
      const styleAttr = cssBits.length > 0 ? ` style="${cssBits.join(" ")}"` : "";
      pills.push(`<span class="pill"${styleAttr}>${escapeHtml(name)}</span>`);
    }
    if (pills.length === 0) {
      return "";
    }
    return `<div class="row">${pills.join("")}</div>`;
  }
  function renderCopyField(label, value, extraClasses = "") {
    const raw = String(value ?? "");
    const classes = extraClasses ? `copy-field ${extraClasses}` : "copy-field";
    return `<div class="${classes}" data-copy="${escapeHtml(raw)}" role="button" tabindex="0"><div class="copy-field-label">${escapeHtml(label)}</div><div class="copy-field-value">${escapeHtml(raw)}</div><button class="copy-overlay" type="button" aria-label="Copy field value">Copy</button></div>`;
  }
  function exitedTimestamp(shell) {
    if (typeof shell.updated_at === "number") {
      return shell.updated_at;
    }
    if (typeof shell.created_at === "number") {
      return shell.created_at;
    }
    return 0;
  }
  function fmtExitedTimestamp(timestamp) {
    if (!(timestamp > 0)) {
      return "Unknown time";
    }
    const dt = new Date(timestamp * 1e3);
    const now = /* @__PURE__ */ new Date();
    const sameDay = dt.getFullYear() === now.getFullYear() && dt.getMonth() === now.getMonth() && dt.getDate() === now.getDate();
    if (sameDay) {
      return dt.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", hour12: false });
    }
    return dt.toLocaleString([], {
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      hour12: false
    });
  }
  function hasLogPaths(shell) {
    return Boolean((shell.stdout_log ?? "").trim() || (shell.stderr_log ?? "").trim());
  }
  function renderExitedContent(exited, subgroupStyles) {
    if (exited.length === 0) {
      return '<div class="shell-card"><div class="shell-meta">No exited shells.</div></div>';
    }
    const parts = [];
    const sortedExited = exited.slice().sort((left, right) => exitedTimestamp(right) - exitedTimestamp(left));
    for (const shell of sortedExited) {
      const shellId = shell.id ?? "";
      const label = shell.label ?? shellId;
      const status = shell.status ?? "exited";
      const exitCode = shell.exit_code;
      const timestamp = exitedTimestamp(shell);
      const timeLabel = fmtExitedTimestamp(timestamp);
      const subgroups = shell.subgroups ?? [];
      const style = cardStyleForSubgroups(subgroups, subgroupStyles);
      const styleBits = [];
      if (style.bg) {
        styleBits.push(`background: ${style.bg};`);
      }
      if (style.border) {
        styleBits.push(`border-color: ${style.border}; border-left: 4px solid ${style.border};`);
      }
      const styleAttr = styleBits.length > 0 ? ` style="${styleBits.join(" ")}"` : "";
      const statusMeta = exitCode === null || exitCode === void 0 ? status : `${status} \xB7 exit: ${exitCode}`;
      const commandText = (shell.command ?? []).join(" ");
      parts.push(`<div class="exited-item" data-exited-item="1" data-exited-ts="${escapeHtml(timestamp)}">`);
      parts.push(`<div class="exited-ts">${escapeHtml(timeLabel)}</div>`);
      parts.push(`<div class="shell-card shell-entry is-collapsed"${styleAttr} data-shell-id="${escapeHtml(shellId)}">`);
      parts.push('<div class="shell-header">');
      parts.push(`<div class="shell-title">${escapeHtml(label)}</div>`);
      parts.push('<div class="shell-actions">');
      parts.push(`<button class="btn btn-small" type="button" data-collapse-toggle="${escapeHtml(shellId)}" aria-expanded="false">Expand</button>`);
      if (hasLogPaths(shell)) {
        parts.push(
          `<button class="btn btn-small" type="button" data-log-open="${escapeHtml(shellId)}" data-log-label="${escapeHtml(label)}">Logs</button>`
        );
      } else {
        parts.push('<button class="btn btn-small" type="button" disabled>Logs Purged</button>');
      }
      parts.push(
        `<form method="post" action="/fws/action/shell/${encodeURIComponent(shellId)}/purge" data-fws-ajax="1"><button class="btn btn-small" type="submit">Purge</button></form>`
      );
      parts.push("</div>");
      parts.push("</div>");
      parts.push(`<div class="shell-details" data-collapse-content="${escapeHtml(shellId)}">`);
      parts.push(renderCopyField("Status", statusMeta));
      parts.push(renderCopyField("ID", shellId));
      parts.push(renderCopyField("Command", commandText, "copy-field--multiline"));
      parts.push(renderCopyField("stdout log", shell.stdout_log ?? "", "copy-field--path"));
      parts.push(renderCopyField("stderr log", shell.stderr_log ?? "", "copy-field--path"));
      const pills = renderSubgroupPills(subgroups, subgroupStyles);
      if (pills) {
        parts.push(pills);
      }
      parts.push("</div>");
      parts.push("</div>");
      parts.push("</div>");
    }
    if (exited.length > EXITED_PAGE_SIZE) {
      parts.push('<div class="row exited-more-row">');
      parts.push('<button class="btn btn-small" type="button" id="fws-exited-more">More</button>');
      parts.push("</div>");
    }
    return parts.join("\n");
  }
  function renderDashboardContent(state) {
    const shellPidSet = /* @__PURE__ */ new Set();
    for (const shell of state.shells) {
      if (typeof shell.pid === "number") {
        shellPidSet.add(shell.pid);
      }
    }
    const childrenByParent = /* @__PURE__ */ new Map();
    for (const process of state.processes) {
      if (typeof process.parent_pid !== "number") {
        continue;
      }
      const siblings = childrenByParent.get(process.parent_pid) ?? [];
      siblings.push(process);
      childrenByParent.set(process.parent_pid, siblings);
    }
    const running = state.shells.filter((shell) => isShellLive(shell));
    const exited = state.shells.filter((shell) => !isShellLive(shell));
    const subgroupStyles = collectSubgroupStyles(state.shells);
    const parts = [];
    parts.push('<div class="section">');
    parts.push(`<div class="section-title">Running <span class="muted">(${running.length})</span></div>`);
    if (running.length === 0) {
      parts.push('<div class="shell-card"><div class="shell-meta">No running shells.</div></div>');
    } else {
      const groups = /* @__PURE__ */ new Map();
      for (const shell of running) {
        const normalized = (shell.subgroups ?? []).map((value) => value.trim()).filter((value) => value.length > 0);
        const umbrella = normalized[0] ?? "(ungrouped)";
        const subgroup = normalized[1] ?? "(root)";
        const subgroupMap = groups.get(umbrella) ?? /* @__PURE__ */ new Map();
        const shells = subgroupMap.get(subgroup) ?? [];
        shells.push(shell);
        subgroupMap.set(subgroup, shells);
        groups.set(umbrella, subgroupMap);
      }
      const umbrellas = Array.from(groups.keys()).sort((left, right) => {
        if (left === "(ungrouped)") {
          return 1;
        }
        if (right === "(ungrouped)") {
          return -1;
        }
        return left.localeCompare(right);
      });
      for (const umbrella of umbrellas) {
        const subgroupMap = groups.get(umbrella) ?? /* @__PURE__ */ new Map();
        const totalShells = Array.from(subgroupMap.values()).reduce((sum, shells) => sum + shells.length, 0);
        parts.push(`<div class="group-card is-collapsed" data-group-id="${escapeHtml(umbrella)}">`);
        parts.push('<div class="group-header">');
        parts.push(`<div class="group-title">${escapeHtml(umbrella)}</div>`);
        parts.push('<div class="shell-actions">');
        parts.push(
          `<button class="btn btn-small" type="button" data-group-toggle="${escapeHtml(umbrella)}" aria-expanded="false">Expand</button>`
        );
        if (umbrella !== "(ungrouped)") {
          parts.push(
            `<form method="post" action="/fws/action/app/${encodeURIComponent(umbrella)}/shutdown" data-fws-ajax="1"><button class="btn btn-small btn-danger" type="submit">Shutdown Group</button></form>`
          );
        }
        parts.push("</div>");
        parts.push("</div>");
        parts.push(`<div class="group-meta">Shells: ${escapeHtml(totalShells)} \xB7 Subgroups: ${escapeHtml(subgroupMap.size)}</div>`);
        parts.push(`<div class="group-content" data-group-content="${escapeHtml(umbrella)}">`);
        const subgroups = Array.from(subgroupMap.keys()).sort((left, right) => {
          if (left === "app-worker") {
            return -1;
          }
          if (right === "app-worker") {
            return 1;
          }
          return left.localeCompare(right);
        });
        for (const subgroup of subgroups) {
          const style = subgroupStyleFor(subgroup, subgroupStyles);
          const styleBits = [];
          if (style.bg) {
            styleBits.push(`background: ${style.bg};`);
          }
          if (style.border) {
            styleBits.push(`border-color: ${style.border}; border-left: 4px solid ${style.border};`);
          }
          const styleAttr = styleBits.length > 0 ? ` style="${styleBits.join(" ")}"` : "";
          const shellsInGroup = (subgroupMap.get(subgroup) ?? []).slice().sort((left, right) => {
            const leftLabel = left.label ?? "";
            const rightLabel = right.label ?? "";
            const leftRank = leftLabel.startsWith("app-worker:") ? 0 : 1;
            const rightRank = rightLabel.startsWith("app-worker:") ? 0 : 1;
            if (leftRank !== rightRank) {
              return leftRank - rightRank;
            }
            const labelCompare = leftLabel.localeCompare(rightLabel);
            if (labelCompare !== 0) {
              return labelCompare;
            }
            return (left.id ?? "").localeCompare(right.id ?? "");
          });
          parts.push(`<div class="subgroup-card"${styleAttr}>`);
          parts.push('<div class="subgroup-header">');
          parts.push(`<div class="subgroup-title">${escapeHtml(subgroup)}</div>`);
          parts.push(`<div class="subgroup-count muted">(${shellsInGroup.length})</div>`);
          parts.push("</div>");
          for (const shell of shellsInGroup) {
            const shellId = shell.id ?? "";
            const label = shell.label ?? shellId;
            const pid = shell.pid;
            const subgroupsForShell = shell.subgroups ?? [];
            const rowStyle = cardStyleForSubgroups(subgroupsForShell, subgroupStyles);
            const rowStyleBits = [];
            if (rowStyle.bg) {
              rowStyleBits.push(`background: ${rowStyle.bg};`);
            }
            if (rowStyle.border) {
              rowStyleBits.push(`border-left: 3px solid ${rowStyle.border};`);
            }
            const rowStyleAttr = rowStyleBits.length > 0 ? ` style="${rowStyleBits.join(" ")}"` : "";
            const commandText = (shell.command ?? []).join(" ");
            const cpu = fmtCpu(shell.stats?.cpu_percent);
            const rss = fmtBytes(shell.stats?.memory_rss);
            const status = shell.status ?? "running";
            parts.push(`<div class="shell-card shell-entry is-collapsed"${rowStyleAttr} data-shell-id="${escapeHtml(shellId)}">`);
            parts.push('<div class="shell-header">');
            parts.push(`<div class="shell-title">${escapeHtml(label)}</div>`);
            parts.push('<div class="shell-actions">');
            parts.push(`<button class="btn btn-small" type="button" data-collapse-toggle="${escapeHtml(shellId)}" aria-expanded="false">Expand</button>`);
            parts.push(
              `<button class="btn btn-small" type="button" data-log-open="${escapeHtml(shellId)}" data-log-label="${escapeHtml(label)}">Logs</button>`
            );
            parts.push(
              `<form method="post" action="/fws/action/shell/${encodeURIComponent(shellId)}/terminate" data-fws-ajax="1"><button class="btn btn-small btn-danger" type="submit">Stop</button></form>`
            );
            parts.push("</div>");
            parts.push("</div>");
            parts.push(`<div class="shell-details" data-collapse-content="${escapeHtml(shellId)}">`);
            parts.push(renderCopyField("Status", status));
            parts.push(renderCopyField("PID", pid ?? ""));
            parts.push(renderCopyField("ID", shellId));
            parts.push(renderCopyField("Backend", shellBackend(shell)));
            parts.push(renderCopyField("CPU", cpu));
            parts.push(renderCopyField("RSS", rss));
            parts.push(renderCopyField("Command", commandText, "copy-field--multiline"));
            parts.push(renderCopyField("stdout log", shell.stdout_log ?? "", "copy-field--path"));
            parts.push(renderCopyField("stderr log", shell.stderr_log ?? "", "copy-field--path"));
            const pills = renderSubgroupPills(subgroupsForShell, subgroupStyles);
            if (pills) {
              parts.push(pills);
            }
            if (typeof pid === "number" && childrenByParent.has(pid)) {
              const children = (childrenByParent.get(pid) ?? []).filter((child) => {
                return typeof child.pid !== "number" || !shellPidSet.has(child.pid);
              });
              if (children.length > 0) {
                const sortedChildren = children.slice().sort((left, right) => {
                  const typeCompare = (left.type ?? "").localeCompare(right.type ?? "");
                  if (typeCompare !== 0) {
                    return typeCompare;
                  }
                  return (left.pid ?? 0) - (right.pid ?? 0);
                });
                parts.push('<div class="children">');
                parts.push(`<div class="children-title">Child Processes (${sortedChildren.length})</div>`);
                for (const child of sortedChildren) {
                  const childPid = child.pid ?? "";
                  const childType = child.type ?? "proc";
                  const childLabel = child.label ?? childPid;
                  parts.push('<div class="child-row child-row--proc">');
                  parts.push('<div class="child-main">');
                  parts.push(`<div class="child-label">${escapeHtml(childLabel)}</div>`);
                  parts.push('<div class="child-meta-line">');
                  parts.push(`<div class="child-meta">PID: ${escapeHtml(childPid)} \xB7 ${escapeHtml(childType)}</div>`);
                  parts.push('<div class="row child-actions-inline">');
                  parts.push(
                    `<form method="post" action="/fws/action/pid/${encodeURIComponent(String(childPid))}/terminate" data-fws-ajax="1"><button class="btn btn-small btn-danger" type="submit">Kill</button></form>`
                  );
                  parts.push("</div>");
                  parts.push("</div>");
                  parts.push("</div>");
                  parts.push("</div>");
                }
                parts.push("</div>");
              }
            }
            parts.push("</div>");
            parts.push("</div>");
          }
          parts.push("</div>");
        }
        parts.push("</div>");
        parts.push("</div>");
      }
    }
    parts.push("</div>");
    parts.push('<div class="section section-exited" id="fws-exited">');
    parts.push('<div class="section-title">');
    parts.push(`Exited <span class="muted">(${exited.length})</span>`);
    parts.push('<div class="shell-actions">');
    parts.push('<button class="btn btn-small" type="button" id="fws-exited-toggle" aria-expanded="false">Expand Exited</button>');
    if (exited.length > 0) {
      parts.push(
        '<form method="post" action="/fws/action/exited/purge" data-fws-ajax="1" data-confirm="Purge ALL exited shells (delete their logs + metadata)?"><button class="btn btn-small btn-danger" type="submit">Purge Exited</button></form>'
      );
    }
    parts.push("</div>");
    parts.push("</div>");
    parts.push(`<div class="exited-content is-collapsed" id="fws-exited-content" data-count="${escapeHtml(exited.length)}">`);
    parts.push(renderExitedContent(exited, subgroupStyles));
    parts.push("</div>");
    parts.push("</div>");
    return parts.join("\n");
  }
  (() => {
    void initFwsConsoleBridge();
    const content = getElementById("fws-content");
    const statusEl = getElementById("fws-status");
    const toggleAllBtn = getElementById("fws-toggle-all");
    const logDrawer = getElementById("fws-log-drawer");
    const logBackBtn = getElementById("fws-log-back");
    const logTitleEl = getElementById("fws-log-title");
    const logSubtitleEl = getElementById("fws-log-subtitle");
    const logStatusEl = getElementById("fws-log-status");
    const logPauseInput = getElementById("fws-log-pause");
    const stdinForm = getElementById("fws-stdin-form");
    const stdinInput = getElementById("fws-stdin-input");
    const stdinNewlineInput = getElementById("fws-stdin-newline");
    const stdinSendButton = getElementById("fws-stdin-send");
    const stdinJsonCompactButton = getElementById("fws-stdin-json-compact");
    const stdinStatusEl = getElementById("fws-stdin-status");
    const ioOverlayInput = getElementById("fws-io-overlay");
    const ioOverlayWrap = getElementById("fws-io-overlay-wrap");
    const collapseState = /* @__PURE__ */ new Map();
    let defaultCollapsed = true;
    let groupExpanded = parseStoredGroupExpanded(window.localStorage.getItem(GROUP_EXPANDED_KEY));
    let logRenderOptions = parseStoredLogRenderOptions(window.localStorage.getItem(LOG_RENDER_OPTIONS_KEY));
    let exitedVisibleCount = EXITED_PAGE_SIZE;
    let dashboardRequestCounter = 0;
    let dashboardState = { shells: [], processes: [] };
    let fwsSocket = null;
    const fwsSocketReady = connectSocketIo(FWS_SOCKETIO_NAMESPACE, {
      path: FWS_SOCKETIO_PATH,
      transports: ["websocket"]
    });
    const logState = {
      shellId: "",
      shellLabel: "",
      paused: false,
      ioOverlayEnabled: false,
      streams: {
        stdout: makeStreamState("stdout-container"),
        stderr: makeStreamState("stderr-container")
      }
    };
    function nextDashboardRequestId() {
      dashboardRequestCounter += 1;
      return `fws_req_${dashboardRequestCounter}`;
    }
    async function getFwsSocket() {
      if (fwsSocket) {
        return fwsSocket;
      }
      fwsSocket = await fwsSocketReady;
      return fwsSocket;
    }
    function isJsonRpcErrorMessage(message) {
      return "error" in message;
    }
    function isJsonRpcResponseMessage(message) {
      return "id" in message && typeof message.id === "string";
    }
    function isServerNotificationMessage(message) {
      return "method" in message && "params" in message;
    }
    function hasDashboardStateResult(result) {
      return isRecord3(result) && isRecord3(result.state) && Array.isArray(result.state.shells) && Array.isArray(result.state.processes);
    }
    function findShellLabel(shellId) {
      const match = dashboardState.shells.find((shell) => shell.id === shellId);
      return match?.label ?? shellId;
    }
    function findShell(shellId) {
      return dashboardState.shells.find((shell) => shell.id === shellId);
    }
    function shellHasIoMetadata(shellId) {
      const shell = findShell(shellId);
      if (!shell) {
        return false;
      }
      const debug = shell.debug;
      const enabled = normalizeStoredBoolean(debug?.io_metadata) || normalizeStoredBoolean(debug?.ioMetadata);
      return enabled === true;
    }
    function syncIoOverlayToggle() {
      const available = Boolean(logState.shellId && shellHasIoMetadata(logState.shellId));
      if (ioOverlayWrap) {
        ioOverlayWrap.classList.toggle("is-disabled", !available);
        ioOverlayWrap.title = available ? "Show stdin/timing sidecar overlay" : "Shell debug.io_metadata is not enabled.";
      }
      if (ioOverlayInput) {
        ioOverlayInput.disabled = !available;
        ioOverlayInput.checked = available && logState.ioOverlayEnabled;
      }
    }
    function setStdinInjectorDisabled(disabled) {
      stdinForm?.classList.toggle("is-disabled", disabled);
      if (stdinInput) {
        stdinInput.disabled = disabled;
      }
      if (stdinNewlineInput) {
        stdinNewlineInput.disabled = disabled;
      }
      if (stdinSendButton) {
        stdinSendButton.disabled = disabled;
      }
      if (stdinJsonCompactButton) {
        stdinJsonCompactButton.disabled = disabled;
      }
    }
    function canAttemptShellInput(shell) {
      if (!shell || !isShellLive(shell)) {
        return false;
      }
      const backend = shellBackend(shell);
      return backend === "pty" || backend === "dtach" || backend.startsWith("pipe");
    }
    function updateStdinInjectorState(statusOverride) {
      const shell = logState.shellId ? findShell(logState.shellId) : void 0;
      const capabilities = shell?.capabilities;
      const canWrite = capabilities?.stdin_write === true;
      const canAttemptWrite = canWrite || canAttemptShellInput(shell);
      setStdinInjectorDisabled(!canAttemptWrite);
      if (!stdinStatusEl) {
        return;
      }
      if (statusOverride) {
        stdinStatusEl.textContent = statusOverride;
        return;
      }
      if (!logState.shellId) {
        stdinStatusEl.textContent = "Select a shell with live stdin.";
        return;
      }
      if (!shell) {
        stdinStatusEl.textContent = "Shell metadata unavailable.";
        return;
      }
      if (!canAttemptWrite) {
        stdinStatusEl.textContent = "Stdin writes are not supported for this shell backend.";
        return;
      }
      const backend = shellBackend(shell);
      if (!canWrite) {
        stdinStatusEl.textContent = `Ready to attempt ${backend} stdin; backend will return any write error.`;
        return;
      }
      stdinStatusEl.textContent = `Ready for ${backend} stdin.`;
    }
    function compareShells(left, right) {
      const leftCreated = typeof left.created_at === "number" ? left.created_at : 0;
      const rightCreated = typeof right.created_at === "number" ? right.created_at : 0;
      if (leftCreated !== rightCreated) {
        return leftCreated - rightCreated;
      }
      return (left.id ?? "").localeCompare(right.id ?? "");
    }
    function pruneProcessesForShell(processes, shellId, rootPid) {
      const blockedPids = /* @__PURE__ */ new Set();
      for (const process of processes) {
        if (process.shell_id === shellId && typeof process.pid === "number") {
          blockedPids.add(process.pid);
        }
      }
      if (typeof rootPid === "number") {
        blockedPids.add(rootPid);
      }
      if (blockedPids.size > 0) {
        const queue = Array.from(blockedPids);
        while (queue.length > 0) {
          const parentPid = queue.shift();
          if (parentPid === void 0) {
            continue;
          }
          for (const process of processes) {
            if (process.parent_pid !== parentPid || typeof process.pid !== "number" || blockedPids.has(process.pid)) {
              continue;
            }
            blockedPids.add(process.pid);
            queue.push(process.pid);
          }
        }
      }
      return processes.filter((process) => {
        if (process.shell_id === shellId) {
          return false;
        }
        return typeof process.pid !== "number" || !blockedPids.has(process.pid);
      });
    }
    function applyShellDelta(nextShell) {
      const shellId = String(nextShell.id || "").trim();
      if (!shellId) {
        return;
      }
      const previousShell = dashboardState.shells.find((shell) => shell.id === shellId);
      const nextShells = dashboardState.shells.filter((shell) => shell.id !== shellId);
      nextShells.push(nextShell);
      let nextProcesses = dashboardState.processes.slice();
      const previousPid = typeof previousShell?.pid === "number" ? previousShell.pid : void 0;
      const nextPid = typeof nextShell.pid === "number" ? nextShell.pid : void 0;
      if (previousPid !== nextPid || !isShellLive(nextShell)) {
        nextProcesses = pruneProcessesForShell(nextProcesses, shellId, previousPid ?? nextPid);
      }
      applyDashboardState({
        shells: nextShells.sort(compareShells),
        processes: nextProcesses
      });
      setStatus("Live", true);
    }
    function removeShellDelta(shellId) {
      const normalizedShellId = String(shellId || "").trim();
      if (!normalizedShellId) {
        return;
      }
      const previousShell = dashboardState.shells.find((shell) => shell.id === normalizedShellId);
      if (!previousShell) {
        return;
      }
      const previousPid = typeof previousShell.pid === "number" ? previousShell.pid : void 0;
      applyDashboardState({
        shells: dashboardState.shells.filter((shell) => shell.id !== normalizedShellId),
        processes: pruneProcessesForShell(dashboardState.processes, normalizedShellId, previousPid)
      });
      setStatus("Live", true);
    }
    function applyDashboardState(nextState) {
      dashboardState = nextState;
      if (content) {
        content.innerHTML = renderDashboardContent(nextState);
        applyCollapseState(content);
        applyGroupState(content);
        applyExitedSectionState();
      }
      if (logState.shellId) {
        const label = findShellLabel(logState.shellId);
        logState.shellLabel = label;
        if (logTitleEl) {
          logTitleEl.textContent = label || "Shell Logs";
        }
      }
      updateStdinInjectorState();
      syncIoOverlayToggle();
    }
    function routeDashboardNotification(message) {
      switch (message.method) {
        case "fws.shell.created":
        case "fws.shell.spawned":
        case "fws.shell.updated":
        case "fws.shell.exited":
          applyShellDelta(message.params.shell);
          return;
        case "fws.shell.removed":
          removeShellDelta(message.params.shell_id);
          return;
        case "fws.error":
          if (!message.params.shell_id) {
            setStatus(message.params.message, false);
          }
          return;
        default:
          return;
      }
    }
    async function sendDashboardRequest(method, params) {
      const requestId = nextDashboardRequestId();
      const request = buildClientRequest(method, requestId, params);
      const socket = await getFwsSocket();
      return await new Promise((resolve, reject) => {
        socket.emit("fws_request", request, (payload) => {
          const message = coerceIncomingJsonRpcMessage(payload);
          if (!message || !isJsonRpcResponseMessage(message)) {
            reject(new Error(`Invalid response for ${method}`));
            return;
          }
          if (isJsonRpcErrorMessage(message)) {
            reject(new Error(message.error.message));
            return;
          }
          resolve(message.result);
        });
      });
    }
    async function submitActionForm(form) {
      const action = form.getAttribute("action") || window.location.href;
      const url = new URL(action, window.location.href);
      const path = url.pathname;
      const formData = new FormData(form);
      if (path === "/fws/action/refresh") {
        const result = await sendDashboardRequest("fws.dashboard.refresh", {});
        if (hasDashboardStateResult(result)) {
          applyDashboardState(result.state);
        }
        return;
      }
      if (path === "/fws/action/logs/purge") {
        await sendDashboardRequest("fws.logs.truncate", {});
        return;
      }
      if (path === "/fws/action/exited/purge") {
        await sendDashboardRequest("fws.exited.purge", {});
        return;
      }
      const shellTerminateMatch = path.match(/^\/fws\/action\/shell\/([^/]+)\/terminate$/);
      if (shellTerminateMatch) {
        await sendDashboardRequest("fws.shell.terminate", { shell_id: decodeURIComponent(shellTerminateMatch[1] ?? "") });
        return;
      }
      const shellPurgeMatch = path.match(/^\/fws\/action\/shell\/([^/]+)\/purge$/);
      if (shellPurgeMatch) {
        await sendDashboardRequest("fws.shell.purge", { shell_id: decodeURIComponent(shellPurgeMatch[1] ?? "") });
        return;
      }
      const pidTerminateMatch = path.match(/^\/fws\/action\/pid\/([^/]+)\/terminate$/);
      if (pidTerminateMatch) {
        const pid = Number.parseInt(decodeURIComponent(pidTerminateMatch[1] ?? ""), 10);
        if (Number.isFinite(pid)) {
          await sendDashboardRequest("fws.pid.terminate", { pid });
        }
        return;
      }
      const appShutdownMatch = path.match(/^\/fws\/action\/app\/([^/]+)\/shutdown$/);
      if (appShutdownMatch) {
        await sendDashboardRequest("fws.app.shutdown", { app_id: decodeURIComponent(appShutdownMatch[1] ?? "") });
        return;
      }
      if (path === "/fws/action/shutdown") {
        const scopeValue = String(formData.get("scope") ?? "tree");
        if (scopeValue !== "tree") {
          throw new Error(`Unsupported shutdown scope: ${scopeValue}`);
        }
        await sendDashboardRequest("fws.shutdown", { scope: "tree" });
      }
    }
    async function copyText(value) {
      const text = String(value ?? "");
      if (!text) {
        return;
      }
      try {
        await navigator.clipboard.writeText(text);
        return;
      } catch {
      }
      const el = document.createElement("textarea");
      el.value = text;
      el.style.position = "fixed";
      el.style.opacity = "0";
      document.body.appendChild(el);
      el.focus();
      el.select();
      try {
        document.execCommand("copy");
      } catch {
      } finally {
        document.body.removeChild(el);
      }
    }
    function flashCopied(field) {
      if (!(field instanceof HTMLElement)) {
        return;
      }
      field.classList.add("is-copied");
      window.setTimeout(() => field.classList.remove("is-copied"), 500);
    }
    function setStatus(text, connected) {
      if (!statusEl) {
        return;
      }
      statusEl.textContent = text;
      statusEl.classList.toggle("disconnected", !connected);
    }
    function setLogStatus(text, connected) {
      if (!logStatusEl) {
        return;
      }
      logStatusEl.textContent = text;
      logStatusEl.classList.toggle("disconnected", !connected);
    }
    function setCardCollapsed(card, collapsed) {
      if (!(card instanceof HTMLElement)) {
        return;
      }
      card.classList.toggle("is-collapsed", collapsed);
      const btn = card.querySelector("[data-collapse-toggle]");
      if (btn) {
        btn.setAttribute("aria-expanded", collapsed ? "false" : "true");
        btn.textContent = collapsed ? "Expand" : "Collapse";
      }
    }
    function applyCollapseState(root) {
      if (!root) {
        return;
      }
      const cards = root.querySelectorAll("[data-shell-id]");
      const visibleIds = /* @__PURE__ */ new Set();
      cards.forEach((card) => {
        const id = card.getAttribute("data-shell-id") || "";
        if (!id) {
          return;
        }
        visibleIds.add(id);
        const collapsed = collapseState.has(id) ? collapseState.get(id) === true : defaultCollapsed;
        collapseState.set(id, collapsed);
        setCardCollapsed(card, collapsed);
      });
      for (const key of Array.from(collapseState.keys())) {
        if (!visibleIds.has(key)) {
          collapseState.delete(key);
        }
      }
      updateToggleAllLabel();
    }
    function updateToggleAllLabel() {
      if (!toggleAllBtn || !content) {
        return;
      }
      const cards = content.querySelectorAll("[data-shell-id]");
      if (cards.length === 0) {
        toggleAllBtn.disabled = true;
        toggleAllBtn.textContent = "Expand All";
        return;
      }
      toggleAllBtn.disabled = false;
      const allCollapsed = Array.from(cards).every((card) => card.classList.contains("is-collapsed"));
      toggleAllBtn.textContent = allCollapsed ? "Expand All" : "Collapse All";
    }
    function setAllCollapsed(collapsed) {
      defaultCollapsed = collapsed;
      if (!content) {
        return;
      }
      const cards = content.querySelectorAll("[data-shell-id]");
      cards.forEach((card) => {
        const id = card.getAttribute("data-shell-id") || "";
        if (id) {
          collapseState.set(id, collapsed);
        }
        setCardCollapsed(card, collapsed);
      });
      updateToggleAllLabel();
    }
    function persistGroupExpanded() {
      try {
        window.localStorage.setItem(GROUP_EXPANDED_KEY, JSON.stringify(groupExpanded));
      } catch {
      }
    }
    function setGroupCollapsed(card, collapsed) {
      if (!(card instanceof HTMLElement)) {
        return;
      }
      card.classList.toggle("is-collapsed", collapsed);
      const btn = card.querySelector("[data-group-toggle]");
      if (btn) {
        btn.setAttribute("aria-expanded", collapsed ? "false" : "true");
        btn.textContent = collapsed ? "Expand" : "Collapse";
      }
    }
    function applyGroupState(root) {
      if (!root) {
        return;
      }
      const cards = root.querySelectorAll("[data-group-id]");
      cards.forEach((card) => {
        const id = card.getAttribute("data-group-id") || "";
        if (!id) {
          return;
        }
        const expanded = groupExpanded[id] === true;
        setGroupCollapsed(card, !expanded);
      });
    }
    function getExitedExpandedDefault() {
      try {
        return window.localStorage.getItem(EXITED_EXPANDED_KEY) === "1";
      } catch {
        return false;
      }
    }
    function setExitedExpanded(expanded) {
      if (!content) {
        return;
      }
      const exitedContent = content.querySelector("#fws-exited-content");
      const exitedToggle = content.querySelector("#fws-exited-toggle");
      if (!exitedContent || !exitedToggle) {
        return;
      }
      exitedContent.classList.toggle("is-collapsed", !expanded);
      exitedToggle.setAttribute("aria-expanded", expanded ? "true" : "false");
      exitedToggle.textContent = expanded ? "Collapse Exited" : "Expand Exited";
      try {
        window.localStorage.setItem(EXITED_EXPANDED_KEY, expanded ? "1" : "0");
      } catch {
      }
    }
    function applyExitedPagination() {
      if (!content) {
        return;
      }
      const exitedContent = content.querySelector("#fws-exited-content");
      if (!exitedContent) {
        return;
      }
      const items = Array.from(exitedContent.querySelectorAll('[data-exited-item="1"]'));
      const moreBtn = exitedContent.querySelector("#fws-exited-more");
      if (items.length === 0) {
        if (moreBtn) {
          moreBtn.style.display = "none";
        }
        return;
      }
      items.forEach((item, idx) => {
        item.style.display = idx < exitedVisibleCount ? "" : "none";
      });
      if (!moreBtn) {
        return;
      }
      if (items.length <= exitedVisibleCount) {
        moreBtn.style.display = "none";
        return;
      }
      moreBtn.style.display = "";
      moreBtn.textContent = `More (${items.length - exitedVisibleCount})`;
    }
    function applyExitedSectionState() {
      const expanded = getExitedExpandedDefault();
      setExitedExpanded(expanded);
      applyExitedPagination();
    }
    function hasActiveFilters(stream) {
      const includeInput = getElementById(`${stream}-include-input`);
      const excludeInput = getElementById(`${stream}-exclude-input`);
      return Boolean((includeInput?.value || "").trim() || (excludeInput?.value || "").trim());
    }
    function compileMatcher(stream, kind, query, mode) {
      const input = getElementById(`${stream}-${kind}-input`);
      input?.classList.remove("invalid");
      if (!query) {
        return () => kind === "include";
      }
      if (mode === "exact") {
        return (line) => line === query;
      }
      try {
        const re = new RegExp(query);
        return (line) => re.test(line);
      } catch {
        input?.classList.add("invalid");
        return () => kind !== "include";
      }
    }
    function getFilterConfig(stream) {
      const includeInput = getElementById(`${stream}-include-input`);
      const excludeInput = getElementById(`${stream}-exclude-input`);
      const includeMode = document.querySelector(`input[name="${stream}-include-mode"]:checked`);
      const excludeMode = document.querySelector(`input[name="${stream}-exclude-mode"]:checked`);
      return {
        includeQuery: (includeInput?.value || "").trim(),
        excludeQuery: (excludeInput?.value || "").trim(),
        includeMode: includeMode?.value === "exact" ? "exact" : "regex",
        excludeMode: excludeMode?.value === "exact" ? "exact" : "regex"
      };
    }
    function getFilteredEntries(stream) {
      const state = logState.streams[stream];
      const cfg = getFilterConfig(stream);
      const includeMatch = compileMatcher(stream, "include", cfg.includeQuery, cfg.includeMode);
      const excludeMatch = compileMatcher(stream, "exclude", cfg.excludeQuery, cfg.excludeMode);
      const allEntries = state.partial ? state.entries.concat([{ kind: "text", text: state.partial }]) : state.entries.slice();
      return allEntries.filter((entry) => {
        if (entry.kind === "io") {
          return stream === "stdout" && logState.ioOverlayEnabled;
        }
        const includeOk = cfg.includeQuery ? includeMatch(entry.text) : true;
        const excludeHit = cfg.excludeQuery ? excludeMatch(entry.text) : false;
        return includeOk && !excludeHit;
      });
    }
    function getFilterHighlight(stream) {
      const cfg = getFilterConfig(stream);
      if (!cfg.includeQuery) {
        return void 0;
      }
      if (cfg.includeMode === "exact") {
        return { kind: "line" };
      }
      try {
        new RegExp(cfg.includeQuery);
      } catch {
        return void 0;
      }
      return { kind: "regex", source: cfg.includeQuery, flags: "g" };
    }
    function isPinned(container) {
      if (!container) {
        return true;
      }
      return Math.abs(container.scrollHeight - container.scrollTop - container.clientHeight) < 12;
    }
    function setPendingLabel(stream) {
      const state = logState.streams[stream];
      const container = state.container;
      if (!container) {
        return;
      }
      container.classList.toggle("is-paused", logState.paused && state.pendingCount > 0);
      const label = state.pendingCount > 0 ? `${state.pendingCount} new line${state.pendingCount === 1 ? "" : "s"} buffered` : "";
      container.setAttribute("data-pending-label", label);
    }
    function formatMetadataTimestamp(record) {
      if (typeof record.ts !== "number" || !Number.isFinite(record.ts)) {
        return "--:--:--.---";
      }
      const date = new Date(record.ts * 1e3);
      const pad = (value, width = 2) => String(value).padStart(width, "0");
      return `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}.${pad(date.getMilliseconds(), 3)}`;
    }
    function metadataPreview(record) {
      if (record.kind === "stdin_eof") {
        return "<EOF>";
      }
      const text = record.text ?? record.preview ?? "";
      if (!text) {
        return "<stdin hidden>";
      }
      return record.preview_truncated ? `${text} \u2026` : text;
    }
    function buildMetadataNode(record) {
      const node = document.createElement("div");
      node.className = "log-line io-metadata-line";
      const label = document.createElement("span");
      label.className = "io-metadata-label";
      const source = record.source || "unknown";
      const bytes = typeof record.byte_count === "number" ? `${record.byte_count}B` : "?B";
      label.textContent = `stdin | ${formatMetadataTimestamp(record)} | ${source} | ${bytes}`;
      const body = document.createElement("span");
      body.className = "io-metadata-body";
      const rendered = renderLogLine(metadataPreview(record), createDefaultAnsiStyle(), {
        prettyJson: logState.streams.stdout.prettyJson,
        highlight: void 0
      });
      body.appendChild(rendered.fragment);
      node.append(label, body);
      return node;
    }
    function buildEntryNode(stream, entry, renderStyle) {
      if (entry.kind === "io") {
        return { node: buildMetadataNode(entry.record), finalStyle: renderStyle };
      }
      const node = document.createElement("div");
      node.className = "log-line";
      const rendered = renderLogLine(entry.text, renderStyle, {
        prettyJson: logState.streams[stream].prettyJson,
        highlight: getFilterHighlight(stream)
      });
      node.appendChild(rendered.fragment);
      return { node, finalStyle: rendered.finalStyle };
    }
    function buildLineNodes(stream, entries) {
      const fragment = document.createDocumentFragment();
      const wrapper = document.createElement("div");
      wrapper.className = "log-lines";
      let renderStyle = createDefaultAnsiStyle();
      for (const entry of entries) {
        const rendered = buildEntryNode(stream, entry, renderStyle);
        renderStyle = rendered.finalStyle;
        const node = rendered.node;
        wrapper.appendChild(node);
      }
      fragment.appendChild(wrapper);
      return fragment;
    }
    function renderStream(stream) {
      const state = logState.streams[stream];
      const container = state.container;
      if (!container) {
        return;
      }
      const pinned = isPinned(container);
      const entries = getFilteredEntries(stream);
      container.innerHTML = "";
      if (entries.length === 0) {
        const empty = document.createElement("div");
        empty.className = "loading";
        empty.textContent = logState.shellId ? "No lines matched." : "Select a shell log.";
        container.appendChild(empty);
      } else {
        container.appendChild(buildLineNodes(stream, entries));
      }
      if (pinned) {
        container.scrollTop = container.scrollHeight;
      }
      setPendingLabel(stream);
    }
    function appendLines(stream, newLines, partialLine, initialAnsiStyle) {
      const state = logState.streams[stream];
      const container = state.container;
      if (!container) {
        return;
      }
      const pinned = isPinned(container);
      let wrapper = container.querySelector(".log-lines");
      if (!wrapper) {
        container.innerHTML = "";
        wrapper = document.createElement("div");
        wrapper.className = "log-lines";
        container.appendChild(wrapper);
      }
      const previousPartialNode = wrapper.querySelector(".log-line.is-partial");
      previousPartialNode?.remove();
      let renderStyle = cloneAnsiStyle(initialAnsiStyle);
      const renderOptions = {
        prettyJson: state.prettyJson,
        highlight: getFilterHighlight(stream)
      };
      for (const line of newLines) {
        const node = document.createElement("div");
        node.className = "log-line";
        const rendered = renderLogLine(line, renderStyle, renderOptions);
        node.appendChild(rendered.fragment);
        renderStyle = rendered.finalStyle;
        wrapper.appendChild(node);
      }
      if (partialLine) {
        const partialNode = document.createElement("div");
        partialNode.className = "log-line is-partial";
        wrapper.appendChild(partialNode);
        partialNode.replaceChildren(renderLogLine(partialLine, renderStyle, renderOptions).fragment);
      }
      if (pinned) {
        container.scrollTop = container.scrollHeight;
      }
    }
    function parseTextIntoState(stream, text) {
      const state = logState.streams[stream];
      const normalized = String(text || "");
      const parts = normalized.split("\n");
      state.partial = normalized.endsWith("\n") ? "" : parts.pop() || "";
      state.entries = parts.map((line) => ({ kind: "text", text: line }));
      state.ansiStyle = createDefaultAnsiStyle();
      for (const entry of state.entries) {
        if (entry.kind === "text") {
          state.ansiStyle = advanceAnsiStyle(entry.text, state.ansiStyle);
        }
      }
      state.pendingCount = 0;
      setPendingLabel(stream);
    }
    function appendChunkToState(stream, chunk) {
      const state = logState.streams[stream];
      const initialAnsiStyle = cloneAnsiStyle(state.ansiStyle);
      const text = `${state.partial}${String(chunk || "")}`;
      const parts = text.split("\n");
      state.partial = text.endsWith("\n") ? "" : parts.pop() || "";
      const newLines = parts;
      if (newLines.length > 0) {
        for (const line of newLines) {
          state.entries.push({ kind: "text", text: line });
          state.ansiStyle = advanceAnsiStyle(line, state.ansiStyle);
        }
      }
      return { newLines, partialLine: state.partial, initialAnsiStyle };
    }
    function resetStream(stream) {
      const state = logState.streams[stream];
      state.entries = [];
      state.partial = "";
      state.pendingCount = 0;
      state.ansiStyle = createDefaultAnsiStyle();
      renderStream(stream);
    }
    function appendIoMetadataRecord(record, options) {
      if (record.kind !== "stdin_write" && record.kind !== "stdin_eof") {
        return;
      }
      const state = logState.streams.stdout;
      state.entries.push({ kind: "io", record });
      if (!options.render) {
        return;
      }
      if (logState.paused) {
        state.pendingCount += 1;
        setPendingLabel("stdout");
        return;
      }
      if (!logState.ioOverlayEnabled) {
        return;
      }
      renderStream("stdout");
    }
    function appendInitialIoMetadata(records) {
      for (const record of records) {
        appendIoMetadataRecord(record, { render: false });
      }
    }
    function saveLogRenderOptions() {
      try {
        window.localStorage.setItem(LOG_RENDER_OPTIONS_KEY, JSON.stringify(logRenderOptions));
      } catch {
      }
    }
    function setStoredPrettyJson(shellId, stream, enabled) {
      if (!shellId) {
        return;
      }
      const shellOptions = logRenderOptions[shellId] ?? {};
      shellOptions[stream] = { prettyJson: enabled };
      logRenderOptions[shellId] = shellOptions;
      saveLogRenderOptions();
    }
    function setStoredIoOverlay(shellId, enabled) {
      if (!shellId) {
        return;
      }
      const shellOptions = logRenderOptions[shellId] ?? {};
      shellOptions.ioOverlay = enabled;
      logRenderOptions[shellId] = shellOptions;
      saveLogRenderOptions();
    }
    function syncPrettyJsonToggle(stream) {
      const input = getElementById(`${stream}-pretty-json`);
      if (input) {
        input.checked = logState.streams[stream].prettyJson;
      }
    }
    function applyStoredLogRenderOptions(shellId) {
      for (const stream of LOG_STREAMS) {
        const options = getStoredStreamRenderOptions(logRenderOptions, shellId, stream);
        logState.streams[stream].prettyJson = options.prettyJson;
        syncPrettyJsonToggle(stream);
      }
      logState.ioOverlayEnabled = Boolean(logRenderOptions[shellId]?.ioOverlay) && shellHasIoMetadata(shellId);
      syncIoOverlayToggle();
    }
    function renderLogError(message) {
      for (const stream of LOG_STREAMS) {
        const state = logState.streams[stream];
        if (state.container) {
          state.container.innerHTML = `<div class="loading">${escapeHtml(message)}</div>`;
        }
      }
    }
    function routeLogNotification(message) {
      const currentShellId = logState.shellId;
      if (!currentShellId) {
        return;
      }
      switch (message.method) {
        case "fws.logs.initial":
          if (message.params.shell_id !== currentShellId) {
            return;
          }
          parseTextIntoState("stdout", message.params.stdout);
          parseTextIntoState("stderr", message.params.stderr);
          appendInitialIoMetadata(message.params.io_metadata);
          renderStream("stdout");
          renderStream("stderr");
          return;
        case "fws.logs.io_metadata":
          if (message.params.shell_id !== currentShellId) {
            return;
          }
          appendIoMetadataRecord(message.params.record, { render: true });
          return;
        case "fws.logs.reset":
          if (message.params.shell_id !== currentShellId) {
            return;
          }
          resetStream(message.params.stream);
          return;
        case "fws.logs.chunk": {
          if (message.params.shell_id !== currentShellId) {
            return;
          }
          const stream = message.params.stream;
          const appended = appendChunkToState(stream, message.params.chunk);
          if (logState.paused) {
            logState.streams[stream].pendingCount += appended.newLines.length;
            setPendingLabel(stream);
            return;
          }
          if (hasActiveFilters(stream)) {
            renderStream(stream);
          } else {
            appendLines(stream, appended.newLines, appended.partialLine, appended.initialAnsiStyle);
          }
          return;
        }
        case "fws.error":
          if (message.params.shell_id !== void 0 && message.params.shell_id !== currentShellId) {
            return;
          }
          renderLogError(message.params.message);
          setLogStatus("Error", false);
          return;
        default:
          return;
      }
    }
    async function openLogSubscription(shellId) {
      try {
        await sendDashboardRequest("fws.logs.open", { shell_id: shellId });
        if (logState.shellId === shellId) {
          setLogStatus("Connected", true);
        }
      } catch (error) {
        if (logState.shellId !== shellId) {
          return;
        }
        renderLogError(error instanceof Error ? error.message : String(error));
        setLogStatus("Error", false);
      }
    }
    async function closeLogSubscription(shellId) {
      try {
        await sendDashboardRequest("fws.logs.close", { shell_id: shellId });
      } catch {
      }
    }
    function syncLogUrl(shellId, replace) {
      const url = new URL(window.location.href);
      if (shellId) {
        url.searchParams.set("log", shellId);
      } else {
        url.searchParams.delete("log");
      }
      if (replace) {
        window.history.replaceState({ log: shellId || null }, "", url);
      } else {
        window.history.pushState({ log: shellId || null }, "", url);
      }
    }
    function openLogDrawer(shellId, shellLabel, options = {}) {
      const nextShellId = String(shellId || "").trim();
      if (!nextShellId || !logDrawer) {
        return;
      }
      if (!options.fromPopState) {
        const sameShell = logState.shellId === nextShellId;
        syncLogUrl(nextShellId, sameShell);
      }
      document.body.classList.add("has-log-drawer");
      logDrawer.classList.add("is-open");
      logDrawer.setAttribute("aria-hidden", "false");
      logState.shellId = nextShellId;
      logState.shellLabel = shellLabel || findShellLabel(nextShellId);
      applyStoredLogRenderOptions(nextShellId);
      updateStdinInjectorState();
      if (logTitleEl) {
        logTitleEl.textContent = logState.shellLabel || "Shell Logs";
      }
      if (logSubtitleEl) {
        logSubtitleEl.textContent = nextShellId;
      }
      setLogStatus("Connecting...", false);
      for (const stream of LOG_STREAMS) {
        parseTextIntoState(stream, "");
        const state = logState.streams[stream];
        if (state.container) {
          state.container.innerHTML = '<div class="loading">Connecting...</div>';
        }
      }
      void getFwsSocket().then((socket) => {
        if (logState.shellId !== nextShellId || !socket.connected) {
          return;
        }
        void openLogSubscription(nextShellId);
      });
    }
    function closeLogDrawer(options = {}) {
      if (!logDrawer) {
        return;
      }
      const previousShellId = logState.shellId;
      logState.shellId = "";
      logState.shellLabel = "";
      logState.ioOverlayEnabled = false;
      updateStdinInjectorState();
      syncIoOverlayToggle();
      logDrawer.classList.remove("is-open");
      logDrawer.setAttribute("aria-hidden", "true");
      document.body.classList.remove("has-log-drawer");
      setLogStatus("Disconnected", false);
      if (previousShellId) {
        void closeLogSubscription(previousShellId);
      }
      if (!options.fromPopState) {
        syncLogUrl("", true);
      }
    }
    async function submitStdinInjection() {
      const shellId = logState.shellId;
      if (!shellId) {
        updateStdinInjectorState("Select a shell first.");
        return;
      }
      const data = stdinInput?.value ?? "";
      const appendNewline = stdinNewlineInput?.checked === true;
      if (!data && !appendNewline) {
        updateStdinInjectorState("Nothing to send.");
        return;
      }
      setStdinInjectorDisabled(true);
      updateStdinInjectorState("Sending stdin...");
      try {
        await sendDashboardRequest("fws.shell.input", {
          shell_id: shellId,
          data,
          append_newline: appendNewline
        });
        const bytesHint = new TextEncoder().encode(appendNewline ? `${data}
` : data).length;
        updateStdinInjectorState(`Sent ${bytesHint} byte${bytesHint === 1 ? "" : "s"} to stdin.`);
      } catch (error) {
        updateStdinInjectorState(error instanceof Error ? error.message : String(error));
      } finally {
        updateStdinInjectorState(stdinStatusEl?.textContent || void 0);
      }
    }
    function compactStdinJson() {
      const input = stdinInput;
      if (!input) {
        return;
      }
      const raw = input.value;
      if (!raw.trim()) {
        updateStdinInjectorState("Nothing to minify.");
        return;
      }
      try {
        const parsed = JSON.parse(raw);
        const compact = JSON.stringify(parsed);
        input.value = compact;
        if (stdinNewlineInput) {
          stdinNewlineInput.checked = true;
        }
        const bytesHint = new TextEncoder().encode(`${compact}
`).length;
        updateStdinInjectorState(`Minified JSON to one line (${bytesHint} byte${bytesHint === 1 ? "" : "s"} with newline).`);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        updateStdinInjectorState(`Invalid JSON: ${message}`);
      }
    }
    function wireFilters(stream) {
      const includeInput = getElementById(`${stream}-include-input`);
      const excludeInput = getElementById(`${stream}-exclude-input`);
      const radios = document.querySelectorAll(`input[name="${stream}-include-mode"], input[name="${stream}-exclude-mode"]`);
      let timer = 0;
      const apply = () => renderStream(stream);
      const applyDebounced = () => {
        if (timer) {
          window.clearTimeout(timer);
        }
        timer = window.setTimeout(apply, 200);
      };
      includeInput?.addEventListener("input", applyDebounced);
      excludeInput?.addEventListener("input", applyDebounced);
      radios.forEach((radio) => radio.addEventListener("change", apply));
    }
    function wirePrettyJsonToggle(stream) {
      const input = getElementById(`${stream}-pretty-json`);
      input?.addEventListener("change", () => {
        const enabled = input.checked;
        logState.streams[stream].prettyJson = enabled;
        setStoredPrettyJson(logState.shellId, stream, enabled);
        renderStream(stream);
      });
    }
    wireFilters("stdout");
    wireFilters("stderr");
    wirePrettyJsonToggle("stdout");
    wirePrettyJsonToggle("stderr");
    ioOverlayInput?.addEventListener("change", () => {
      const enabled = ioOverlayInput.checked && shellHasIoMetadata(logState.shellId);
      logState.ioOverlayEnabled = enabled;
      setStoredIoOverlay(logState.shellId, enabled);
      syncIoOverlayToggle();
      renderStream("stdout");
    });
    stdinForm?.addEventListener("submit", (event) => {
      event.preventDefault();
      void submitStdinInjection();
    });
    stdinJsonCompactButton?.addEventListener("click", () => {
      compactStdinJson();
    });
    if (logPauseInput) {
      logPauseInput.addEventListener("change", () => {
        logState.paused = logPauseInput.checked;
        if (!logState.paused) {
          for (const stream of LOG_STREAMS) {
            logState.streams[stream].pendingCount = 0;
            renderStream(stream);
          }
        } else {
          for (const stream of LOG_STREAMS) {
            setPendingLabel(stream);
          }
        }
      });
    }
    logBackBtn?.addEventListener("click", () => closeLogDrawer());
    window.addEventListener("popstate", () => {
      const url = new URL(window.location.href);
      const shellId = url.searchParams.get("log");
      if (shellId) {
        openLogDrawer(shellId, findShellLabel(shellId), { fromPopState: true });
        return;
      }
      closeLogDrawer({ fromPopState: true });
    });
    document.addEventListener("submit", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLFormElement) || !target.matches('form[data-fws-ajax="1"]')) {
        return;
      }
      event.preventDefault();
      const confirmText = target.getAttribute("data-confirm");
      if (confirmText && !window.confirm(confirmText)) {
        return;
      }
      void submitActionForm(target).catch(() => {
        setStatus("Error", false);
      });
    });
    document.addEventListener("click", (event) => {
      const target = event.target;
      if (!isElement(target)) {
        return;
      }
      const toggle = target.closest("[data-collapse-toggle]");
      if (toggle) {
        event.preventDefault();
        const card = toggle.closest("[data-shell-id]");
        if (!card) {
          return;
        }
        const id = card.getAttribute("data-shell-id") || "";
        const nextCollapsed = !card.classList.contains("is-collapsed");
        if (id) {
          collapseState.set(id, nextCollapsed);
        }
        setCardCollapsed(card, nextCollapsed);
        updateToggleAllLabel();
        return;
      }
      const groupToggle = target.closest("[data-group-toggle]");
      if (groupToggle) {
        event.preventDefault();
        const card = groupToggle.closest("[data-group-id]");
        if (!card) {
          return;
        }
        const id = card.getAttribute("data-group-id") || "";
        if (!id) {
          return;
        }
        const currentlyCollapsed = card.classList.contains("is-collapsed");
        const expanded = currentlyCollapsed;
        groupExpanded[id] = expanded;
        persistGroupExpanded();
        setGroupCollapsed(card, !expanded);
        return;
      }
      if (toggleAllBtn && target.closest("#fws-toggle-all")) {
        event.preventDefault();
        const shouldCollapse = toggleAllBtn.textContent === "Collapse All";
        setAllCollapsed(shouldCollapse);
        return;
      }
      if (target.closest("#fws-exited-toggle")) {
        event.preventDefault();
        const toggleBtn = target.closest("#fws-exited-toggle");
        if (!toggleBtn) {
          return;
        }
        const expanded = toggleBtn.getAttribute("aria-expanded") === "true";
        setExitedExpanded(!expanded);
        if (!expanded) {
          applyExitedPagination();
        }
        return;
      }
      if (target.closest("#fws-exited-more")) {
        event.preventDefault();
        exitedVisibleCount += EXITED_PAGE_SIZE;
        applyExitedPagination();
        return;
      }
      const logButton = target.closest("[data-log-open]");
      if (logButton) {
        event.preventDefault();
        openLogDrawer(logButton.getAttribute("data-log-open") || "", logButton.getAttribute("data-log-label") || "");
        return;
      }
      const copyButton = target.closest(".copy-overlay");
      const copyField = target.closest(".copy-field");
      if (!copyField) {
        return;
      }
      if (!copyButton && target.closest("a,button,form,input,textarea,select,label")) {
        return;
      }
      const value = copyField.getAttribute("data-copy") || "";
      void copyText(value);
      flashCopied(copyField);
    });
    document.addEventListener("keydown", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement) || !target.classList.contains("copy-field")) {
        return;
      }
      if (event.key !== "Enter" && event.key !== " ") {
        return;
      }
      event.preventDefault();
      const value = target.getAttribute("data-copy") || "";
      void copyText(value);
      flashCopied(target);
    });
    void fwsSocketReady.then((socket) => {
      fwsSocket = socket;
      const handleConnect = () => {
        setStatus("Connecting...", false);
        if (logState.shellId) {
          setLogStatus("Connecting...", false);
        }
        void sendDashboardRequest("fws.dashboard.open", { view: "html" }).then((result) => {
          if (hasDashboardStateResult(result)) {
            applyDashboardState(result.state);
            setStatus("Live", true);
          } else {
            setStatus("Error", false);
          }
        }).catch(() => setStatus("Error", false));
        if (logState.shellId) {
          void openLogSubscription(logState.shellId);
        }
      };
      socket.on("connect", handleConnect);
      socket.on("fws_notification", (payload) => {
        const message = coerceIncomingJsonRpcMessage(payload);
        if (!message || !isServerNotificationMessage(message)) {
          return;
        }
        routeDashboardNotification(message);
        routeLogNotification(message);
      });
      socket.on("connect_error", () => {
        setStatus("Error", false);
        if (logState.shellId) {
          setLogStatus("Error", false);
        }
      });
      socket.on("disconnect", () => {
        setStatus("Disconnected", false);
        if (logState.shellId) {
          setLogStatus("Disconnected", false);
        }
      });
      if (socket.connected) {
        handleConnect();
      }
    }).catch(() => {
      setStatus("Error", false);
      if (logState.shellId) {
        setLogStatus("Error", false);
      }
    });
    updateToggleAllLabel();
    const initialLog = new URL(window.location.href).searchParams.get("log");
    if (initialLog) {
      openLogDrawer(initialLog, initialLog, { fromPopState: true });
    }
  })();
})();
