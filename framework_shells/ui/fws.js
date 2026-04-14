"use strict";
(() => {
  // framework_shells/ui/src/reconnecting_websocket.ts
  var DEFAULT_OPTIONS = {
    maxRetries: Number.POSITIVE_INFINITY,
    reconnectInterval: 1e3,
    maxReconnectInterval: 3e4,
    reconnectDecay: 1.5,
    debug: false,
    protocols: []
  };
  var ReconnectingWebSocket = class {
    constructor(url, options = {}) {
      this.ws = null;
      this.reconnectAttempts = 0;
      this.reconnectTimeout = null;
      this.messageQueue = [];
      this.forcedClose = false;
      this.readyState = WebSocket.CONNECTING;
      this.onopen = null;
      this.onmessage = null;
      this.onerror = null;
      this.onclose = null;
      this.onreconnect = null;
      this.url = url;
      this.options = {
        maxRetries: options.maxRetries ?? DEFAULT_OPTIONS.maxRetries,
        reconnectInterval: options.reconnectInterval ?? DEFAULT_OPTIONS.reconnectInterval,
        maxReconnectInterval: options.maxReconnectInterval ?? DEFAULT_OPTIONS.maxReconnectInterval,
        reconnectDecay: options.reconnectDecay ?? DEFAULT_OPTIONS.reconnectDecay,
        debug: options.debug ?? DEFAULT_OPTIONS.debug,
        protocols: options.protocols ?? DEFAULT_OPTIONS.protocols
      };
      this.connect();
    }
    log(...args) {
      if (this.options.debug) {
        console.log("[ReconnectingWebSocket]", ...args);
      }
    }
    connect() {
      if (this.forcedClose) {
        this.log("Connection blocked: forcedClose = true");
        return;
      }
      this.log(`Connecting to ${this.url}...`);
      try {
        this.ws = new WebSocket(this.url, this.options.protocols);
        this.readyState = WebSocket.CONNECTING;
        this.ws.onopen = (event) => {
          this.log("Connected successfully");
          this.readyState = WebSocket.OPEN;
          this.reconnectAttempts = 0;
          while (this.messageQueue.length > 0) {
            const message = this.messageQueue.shift();
            if (message === void 0 || this.ws === null) {
              continue;
            }
            this.log("Sending queued message:", message);
            this.ws.send(message);
          }
          this.onopen?.(event);
        };
        this.ws.onmessage = (event) => {
          this.onmessage?.(event);
        };
        this.ws.onerror = (event) => {
          this.log("WebSocket error:", event);
          this.onerror?.(event);
        };
        this.ws.onclose = (event) => {
          this.log("Connection closed:", event.code, event.reason);
          this.readyState = WebSocket.CLOSED;
          this.onclose?.(event);
          if (!this.forcedClose) {
            this.scheduleReconnect();
          }
        };
      } catch (error) {
        this.log("Connection error:", error);
        this.readyState = WebSocket.CLOSED;
        this.onerror?.(error instanceof Error ? error : new Error(String(error)));
        if (!this.forcedClose) {
          this.scheduleReconnect();
        }
      }
    }
    scheduleReconnect() {
      if (this.reconnectAttempts >= this.options.maxRetries) {
        this.log(`Max reconnection attempts (${this.options.maxRetries}) reached`);
        return;
      }
      this.reconnectAttempts += 1;
      const delay = Math.min(
        this.options.reconnectInterval * Math.pow(this.options.reconnectDecay, this.reconnectAttempts - 1),
        this.options.maxReconnectInterval
      );
      this.log(`Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts}/${this.options.maxRetries})...`);
      this.onreconnect?.(this.reconnectAttempts, delay);
      this.reconnectTimeout = window.setTimeout(() => {
        this.connect();
      }, delay);
    }
    send(data) {
      if (this.ws !== null && this.ws.readyState === WebSocket.OPEN) {
        this.log("Sending message:", data);
        this.ws.send(data);
        return;
      }
      this.log("Queueing message (not connected):", data);
      this.messageQueue.push(data);
    }
    close(code = 1e3, reason = "Normal closure") {
      this.log("Manually closing connection");
      this.forcedClose = true;
      if (this.reconnectTimeout !== null) {
        window.clearTimeout(this.reconnectTimeout);
        this.reconnectTimeout = null;
      }
      this.ws?.close(code, reason);
      this.readyState = WebSocket.CLOSED;
    }
    reconnect() {
      this.log("Manual reconnect requested");
      this.forcedClose = false;
      this.reconnectAttempts = 0;
      if (this.reconnectTimeout !== null) {
        window.clearTimeout(this.reconnectTimeout);
        this.reconnectTimeout = null;
      }
      this.ws?.close();
      this.connect();
    }
    get bufferedAmount() {
      return this.ws?.bufferedAmount ?? 0;
    }
    get extensions() {
      return this.ws?.extensions ?? "";
    }
    get protocol() {
      return this.ws?.protocol ?? "";
    }
    get binaryType() {
      return this.ws?.binaryType ?? "blob";
    }
    set binaryType(type) {
      if (this.ws !== null) {
        this.ws.binaryType = type;
      }
    }
  };
  var reconnecting_websocket_default = ReconnectingWebSocket;

  // framework_shells/ui/src/protocol.ts
  function isRecord(value) {
    return typeof value === "object" && value !== null;
  }
  function isLogStreamName(value) {
    return value === "stdout" || value === "stderr";
  }
  function isJsonRpcVersion(value) {
    return value === "2.0";
  }
  function parseJsonRpcObject(raw) {
    let parsed;
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
  function stringifyClientRequest(method, id, params) {
    const payload = {
      jsonrpc: "2.0",
      id,
      method,
      params
    };
    return JSON.stringify(payload);
  }
  function frameJsonRpcLine(payload) {
    return payload.endsWith("\n") ? payload : `${payload}
`;
  }
  function consumeJsonlChunk(buffer, chunk) {
    if (typeof chunk !== "string" || chunk.length === 0) {
      return { lines: [], buffer };
    }
    const combined = buffer + chunk;
    const parts = combined.split("\n");
    const nextBuffer = parts.pop() ?? "";
    const lines = parts.map((line) => line.trim()).filter((line) => line.length > 0);
    return { lines, buffer: nextBuffer };
  }
  function parseIncomingJsonRpcMessage(raw) {
    const parsed = parseJsonRpcObject(raw);
    if (!parsed) {
      return null;
    }
    if (typeof parsed.id === "string" && isRecord(parsed.result)) {
      const result = parsed.result;
      if (result.accepted === true) {
        if (typeof result.shell_id === "string") {
          return {
            jsonrpc: "2.0",
            id: parsed.id,
            result: { accepted: true, shell_id: result.shell_id }
          };
        }
        return {
          jsonrpc: "2.0",
          id: parsed.id,
          result: { accepted: true }
        };
      }
      if (result.ok === true) {
        return {
          jsonrpc: "2.0",
          id: parsed.id,
          result: { ok: true }
        };
      }
      return null;
    }
    if ((typeof parsed.id === "string" || parsed.id === null) && isRecord(parsed.error)) {
      const error = parsed.error;
      if (typeof error.code !== "number" || typeof error.message !== "string") {
        return null;
      }
      const response = {
        jsonrpc: "2.0",
        id: typeof parsed.id === "string" ? parsed.id : null,
        error: {
          code: error.code,
          message: error.message
        }
      };
      if (isRecord(error.data)) {
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
    if (typeof parsed.method !== "string" || !isRecord(parsed.params)) {
      return null;
    }
    switch (parsed.method) {
      case "fws.dashboard.snapshot":
        if (typeof parsed.params.html === "string") {
          return {
            jsonrpc: "2.0",
            method: parsed.method,
            params: { html: parsed.params.html }
          };
        }
        return null;
      case "fws.logs.initial":
        if (typeof parsed.params.shell_id === "string" && typeof parsed.params.stdout === "string" && typeof parsed.params.stderr === "string") {
          return {
            jsonrpc: "2.0",
            method: parsed.method,
            params: {
              shell_id: parsed.params.shell_id,
              stdout: parsed.params.stdout,
              stderr: parsed.params.stderr
            }
          };
        }
        return null;
      case "fws.logs.chunk":
        if (typeof parsed.params.shell_id === "string" && isLogStreamName(parsed.params.stream) && typeof parsed.params.chunk === "string") {
          return {
            jsonrpc: "2.0",
            method: parsed.method,
            params: {
              shell_id: parsed.params.shell_id,
              stream: parsed.params.stream,
              chunk: parsed.params.chunk
            }
          };
        }
        return null;
      case "fws.logs.reset":
        if (typeof parsed.params.shell_id === "string" && isLogStreamName(parsed.params.stream)) {
          return {
            jsonrpc: "2.0",
            method: parsed.method,
            params: {
              shell_id: parsed.params.shell_id,
              stream: parsed.params.stream
            }
          };
        }
        return null;
      case "fws.error":
        if (typeof parsed.params.message === "string") {
          const result = { message: parsed.params.message };
          if (typeof parsed.params.code === "string") {
            result.code = parsed.params.code;
          }
          if (typeof parsed.params.shell_id === "string") {
            result.shell_id = parsed.params.shell_id;
          }
          return {
            jsonrpc: "2.0",
            method: parsed.method,
            params: result
          };
        }
        return null;
      default:
        return null;
    }
  }

  // framework_shells/ui/src/fws.ts
  var LOG_STREAMS = ["stdout", "stderr"];
  var EXITED_EXPANDED_KEY = "fws.exited.expanded";
  var GROUP_EXPANDED_KEY = "fws.group.expanded";
  var EXITED_PAGE_SIZE = 50;
  function isRecord2(value) {
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
      if (!isRecord2(parsed)) {
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
  function makeStreamState(containerId) {
    return {
      container: getElementById(containerId),
      lines: [],
      partial: "",
      pendingCount: 0
    };
  }
  function getWebSocketUrl(path) {
    const scheme = window.location.protocol === "https:" ? "wss" : "ws";
    return `${scheme}://${window.location.host}${path}`;
  }
  (() => {
    const content = getElementById("fws-content");
    const statusEl = getElementById("fws-status");
    const toggleAllBtn = getElementById("fws-toggle-all");
    const logDrawer = getElementById("fws-log-drawer");
    const logBackBtn = getElementById("fws-log-back");
    const logTitleEl = getElementById("fws-log-title");
    const logSubtitleEl = getElementById("fws-log-subtitle");
    const logStatusEl = getElementById("fws-log-status");
    const logPauseInput = getElementById("fws-log-pause");
    const collapseState = /* @__PURE__ */ new Map();
    const exitedCache = { html: "", token: "", loading: null };
    let defaultCollapsed = true;
    let groupExpanded = parseStoredGroupExpanded(window.localStorage.getItem(GROUP_EXPANDED_KEY));
    let exitedVisibleCount = EXITED_PAGE_SIZE;
    let dashboardMessageBuffer = "";
    let dashboardRequestCounter = 0;
    const dashboardPendingRequests = /* @__PURE__ */ new Map();
    const logState = {
      shellId: "",
      shellLabel: "",
      socket: null,
      paused: false,
      streams: {
        stdout: makeStreamState("stdout-container"),
        stderr: makeStreamState("stderr-container")
      }
    };
    function nextDashboardRequestId() {
      dashboardRequestCounter += 1;
      return `fws_req_${dashboardRequestCounter}`;
    }
    function rejectPendingRequests(pending, message) {
      const error = new Error(message);
      for (const [, request] of pending) {
        request.reject(error);
      }
      pending.clear();
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
    function routeDashboardRpcMessage(message) {
      if (isJsonRpcResponseMessage(message)) {
        const pending = dashboardPendingRequests.get(message.id);
        if (!pending) {
          return;
        }
        dashboardPendingRequests.delete(message.id);
        if (isJsonRpcErrorMessage(message)) {
          pending.reject(new Error(message.error.message));
          return;
        }
        pending.resolve(message.result);
        return;
      }
      if (!isServerNotificationMessage(message)) {
        return;
      }
      if (message.method !== "fws.dashboard.snapshot") {
        return;
      }
      if (content) {
        content.innerHTML = message.params.html;
        applyCollapseState(content);
        applyGroupState(content);
        applyExitedSectionState();
      }
    }
    function processDashboardChunk(raw) {
      const consumed = consumeJsonlChunk(dashboardMessageBuffer, raw);
      dashboardMessageBuffer = consumed.buffer;
      for (const line of consumed.lines) {
        const message = parseIncomingJsonRpcMessage(line);
        if (message) {
          routeDashboardRpcMessage(message);
        }
      }
    }
    async function sendDashboardRequest(method, params) {
      const requestId = nextDashboardRequestId();
      return await new Promise((resolve, reject) => {
        dashboardPendingRequests.set(requestId, { resolve, reject });
        ws.send(frameJsonRpcLine(stringifyClientRequest(method, requestId, params)));
      });
    }
    async function submitActionForm(form) {
      const action = form.getAttribute("action") || window.location.href;
      const url = new URL(action, window.location.href);
      const path = url.pathname;
      const formData = new FormData(form);
      if (path === "/fws/action/refresh") {
        await sendDashboardRequest("fws.dashboard.refresh", {});
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
        const scope = scopeValue === "shells" ? "shells" : "tree";
        await sendDashboardRequest("fws.shutdown", { scope });
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
    async function ensureExitedLoaded(forceReload) {
      if (!content) {
        return;
      }
      const exitedContent = content.querySelector("#fws-exited-content");
      if (!exitedContent) {
        return;
      }
      const token = exitedContent.getAttribute("data-token") || "";
      if (!forceReload && exitedContent.getAttribute("data-loaded") === "1") {
        return;
      }
      if (!forceReload && exitedCache.html && exitedCache.token === token) {
        exitedContent.innerHTML = exitedCache.html;
        exitedContent.setAttribute("data-loaded", "1");
        applyCollapseState(exitedContent);
        applyExitedPagination();
        return;
      }
      if (exitedCache.loading && !forceReload) {
        return exitedCache.loading;
      }
      exitedContent.innerHTML = '<div class="loading">Loading exited shells...</div>';
      exitedCache.loading = (async () => {
        try {
          const res = await fetch("/fws/exited", { credentials: "same-origin", cache: "no-store" });
          const html = await res.text();
          exitedContent.innerHTML = html;
          exitedContent.setAttribute("data-loaded", "1");
          exitedCache.html = html;
          exitedCache.token = token;
          applyCollapseState(exitedContent);
          applyExitedPagination();
        } catch {
          exitedContent.innerHTML = '<div class="shell-card"><div class="shell-meta">Failed to load exited shells.</div></div>';
          exitedContent.setAttribute("data-loaded", "0");
        } finally {
          exitedCache.loading = null;
        }
      })();
      return exitedCache.loading;
    }
    function applyExitedSectionState() {
      const expanded = getExitedExpandedDefault();
      setExitedExpanded(expanded);
      if (expanded) {
        void ensureExitedLoaded(false);
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
    function getFilteredLines(stream) {
      const state = logState.streams[stream];
      const cfg = getFilterConfig(stream);
      const includeMatch = compileMatcher(stream, "include", cfg.includeQuery, cfg.includeMode);
      const excludeMatch = compileMatcher(stream, "exclude", cfg.excludeQuery, cfg.excludeMode);
      const allLines = state.partial ? state.lines.concat([state.partial]) : state.lines.slice();
      return allLines.filter((line) => {
        const includeOk = cfg.includeQuery ? includeMatch(line) : true;
        const excludeHit = cfg.excludeQuery ? excludeMatch(line) : false;
        return includeOk && !excludeHit;
      });
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
    function buildLineNodes(lines) {
      const fragment = document.createDocumentFragment();
      const wrapper = document.createElement("div");
      wrapper.className = "log-lines";
      for (const line of lines) {
        const node = document.createElement("div");
        node.className = "log-line";
        node.textContent = line;
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
      const lines = getFilteredLines(stream);
      container.innerHTML = "";
      if (lines.length === 0) {
        const empty = document.createElement("div");
        empty.className = "loading";
        empty.textContent = logState.shellId ? "No lines matched." : "Select a shell log.";
        container.appendChild(empty);
      } else {
        container.appendChild(buildLineNodes(lines));
      }
      if (pinned) {
        container.scrollTop = container.scrollHeight;
      }
      setPendingLabel(stream);
    }
    function appendLines(stream, newLines, partialLine) {
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
      for (const line of newLines) {
        const node = document.createElement("div");
        node.className = "log-line";
        node.textContent = line;
        wrapper.appendChild(node);
      }
      let partialNode = wrapper.querySelector(".log-line.is-partial");
      if (partialLine) {
        if (!partialNode) {
          partialNode = document.createElement("div");
          partialNode.className = "log-line is-partial";
          wrapper.appendChild(partialNode);
        }
        partialNode.textContent = partialLine;
      } else if (partialNode) {
        partialNode.remove();
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
      state.lines = parts;
      state.pendingCount = 0;
      setPendingLabel(stream);
    }
    function appendChunkToState(stream, chunk) {
      const state = logState.streams[stream];
      const text = `${state.partial}${String(chunk || "")}`;
      const parts = text.split("\n");
      state.partial = text.endsWith("\n") ? "" : parts.pop() || "";
      const newLines = parts;
      if (newLines.length > 0) {
        state.lines.push(...newLines);
      }
      return { newLines, partialLine: state.partial };
    }
    function resetStream(stream) {
      const state = logState.streams[stream];
      state.lines = [];
      state.partial = "";
      state.pendingCount = 0;
      renderStream(stream);
    }
    function closeLogSocket() {
      if (!logState.socket) {
        return;
      }
      try {
        logState.socket.close();
      } catch {
      }
      logState.socket = null;
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
      logState.shellLabel = shellLabel || nextShellId;
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
      closeLogSocket();
      const socket = new reconnecting_websocket_default(getWebSocketUrl(`/ws/fws/logs/${encodeURIComponent(nextShellId)}`), {
        reconnectInterval: 1e3,
        maxReconnectInterval: 5e3,
        reconnectDecay: 1.5
      });
      logState.socket = socket;
      let logMessageBuffer = "";
      let logOpenRequestId = "";
      socket.onopen = () => {
        if (logState.socket !== socket) {
          return;
        }
        setLogStatus("Connecting...", false);
        logOpenRequestId = `fws_log_${Date.now()}`;
        socket.send(frameJsonRpcLine(stringifyClientRequest("fws.logs.open", logOpenRequestId, { shell_id: nextShellId })));
      };
      socket.onmessage = (event) => {
        if (logState.socket !== socket) {
          return;
        }
        const consumed = consumeJsonlChunk(logMessageBuffer, event.data);
        logMessageBuffer = consumed.buffer;
        for (const line of consumed.lines) {
          const message = parseIncomingJsonRpcMessage(line);
          if (!message) {
            continue;
          }
          if ("id" in message && typeof message.id === "string") {
            if (message.id === logOpenRequestId) {
              if ("error" in message) {
                for (const stream of LOG_STREAMS) {
                  const state = logState.streams[stream];
                  if (state.container) {
                    state.container.innerHTML = `<div class="loading">${message.error.message}</div>`;
                  }
                }
                setLogStatus("Error", false);
              } else {
                setLogStatus("Connected", true);
              }
            }
            continue;
          }
          if (!isServerNotificationMessage(message)) {
            continue;
          }
          switch (message.method) {
            case "fws.logs.initial":
              parseTextIntoState("stdout", message.params.stdout);
              parseTextIntoState("stderr", message.params.stderr);
              renderStream("stdout");
              renderStream("stderr");
              break;
            case "fws.logs.reset":
              resetStream(message.params.stream);
              break;
            case "fws.logs.chunk": {
              const stream = message.params.stream;
              const appended = appendChunkToState(stream, message.params.chunk);
              if (logState.paused) {
                logState.streams[stream].pendingCount += appended.newLines.length;
                setPendingLabel(stream);
                break;
              }
              if (hasActiveFilters(stream)) {
                renderStream(stream);
              } else {
                appendLines(stream, appended.newLines, appended.partialLine);
              }
              break;
            }
            case "fws.error":
              for (const stream of LOG_STREAMS) {
                const state = logState.streams[stream];
                if (state.container) {
                  state.container.innerHTML = `<div class="loading">${message.params.message}</div>`;
                }
              }
              if (message.params.shell_id === nextShellId || message.params.shell_id === void 0) {
                setLogStatus("Error", false);
              }
              break;
            default:
              break;
          }
        }
      };
      socket.onreconnect = (_attempt, delayMs) => {
        if (logState.socket !== socket) {
          return;
        }
        setLogStatus(`Reconnecting in ${Math.round(delayMs)}ms...`, false);
      };
      socket.onerror = () => {
        if (logState.socket !== socket) {
          return;
        }
        setLogStatus("Error", false);
      };
      socket.onclose = () => {
        if (logState.socket !== socket) {
          return;
        }
        setLogStatus("Disconnected", false);
      };
    }
    function closeLogDrawer(options = {}) {
      if (!logDrawer) {
        return;
      }
      closeLogSocket();
      logState.shellId = "";
      logState.shellLabel = "";
      logDrawer.classList.remove("is-open");
      logDrawer.setAttribute("aria-hidden", "true");
      document.body.classList.remove("has-log-drawer");
      setLogStatus("Disconnected", false);
      if (!options.fromPopState) {
        syncLogUrl("", true);
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
    wireFilters("stdout");
    wireFilters("stderr");
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
        openLogDrawer(shellId, shellId, { fromPopState: true });
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
          void ensureExitedLoaded(false);
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
    const ws = new reconnecting_websocket_default(getWebSocketUrl("/ws/fws"), {
      reconnectInterval: 1500,
      maxReconnectInterval: 5e3,
      reconnectDecay: 1.5
    });
    ws.onopen = () => {
      dashboardMessageBuffer = "";
      setStatus("Connecting...", false);
      void sendDashboardRequest("fws.dashboard.open", { view: "html" }).then(() => setStatus("Live", true)).catch(() => setStatus("Error", false));
    };
    ws.onmessage = (event) => {
      processDashboardChunk(event.data);
    };
    ws.onreconnect = (_attempt, delayMs) => {
      setStatus(`Reconnecting in ${Math.round(delayMs)}ms...`, false);
    };
    ws.onerror = () => {
      setStatus("Error", false);
    };
    ws.onclose = () => {
      rejectPendingRequests(dashboardPendingRequests, "FWS dashboard socket closed");
    };
    ws.onclose = () => {
      setStatus("Disconnected", false);
    };
    updateToggleAllLabel();
    const initialLog = new URL(window.location.href).searchParams.get("log");
    if (initialLog) {
      openLogDrawer(initialLog, initialLog, { fromPopState: true });
    }
  })();
})();
