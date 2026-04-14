import ReconnectingWebSocket from './reconnecting_websocket';
import {
  type LogStreamName,
  parseServerNotificationData,
  stringifyClientNotification,
} from './protocol';

const LOG_STREAMS: LogStreamName[] = ['stdout', 'stderr'];
const EXITED_EXPANDED_KEY = 'fws.exited.expanded';
const GROUP_EXPANDED_KEY = 'fws.group.expanded';
const EXITED_PAGE_SIZE = 50;

type FilterMode = 'regex' | 'exact';

interface StreamState {
  container: HTMLElement | null;
  lines: string[];
  partial: string;
  pendingCount: number;
}

interface LogState {
  shellId: string;
  shellLabel: string;
  socket: ReconnectingWebSocket | null;
  paused: boolean;
  streams: Record<LogStreamName, StreamState>;
}

interface ExitedCache {
  html: string;
  token: string;
  loading: Promise<void> | null;
}

interface DrawerOptions {
  fromPopState?: boolean;
}

interface FilterConfig {
  includeQuery: string;
  excludeQuery: string;
  includeMode: FilterMode;
  excludeMode: FilterMode;
}

type Matcher = (line: string) => boolean;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function getElementById<T extends HTMLElement>(id: string): T | null {
  const element = document.getElementById(id);
  if (element === null) {
    return null;
  }
  return element as T;
}

function isElement(target: EventTarget | null): target is Element {
  return target instanceof Element;
}

function normalizeStoredBoolean(value: unknown): boolean {
  return value === true || value === 1 || value === '1';
}

function parseStoredGroupExpanded(raw: string | null): Record<string, boolean> {
  if (!raw) {
    return {};
  }
  try {
    const parsed: unknown = JSON.parse(raw);
    if (!isRecord(parsed)) {
      return {};
    }
    const result: Record<string, boolean> = {};
    for (const [key, value] of Object.entries(parsed)) {
      result[key] = normalizeStoredBoolean(value);
    }
    return result;
  } catch {
    return {};
  }
}

function makeStreamState(containerId: string): StreamState {
  return {
    container: getElementById<HTMLElement>(containerId),
    lines: [],
    partial: '',
    pendingCount: 0,
  };
}

function getWebSocketUrl(path: string): string {
  const scheme = window.location.protocol === 'https:' ? 'wss' : 'ws';
  return `${scheme}://${window.location.host}${path}`;
}

(() => {
  const content = getElementById<HTMLElement>('fws-content');
  const statusEl = getElementById<HTMLElement>('fws-status');
  const toggleAllBtn = getElementById<HTMLButtonElement>('fws-toggle-all');
  const logDrawer = getElementById<HTMLElement>('fws-log-drawer');
  const logBackBtn = getElementById<HTMLButtonElement>('fws-log-back');
  const logTitleEl = getElementById<HTMLElement>('fws-log-title');
  const logSubtitleEl = getElementById<HTMLElement>('fws-log-subtitle');
  const logStatusEl = getElementById<HTMLElement>('fws-log-status');
  const logPauseInput = getElementById<HTMLInputElement>('fws-log-pause');

  const collapseState = new Map<string, boolean>();
  const exitedCache: ExitedCache = { html: '', token: '', loading: null };
  let defaultCollapsed = true;
  let groupExpanded = parseStoredGroupExpanded(window.localStorage.getItem(GROUP_EXPANDED_KEY));
  let exitedVisibleCount = EXITED_PAGE_SIZE;

  const logState: LogState = {
    shellId: '',
    shellLabel: '',
    socket: null,
    paused: false,
    streams: {
      stdout: makeStreamState('stdout-container'),
      stderr: makeStreamState('stderr-container'),
    },
  };

  async function postForm(form: HTMLFormElement): Promise<void> {
    const method = (form.getAttribute('method') || 'post').toUpperCase();
    const action = form.getAttribute('action') || window.location.href;
    const body = new FormData(form);

    try {
      await fetch(action, {
        method,
        body,
        credentials: 'same-origin',
        headers: { 'X-FWS-AJAX': '1' },
      });
    } catch {
      // ignore; websocket snapshot loop will reconcile when possible
    }
  }

  async function copyText(value: unknown): Promise<void> {
    const text = String(value ?? '');
    if (!text) {
      return;
    }
    try {
      await navigator.clipboard.writeText(text);
      return;
    } catch {
      // fallback below
    }
    const el = document.createElement('textarea');
    el.value = text;
    el.style.position = 'fixed';
    el.style.opacity = '0';
    document.body.appendChild(el);
    el.focus();
    el.select();
    try {
      document.execCommand('copy');
    } catch {
      // ignore
    } finally {
      document.body.removeChild(el);
    }
  }

  function flashCopied(field: Element | null): void {
    if (!(field instanceof HTMLElement)) {
      return;
    }
    field.classList.add('is-copied');
    window.setTimeout(() => field.classList.remove('is-copied'), 500);
  }

  function setStatus(text: string, connected: boolean): void {
    if (!statusEl) {
      return;
    }
    statusEl.textContent = text;
    statusEl.classList.toggle('disconnected', !connected);
  }

  function setLogStatus(text: string, connected: boolean): void {
    if (!logStatusEl) {
      return;
    }
    logStatusEl.textContent = text;
    logStatusEl.classList.toggle('disconnected', !connected);
  }

  function setCardCollapsed(card: Element | null, collapsed: boolean): void {
    if (!(card instanceof HTMLElement)) {
      return;
    }
    card.classList.toggle('is-collapsed', collapsed);
    const btn = card.querySelector<HTMLElement>('[data-collapse-toggle]');
    if (btn) {
      btn.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
      btn.textContent = collapsed ? 'Expand' : 'Collapse';
    }
  }

  function applyCollapseState(root: ParentNode | null): void {
    if (!root) {
      return;
    }
    const cards = root.querySelectorAll<HTMLElement>('[data-shell-id]');
    const visibleIds = new Set<string>();
    cards.forEach((card) => {
      const id = card.getAttribute('data-shell-id') || '';
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

  function updateToggleAllLabel(): void {
    if (!toggleAllBtn || !content) {
      return;
    }
    const cards = content.querySelectorAll<HTMLElement>('[data-shell-id]');
    if (cards.length === 0) {
      toggleAllBtn.disabled = true;
      toggleAllBtn.textContent = 'Expand All';
      return;
    }
    toggleAllBtn.disabled = false;
    const allCollapsed = Array.from(cards).every((card) => card.classList.contains('is-collapsed'));
    toggleAllBtn.textContent = allCollapsed ? 'Expand All' : 'Collapse All';
  }

  function setAllCollapsed(collapsed: boolean): void {
    defaultCollapsed = collapsed;
    if (!content) {
      return;
    }
    const cards = content.querySelectorAll<HTMLElement>('[data-shell-id]');
    cards.forEach((card) => {
      const id = card.getAttribute('data-shell-id') || '';
      if (id) {
        collapseState.set(id, collapsed);
      }
      setCardCollapsed(card, collapsed);
    });
    updateToggleAllLabel();
  }

  function persistGroupExpanded(): void {
    try {
      window.localStorage.setItem(GROUP_EXPANDED_KEY, JSON.stringify(groupExpanded));
    } catch {
      // ignore
    }
  }

  function setGroupCollapsed(card: Element | null, collapsed: boolean): void {
    if (!(card instanceof HTMLElement)) {
      return;
    }
    card.classList.toggle('is-collapsed', collapsed);
    const btn = card.querySelector<HTMLElement>('[data-group-toggle]');
    if (btn) {
      btn.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
      btn.textContent = collapsed ? 'Expand' : 'Collapse';
    }
  }

  function applyGroupState(root: ParentNode | null): void {
    if (!root) {
      return;
    }
    const cards = root.querySelectorAll<HTMLElement>('[data-group-id]');
    cards.forEach((card) => {
      const id = card.getAttribute('data-group-id') || '';
      if (!id) {
        return;
      }
      const expanded = groupExpanded[id] === true;
      setGroupCollapsed(card, !expanded);
    });
  }

  function getExitedExpandedDefault(): boolean {
    try {
      return window.localStorage.getItem(EXITED_EXPANDED_KEY) === '1';
    } catch {
      return false;
    }
  }

  function setExitedExpanded(expanded: boolean): void {
    if (!content) {
      return;
    }
    const exitedContent = content.querySelector<HTMLElement>('#fws-exited-content');
    const exitedToggle = content.querySelector<HTMLElement>('#fws-exited-toggle');
    if (!exitedContent || !exitedToggle) {
      return;
    }
    exitedContent.classList.toggle('is-collapsed', !expanded);
    exitedToggle.setAttribute('aria-expanded', expanded ? 'true' : 'false');
    exitedToggle.textContent = expanded ? 'Collapse Exited' : 'Expand Exited';
    try {
      window.localStorage.setItem(EXITED_EXPANDED_KEY, expanded ? '1' : '0');
    } catch {
      // ignore
    }
  }

  async function ensureExitedLoaded(forceReload: boolean): Promise<void> {
    if (!content) {
      return;
    }
    const exitedContent = content.querySelector<HTMLElement>('#fws-exited-content');
    if (!exitedContent) {
      return;
    }
    const token = exitedContent.getAttribute('data-token') || '';
    if (!forceReload && exitedContent.getAttribute('data-loaded') === '1') {
      return;
    }
    if (!forceReload && exitedCache.html && exitedCache.token === token) {
      exitedContent.innerHTML = exitedCache.html;
      exitedContent.setAttribute('data-loaded', '1');
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
        const res = await fetch('/fws/exited', { credentials: 'same-origin', cache: 'no-store' });
        const html = await res.text();
        exitedContent.innerHTML = html;
        exitedContent.setAttribute('data-loaded', '1');
        exitedCache.html = html;
        exitedCache.token = token;
        applyCollapseState(exitedContent);
        applyExitedPagination();
      } catch {
        exitedContent.innerHTML = '<div class="shell-card"><div class="shell-meta">Failed to load exited shells.</div></div>';
        exitedContent.setAttribute('data-loaded', '0');
      } finally {
        exitedCache.loading = null;
      }
    })();
    return exitedCache.loading;
  }

  function applyExitedSectionState(): void {
    const expanded = getExitedExpandedDefault();
    setExitedExpanded(expanded);
    if (expanded) {
      void ensureExitedLoaded(false);
    }
  }

  function applyExitedPagination(): void {
    if (!content) {
      return;
    }
    const exitedContent = content.querySelector<HTMLElement>('#fws-exited-content');
    if (!exitedContent) {
      return;
    }
    const items = Array.from(exitedContent.querySelectorAll<HTMLElement>('[data-exited-item="1"]'));
    const moreBtn = exitedContent.querySelector<HTMLElement>('#fws-exited-more');
    if (items.length === 0) {
      if (moreBtn) {
        moreBtn.style.display = 'none';
      }
      return;
    }
    items.forEach((item, idx) => {
      item.style.display = idx < exitedVisibleCount ? '' : 'none';
    });
    if (!moreBtn) {
      return;
    }
    if (items.length <= exitedVisibleCount) {
      moreBtn.style.display = 'none';
      return;
    }
    moreBtn.style.display = '';
    moreBtn.textContent = `More (${items.length - exitedVisibleCount})`;
  }

  function hasActiveFilters(stream: LogStreamName): boolean {
    const includeInput = getElementById<HTMLInputElement>(`${stream}-include-input`);
    const excludeInput = getElementById<HTMLInputElement>(`${stream}-exclude-input`);
    return Boolean((includeInput?.value || '').trim() || (excludeInput?.value || '').trim());
  }

  function compileMatcher(stream: LogStreamName, kind: 'include' | 'exclude', query: string, mode: FilterMode): Matcher {
    const input = getElementById<HTMLInputElement>(`${stream}-${kind}-input`);
    input?.classList.remove('invalid');
    if (!query) {
      return () => kind === 'include';
    }
    if (mode === 'exact') {
      return (line: string) => line === query;
    }
    try {
      const re = new RegExp(query);
      return (line: string) => re.test(line);
    } catch {
      input?.classList.add('invalid');
      return () => kind !== 'include';
    }
  }

  function getFilterConfig(stream: LogStreamName): FilterConfig {
    const includeInput = getElementById<HTMLInputElement>(`${stream}-include-input`);
    const excludeInput = getElementById<HTMLInputElement>(`${stream}-exclude-input`);
    const includeMode = document.querySelector<HTMLInputElement>(`input[name="${stream}-include-mode"]:checked`);
    const excludeMode = document.querySelector<HTMLInputElement>(`input[name="${stream}-exclude-mode"]:checked`);
    return {
      includeQuery: (includeInput?.value || '').trim(),
      excludeQuery: (excludeInput?.value || '').trim(),
      includeMode: includeMode?.value === 'exact' ? 'exact' : 'regex',
      excludeMode: excludeMode?.value === 'exact' ? 'exact' : 'regex',
    };
  }

  function getFilteredLines(stream: LogStreamName): string[] {
    const state = logState.streams[stream];
    const cfg = getFilterConfig(stream);
    const includeMatch = compileMatcher(stream, 'include', cfg.includeQuery, cfg.includeMode);
    const excludeMatch = compileMatcher(stream, 'exclude', cfg.excludeQuery, cfg.excludeMode);
    const allLines = state.partial ? state.lines.concat([state.partial]) : state.lines.slice();
    return allLines.filter((line) => {
      const includeOk = cfg.includeQuery ? includeMatch(line) : true;
      const excludeHit = cfg.excludeQuery ? excludeMatch(line) : false;
      return includeOk && !excludeHit;
    });
  }

  function isPinned(container: HTMLElement | null): boolean {
    if (!container) {
      return true;
    }
    return Math.abs(container.scrollHeight - container.scrollTop - container.clientHeight) < 12;
  }

  function setPendingLabel(stream: LogStreamName): void {
    const state = logState.streams[stream];
    const container = state.container;
    if (!container) {
      return;
    }
    container.classList.toggle('is-paused', logState.paused && state.pendingCount > 0);
    const label = state.pendingCount > 0 ? `${state.pendingCount} new line${state.pendingCount === 1 ? '' : 's'} buffered` : '';
    container.setAttribute('data-pending-label', label);
  }

  function buildLineNodes(lines: string[]): DocumentFragment {
    const fragment = document.createDocumentFragment();
    const wrapper = document.createElement('div');
    wrapper.className = 'log-lines';
    for (const line of lines) {
      const node = document.createElement('div');
      node.className = 'log-line';
      node.textContent = line;
      wrapper.appendChild(node);
    }
    fragment.appendChild(wrapper);
    return fragment;
  }

  function renderStream(stream: LogStreamName): void {
    const state = logState.streams[stream];
    const container = state.container;
    if (!container) {
      return;
    }
    const pinned = isPinned(container);
    const lines = getFilteredLines(stream);
    container.innerHTML = '';
    if (lines.length === 0) {
      const empty = document.createElement('div');
      empty.className = 'loading';
      empty.textContent = logState.shellId ? 'No lines matched.' : 'Select a shell log.';
      container.appendChild(empty);
    } else {
      container.appendChild(buildLineNodes(lines));
    }
    if (pinned) {
      container.scrollTop = container.scrollHeight;
    }
    setPendingLabel(stream);
  }

  function appendLines(stream: LogStreamName, newLines: string[], partialLine: string): void {
    const state = logState.streams[stream];
    const container = state.container;
    if (!container) {
      return;
    }
    const pinned = isPinned(container);
    let wrapper = container.querySelector<HTMLElement>('.log-lines');
    if (!wrapper) {
      container.innerHTML = '';
      wrapper = document.createElement('div');
      wrapper.className = 'log-lines';
      container.appendChild(wrapper);
    }

    for (const line of newLines) {
      const node = document.createElement('div');
      node.className = 'log-line';
      node.textContent = line;
      wrapper.appendChild(node);
    }

    let partialNode = wrapper.querySelector<HTMLElement>('.log-line.is-partial');
    if (partialLine) {
      if (!partialNode) {
        partialNode = document.createElement('div');
        partialNode.className = 'log-line is-partial';
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

  function parseTextIntoState(stream: LogStreamName, text: string): void {
    const state = logState.streams[stream];
    const normalized = String(text || '');
    const parts = normalized.split('\n');
    state.partial = normalized.endsWith('\n') ? '' : parts.pop() || '';
    state.lines = parts;
    state.pendingCount = 0;
    setPendingLabel(stream);
  }

  function appendChunkToState(stream: LogStreamName, chunk: string): { newLines: string[]; partialLine: string } {
    const state = logState.streams[stream];
    const text = `${state.partial}${String(chunk || '')}`;
    const parts = text.split('\n');
    state.partial = text.endsWith('\n') ? '' : parts.pop() || '';
    const newLines = parts;
    if (newLines.length > 0) {
      state.lines.push(...newLines);
    }
    return { newLines, partialLine: state.partial };
  }

  function resetStream(stream: LogStreamName): void {
    const state = logState.streams[stream];
    state.lines = [];
    state.partial = '';
    state.pendingCount = 0;
    renderStream(stream);
  }

  function closeLogSocket(): void {
    if (!logState.socket) {
      return;
    }
    try {
      logState.socket.close();
    } catch {
      // ignore
    }
    logState.socket = null;
  }

  function syncLogUrl(shellId: string, replace: boolean): void {
    const url = new URL(window.location.href);
    if (shellId) {
      url.searchParams.set('log', shellId);
    } else {
      url.searchParams.delete('log');
    }
    if (replace) {
      window.history.replaceState({ log: shellId || null }, '', url);
    } else {
      window.history.pushState({ log: shellId || null }, '', url);
    }
  }

  function openLogDrawer(shellId: string, shellLabel: string, options: DrawerOptions = {}): void {
    const nextShellId = String(shellId || '').trim();
    if (!nextShellId || !logDrawer) {
      return;
    }

    if (!options.fromPopState) {
      const sameShell = logState.shellId === nextShellId;
      syncLogUrl(nextShellId, sameShell);
    }

    document.body.classList.add('has-log-drawer');
    logDrawer.classList.add('is-open');
    logDrawer.setAttribute('aria-hidden', 'false');
    logState.shellId = nextShellId;
    logState.shellLabel = shellLabel || nextShellId;
    if (logTitleEl) {
      logTitleEl.textContent = logState.shellLabel || 'Shell Logs';
    }
    if (logSubtitleEl) {
      logSubtitleEl.textContent = nextShellId;
    }
    setLogStatus('Connecting...', false);

    for (const stream of LOG_STREAMS) {
      parseTextIntoState(stream, '');
      const state = logState.streams[stream];
      if (state.container) {
        state.container.innerHTML = '<div class="loading">Connecting...</div>';
      }
    }

    closeLogSocket();

    const socket = new ReconnectingWebSocket(getWebSocketUrl(`/ws/fws/logs/${encodeURIComponent(nextShellId)}`), {
      reconnectInterval: 1000,
      maxReconnectInterval: 5000,
      reconnectDecay: 1.5,
    });
    logState.socket = socket;

    socket.onopen = () => {
      if (logState.socket !== socket) {
        return;
      }
      setLogStatus('Connected', true);
      socket.send(stringifyClientNotification('fws.logs.connect', { shell_id: nextShellId }));
    };

    socket.onmessage = (event: MessageEvent) => {
      if (logState.socket !== socket) {
        return;
      }
      const notification = parseServerNotificationData(event.data);
      if (!notification) {
        return;
      }
      switch (notification.method) {
        case 'fws.logs.initial':
          parseTextIntoState('stdout', notification.params.stdout);
          parseTextIntoState('stderr', notification.params.stderr);
          renderStream('stdout');
          renderStream('stderr');
          break;
        case 'fws.logs.reset':
          resetStream(notification.params.stream);
          break;
        case 'fws.logs.chunk': {
          const stream = notification.params.stream;
          const appended = appendChunkToState(stream, notification.params.chunk);
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
        case 'fws.error':
          for (const stream of LOG_STREAMS) {
            const state = logState.streams[stream];
            if (state.container) {
              state.container.innerHTML = `<div class="loading">${notification.params.message}</div>`;
            }
          }
          if (notification.params.shell_id === nextShellId || notification.params.shell_id === undefined) {
            setLogStatus('Error', false);
          }
          break;
        default:
          break;
      }
    };

    socket.onreconnect = (_attempt: number, delayMs: number) => {
      if (logState.socket !== socket) {
        return;
      }
      setLogStatus(`Reconnecting in ${Math.round(delayMs)}ms...`, false);
    };

    socket.onerror = () => {
      if (logState.socket !== socket) {
        return;
      }
      setLogStatus('Error', false);
    };

    socket.onclose = () => {
      if (logState.socket !== socket) {
        return;
      }
      setLogStatus('Disconnected', false);
    };
  }

  function closeLogDrawer(options: DrawerOptions = {}): void {
    if (!logDrawer) {
      return;
    }
    closeLogSocket();
    logState.shellId = '';
    logState.shellLabel = '';
    logDrawer.classList.remove('is-open');
    logDrawer.setAttribute('aria-hidden', 'true');
    document.body.classList.remove('has-log-drawer');
    setLogStatus('Disconnected', false);
    if (!options.fromPopState) {
      syncLogUrl('', true);
    }
  }

  function wireFilters(stream: LogStreamName): void {
    const includeInput = getElementById<HTMLInputElement>(`${stream}-include-input`);
    const excludeInput = getElementById<HTMLInputElement>(`${stream}-exclude-input`);
    const radios = document.querySelectorAll<HTMLInputElement>(`input[name="${stream}-include-mode"], input[name="${stream}-exclude-mode"]`);
    let timer = 0;
    const apply = (): void => renderStream(stream);
    const applyDebounced = (): void => {
      if (timer) {
        window.clearTimeout(timer);
      }
      timer = window.setTimeout(apply, 200);
    };
    includeInput?.addEventListener('input', applyDebounced);
    excludeInput?.addEventListener('input', applyDebounced);
    radios.forEach((radio) => radio.addEventListener('change', apply));
  }

  wireFilters('stdout');
  wireFilters('stderr');

  if (logPauseInput) {
    logPauseInput.addEventListener('change', () => {
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

  logBackBtn?.addEventListener('click', () => closeLogDrawer());

  window.addEventListener('popstate', () => {
    const url = new URL(window.location.href);
    const shellId = url.searchParams.get('log');
    if (shellId) {
      openLogDrawer(shellId, shellId, { fromPopState: true });
      return;
    }
    closeLogDrawer({ fromPopState: true });
  });

  document.addEventListener('submit', (event: SubmitEvent) => {
    const target = event.target;
    if (!(target instanceof HTMLFormElement) || !target.matches('form[data-fws-ajax="1"]')) {
      return;
    }

    event.preventDefault();

    const confirmText = target.getAttribute('data-confirm');
    if (confirmText && !window.confirm(confirmText)) {
      return;
    }

    void postForm(target);
  });

  document.addEventListener('click', (event: MouseEvent) => {
    const target = event.target;
    if (!isElement(target)) {
      return;
    }

    const toggle = target.closest('[data-collapse-toggle]');
    if (toggle) {
      event.preventDefault();
      const card = toggle.closest('[data-shell-id]');
      if (!card) {
        return;
      }
      const id = card.getAttribute('data-shell-id') || '';
      const nextCollapsed = !card.classList.contains('is-collapsed');
      if (id) {
        collapseState.set(id, nextCollapsed);
      }
      setCardCollapsed(card, nextCollapsed);
      updateToggleAllLabel();
      return;
    }

    const groupToggle = target.closest('[data-group-toggle]');
    if (groupToggle) {
      event.preventDefault();
      const card = groupToggle.closest('[data-group-id]');
      if (!card) {
        return;
      }
      const id = card.getAttribute('data-group-id') || '';
      if (!id) {
        return;
      }
      const currentlyCollapsed = card.classList.contains('is-collapsed');
      const expanded = currentlyCollapsed;
      groupExpanded[id] = expanded;
      persistGroupExpanded();
      setGroupCollapsed(card, !expanded);
      return;
    }

    if (toggleAllBtn && target.closest('#fws-toggle-all')) {
      event.preventDefault();
      const shouldCollapse = toggleAllBtn.textContent === 'Collapse All';
      setAllCollapsed(shouldCollapse);
      return;
    }

    if (target.closest('#fws-exited-toggle')) {
      event.preventDefault();
      const toggleBtn = target.closest<HTMLElement>('#fws-exited-toggle');
      if (!toggleBtn) {
        return;
      }
      const expanded = toggleBtn.getAttribute('aria-expanded') === 'true';
      setExitedExpanded(!expanded);
      if (!expanded) {
        void ensureExitedLoaded(false);
      }
      return;
    }

    if (target.closest('#fws-exited-more')) {
      event.preventDefault();
      exitedVisibleCount += EXITED_PAGE_SIZE;
      applyExitedPagination();
      return;
    }

    const logButton = target.closest<HTMLElement>('[data-log-open]');
    if (logButton) {
      event.preventDefault();
      openLogDrawer(logButton.getAttribute('data-log-open') || '', logButton.getAttribute('data-log-label') || '');
      return;
    }

    const copyButton = target.closest('.copy-overlay');
    const copyField = target.closest('.copy-field');
    if (!copyField) {
      return;
    }

    if (!copyButton && target.closest('a,button,form,input,textarea,select,label')) {
      return;
    }

    const value = copyField.getAttribute('data-copy') || '';
    void copyText(value);
    flashCopied(copyField);
  });

  document.addEventListener('keydown', (event: KeyboardEvent) => {
    const target = event.target;
    if (!(target instanceof HTMLElement) || !target.classList.contains('copy-field')) {
      return;
    }
    if (event.key !== 'Enter' && event.key !== ' ') {
      return;
    }
    event.preventDefault();
    const value = target.getAttribute('data-copy') || '';
    void copyText(value);
    flashCopied(target);
  });

  const ws = new ReconnectingWebSocket(getWebSocketUrl('/ws/fws'), {
    reconnectInterval: 1500,
    maxReconnectInterval: 5000,
    reconnectDecay: 1.5,
  });

  ws.onopen = () => {
    setStatus('Live', true);
    ws.send(stringifyClientNotification('fws.dashboard.connect', { view: 'html' }));
  };

  ws.onmessage = (event: MessageEvent) => {
    const notification = parseServerNotificationData(event.data);
    if (!notification || notification.method !== 'fws.dashboard.snapshot') {
      return;
    }
    if (content) {
      content.innerHTML = notification.params.html;
      applyCollapseState(content);
      applyGroupState(content);
      applyExitedSectionState();
    }
  };

  ws.onreconnect = (_attempt: number, delayMs: number) => {
    setStatus(`Reconnecting in ${Math.round(delayMs)}ms...`, false);
  };

  ws.onerror = () => {
    setStatus('Error', false);
  };

  ws.onclose = () => {
    setStatus('Disconnected', false);
  };

  updateToggleAllLabel();

  const initialLog = new URL(window.location.href).searchParams.get('log');
  if (initialLog) {
    openLogDrawer(initialLog, initialLog, { fromPopState: true });
  }
})();
