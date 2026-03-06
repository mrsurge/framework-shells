(function () {
  const content = document.getElementById('fws-content');
  const statusEl = document.getElementById('fws-status');
  const toggleAllBtn = document.getElementById('fws-toggle-all');
  const logDrawer = document.getElementById('fws-log-drawer');
  const logBackBtn = document.getElementById('fws-log-back');
  const logTitleEl = document.getElementById('fws-log-title');
  const logSubtitleEl = document.getElementById('fws-log-subtitle');
  const logStatusEl = document.getElementById('fws-log-status');
  const logPauseInput = document.getElementById('fws-log-pause');
  const EXITED_EXPANDED_KEY = 'fws.exited.expanded';
  const GROUP_EXPANDED_KEY = 'fws.group.expanded';
  const collapseState = new Map();
  const exitedCache = { html: '', token: '', loading: null };
  let defaultCollapsed = true;
  let groupExpanded = {};

  const logState = {
    shellId: '',
    shellLabel: '',
    socket: null,
    paused: false,
    streams: {
      stdout: makeStreamState('stdout-container'),
      stderr: makeStreamState('stderr-container'),
    },
  };

  function makeStreamState(containerId) {
    const container = document.getElementById(containerId);
    return {
      container,
      lines: [],
      partial: '',
      pendingCount: 0,
    };
  }

  try {
    const raw = localStorage.getItem(GROUP_EXPANDED_KEY);
    if (raw) {
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === 'object') groupExpanded = parsed;
    }
  } catch (err) {
    groupExpanded = {};
  }

  async function postForm(form) {
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
    } catch (err) {
      // ignore; websocket snapshot loop will reconcile when possible
    }
  }

  async function copyText(value) {
    const text = String(value == null ? '' : value);
    if (!text) return;
    try {
      await navigator.clipboard.writeText(text);
      return;
    } catch (err) {
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
    } catch (err) {
      // ignore
    } finally {
      document.body.removeChild(el);
    }
  }

  function flashCopied(field) {
    if (!field) return;
    field.classList.add('is-copied');
    window.setTimeout(() => field.classList.remove('is-copied'), 500);
  }

  function setStatus(text, connected) {
    if (!statusEl) return;
    statusEl.textContent = text;
    if (connected) statusEl.classList.remove('disconnected');
    else statusEl.classList.add('disconnected');
  }

  function setLogStatus(text, connected) {
    if (!logStatusEl) return;
    logStatusEl.textContent = text;
    if (connected) logStatusEl.classList.remove('disconnected');
    else logStatusEl.classList.add('disconnected');
  }

  function setCardCollapsed(card, collapsed) {
    if (!card) return;
    card.classList.toggle('is-collapsed', collapsed);
    const btn = card.querySelector('[data-collapse-toggle]');
    if (btn) {
      btn.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
      btn.textContent = collapsed ? 'Expand' : 'Collapse';
    }
  }

  function applyCollapseState(root) {
    if (!root) return;
    const cards = root.querySelectorAll('[data-shell-id]');
    const visibleIds = new Set();
    cards.forEach((card) => {
      const id = card.getAttribute('data-shell-id') || '';
      if (!id) return;
      visibleIds.add(id);
      const collapsed = collapseState.has(id) ? collapseState.get(id) : defaultCollapsed;
      collapseState.set(id, !!collapsed);
      setCardCollapsed(card, !!collapsed);
    });

    for (const key of Array.from(collapseState.keys())) {
      if (!visibleIds.has(key)) collapseState.delete(key);
    }
    updateToggleAllLabel();
  }

  function updateToggleAllLabel() {
    if (!toggleAllBtn || !content) return;
    const cards = content.querySelectorAll('[data-shell-id]');
    if (!cards.length) {
      toggleAllBtn.disabled = true;
      toggleAllBtn.textContent = 'Expand All';
      return;
    }
    toggleAllBtn.disabled = false;
    const allCollapsed = Array.from(cards).every((card) => card.classList.contains('is-collapsed'));
    toggleAllBtn.textContent = allCollapsed ? 'Expand All' : 'Collapse All';
  }

  function setAllCollapsed(collapsed) {
    defaultCollapsed = !!collapsed;
    if (!content) return;
    const cards = content.querySelectorAll('[data-shell-id]');
    cards.forEach((card) => {
      const id = card.getAttribute('data-shell-id') || '';
      if (id) collapseState.set(id, !!collapsed);
      setCardCollapsed(card, !!collapsed);
    });
    updateToggleAllLabel();
  }

  function persistGroupExpanded() {
    try {
      localStorage.setItem(GROUP_EXPANDED_KEY, JSON.stringify(groupExpanded));
    } catch (err) {
      // ignore
    }
  }

  function setGroupCollapsed(card, collapsed) {
    if (!card) return;
    card.classList.toggle('is-collapsed', collapsed);
    const btn = card.querySelector('[data-group-toggle]');
    if (btn) {
      btn.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
      btn.textContent = collapsed ? 'Expand' : 'Collapse';
    }
  }

  function applyGroupState(root) {
    if (!root) return;
    const cards = root.querySelectorAll('[data-group-id]');
    cards.forEach((card) => {
      const id = card.getAttribute('data-group-id') || '';
      if (!id) return;
      const expanded = groupExpanded[id] === 1 || groupExpanded[id] === true || groupExpanded[id] === '1';
      setGroupCollapsed(card, !expanded);
    });
  }

  function getExitedExpandedDefault() {
    try {
      return localStorage.getItem(EXITED_EXPANDED_KEY) === '1';
    } catch (err) {
      return false;
    }
  }

  function setExitedExpanded(expanded) {
    if (!content) return;
    const exitedContent = content.querySelector('#fws-exited-content');
    const exitedToggle = content.querySelector('#fws-exited-toggle');
    if (!exitedContent || !exitedToggle) return;
    exitedContent.classList.toggle('is-collapsed', !expanded);
    exitedToggle.setAttribute('aria-expanded', expanded ? 'true' : 'false');
    exitedToggle.textContent = expanded ? 'Collapse Exited' : 'Expand Exited';
    try {
      localStorage.setItem(EXITED_EXPANDED_KEY, expanded ? '1' : '0');
    } catch (err) {
      // ignore
    }
  }

  async function ensureExitedLoaded(forceReload) {
    if (!content) return;
    const exitedContent = content.querySelector('#fws-exited-content');
    if (!exitedContent) return;
    const token = exitedContent.getAttribute('data-token') || '';
    if (!forceReload && exitedContent.getAttribute('data-loaded') === '1') return;
    if (!forceReload && exitedCache.html && exitedCache.token === token) {
      exitedContent.innerHTML = exitedCache.html;
      exitedContent.setAttribute('data-loaded', '1');
      applyCollapseState(exitedContent);
      return;
    }
    if (exitedCache.loading && !forceReload) return exitedCache.loading;
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
      } catch (err) {
        exitedContent.innerHTML = '<div class="shell-card"><div class="shell-meta">Failed to load exited shells.</div></div>';
        exitedContent.setAttribute('data-loaded', '0');
      } finally {
        exitedCache.loading = null;
      }
    })();
    return exitedCache.loading;
  }

  function applyExitedSectionState() {
    const expanded = getExitedExpandedDefault();
    setExitedExpanded(expanded);
    if (expanded) ensureExitedLoaded(false);
  }

  function hasActiveFilters(stream) {
    const includeInput = document.getElementById(`${stream}-include-input`);
    const excludeInput = document.getElementById(`${stream}-exclude-input`);
    return !!(((includeInput && includeInput.value) || '').trim() || ((excludeInput && excludeInput.value) || '').trim());
  }

  function compileMatcher(stream, kind, query, mode) {
    const input = document.getElementById(`${stream}-${kind}-input`);
    if (input) input.classList.remove('invalid');
    if (!query) return () => kind === 'include';
    if (mode === 'exact') {
      return (line) => line === query;
    }
    try {
      const re = new RegExp(query);
      return (line) => re.test(line);
    } catch (err) {
      if (input) input.classList.add('invalid');
      return () => kind !== 'include';
    }
  }

  function getFilterConfig(stream) {
    const includeInput = document.getElementById(`${stream}-include-input`);
    const excludeInput = document.getElementById(`${stream}-exclude-input`);
    const includeMode = document.querySelector(`input[name="${stream}-include-mode"]:checked`);
    const excludeMode = document.querySelector(`input[name="${stream}-exclude-mode"]:checked`);
    return {
      includeQuery: ((includeInput && includeInput.value) || '').trim(),
      excludeQuery: ((excludeInput && excludeInput.value) || '').trim(),
      includeMode: includeMode ? includeMode.value : 'regex',
      excludeMode: excludeMode ? excludeMode.value : 'regex',
    };
  }

  function getFilteredLines(stream) {
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

  function isPinned(container) {
    if (!container) return true;
    return Math.abs(container.scrollHeight - container.scrollTop - container.clientHeight) < 12;
  }

  function setPendingLabel(stream) {
    const state = logState.streams[stream];
    const container = state.container;
    if (!container) return;
    container.classList.toggle('is-paused', logState.paused && state.pendingCount > 0);
    container.setAttribute('data-pending-label', state.pendingCount > 0 ? `${state.pendingCount} new line${state.pendingCount === 1 ? '' : 's'} buffered` : '');
  }

  function buildLineNodes(lines) {
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

  function renderStream(stream) {
    const state = logState.streams[stream];
    const container = state.container;
    if (!container) return;
    const pinned = isPinned(container);
    const lines = getFilteredLines(stream);
    container.innerHTML = '';
    if (!lines.length) {
      const empty = document.createElement('div');
      empty.className = 'loading';
      empty.textContent = logState.shellId ? 'No lines matched.' : 'Select a shell log.';
      container.appendChild(empty);
    } else {
      container.appendChild(buildLineNodes(lines));
    }
    if (pinned) container.scrollTop = container.scrollHeight;
    setPendingLabel(stream);
  }

  function appendLines(stream, newLines, partialLine) {
    const state = logState.streams[stream];
    const container = state.container;
    if (!container) return;
    const pinned = isPinned(container);
    let wrapper = container.querySelector('.log-lines');
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

    let partialNode = wrapper.querySelector('.log-line.is-partial');
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

    if (pinned) container.scrollTop = container.scrollHeight;
  }

  function parseTextIntoState(stream, text) {
    const state = logState.streams[stream];
    const normalized = String(text || '');
    const parts = normalized.split('\n');
    state.partial = normalized.endsWith('\n') ? '' : parts.pop() || '';
    state.lines = parts;
    state.pendingCount = 0;
    setPendingLabel(stream);
  }

  function appendChunkToState(stream, chunk) {
    const state = logState.streams[stream];
    const text = `${state.partial}${String(chunk || '')}`;
    const parts = text.split('\n');
    state.partial = text.endsWith('\n') ? '' : parts.pop() || '';
    const newLines = parts;
    if (newLines.length) state.lines.push(...newLines);
    return { newLines, partialLine: state.partial };
  }

  function resetStream(stream) {
    const state = logState.streams[stream];
    state.lines = [];
    state.partial = '';
    state.pendingCount = 0;
    renderStream(stream);
  }

  function closeLogSocket() {
    if (logState.socket) {
      try {
        logState.socket.close();
      } catch (err) {
        // ignore
      }
      logState.socket = null;
    }
  }

  function syncLogUrl(shellId, replace) {
    const url = new URL(window.location.href);
    if (shellId) url.searchParams.set('log', shellId);
    else url.searchParams.delete('log');
    const method = replace ? 'replaceState' : 'pushState';
    window.history[method]({ log: shellId || null }, '', url);
  }

  function openLogDrawer(shellId, shellLabel, options) {
    const opts = options || {};
    const nextShellId = String(shellId || '').trim();
    if (!nextShellId || !logDrawer) return;

    if (!opts.fromPopState) {
      const sameShell = logState.shellId === nextShellId;
      syncLogUrl(nextShellId, sameShell);
    }

    document.body.classList.add('has-log-drawer');
    logDrawer.classList.add('is-open');
    logDrawer.setAttribute('aria-hidden', 'false');
    logState.shellId = nextShellId;
    logState.shellLabel = shellLabel || nextShellId;
    if (logTitleEl) logTitleEl.textContent = logState.shellLabel || 'Shell Logs';
    if (logSubtitleEl) logSubtitleEl.textContent = nextShellId;
    setLogStatus('Connecting...', false);

    for (const stream of ['stdout', 'stderr']) {
      parseTextIntoState(stream, '');
      const state = logState.streams[stream];
      if (state.container) state.container.innerHTML = '<div class=\"loading\">Connecting...</div>';
    }

    closeLogSocket();

    const scheme = window.location.protocol === 'https:' ? 'wss' : 'ws';
    const wsUrl = `${scheme}://${window.location.host}/ws/fws/logs/${encodeURIComponent(nextShellId)}`;
    const socket = new WebSocket(wsUrl);
    logState.socket = socket;

    socket.onopen = () => {
      if (logState.socket !== socket) return;
      setLogStatus('Connected', true);
    };

    socket.onmessage = (event) => {
      if (logState.socket !== socket) return;
      try {
        const msg = JSON.parse(event.data);
        if (msg.type === 'initial') {
          parseTextIntoState('stdout', msg.stdout || '');
          parseTextIntoState('stderr', msg.stderr || '');
          renderStream('stdout');
          renderStream('stderr');
          return;
        }
        if (msg.type === 'reset' && (msg.stream === 'stdout' || msg.stream === 'stderr')) {
          resetStream(msg.stream);
          return;
        }
        if (msg.type === 'update' && (msg.stream === 'stdout' || msg.stream === 'stderr')) {
          const stream = msg.stream;
          const appended = appendChunkToState(stream, msg.data || '');
          if (logState.paused) {
            logState.streams[stream].pendingCount += appended.newLines.length;
            setPendingLabel(stream);
            return;
          }
          if (hasActiveFilters(stream)) {
            renderStream(stream);
          } else {
            appendLines(stream, appended.newLines, appended.partialLine);
          }
          return;
        }
        if (msg.type === 'error') {
          for (const stream of ['stdout', 'stderr']) {
            const state = logState.streams[stream];
            state.container.innerHTML = `<div class="loading">${msg.message}</div>`;
          }
        }
      } catch (err) {
        // ignore
      }
    };

    socket.onerror = () => {
      if (logState.socket !== socket) return;
      setLogStatus('Error', false);
    };

    socket.onclose = () => {
      if (logState.socket !== socket) return;
      setLogStatus('Disconnected', false);
      logState.socket = null;
    };
  }

  function closeLogDrawer(options) {
    const opts = options || {};
    if (!logDrawer) return;
    closeLogSocket();
    logState.shellId = '';
    logState.shellLabel = '';
    logDrawer.classList.remove('is-open');
    logDrawer.setAttribute('aria-hidden', 'true');
    document.body.classList.remove('has-log-drawer');
    setLogStatus('Disconnected', false);
    if (!opts.fromPopState) syncLogUrl('', true);
  }

  function wireFilters(stream) {
    const includeInput = document.getElementById(`${stream}-include-input`);
    const excludeInput = document.getElementById(`${stream}-exclude-input`);
    const radios = document.querySelectorAll(`input[name="${stream}-include-mode"], input[name="${stream}-exclude-mode"]`);
    let timer = null;
    const apply = () => renderStream(stream);
    const applyDebounced = () => {
      if (timer) clearTimeout(timer);
      timer = setTimeout(apply, 200);
    };
    includeInput.addEventListener('input', applyDebounced);
    excludeInput.addEventListener('input', applyDebounced);
    radios.forEach((r) => r.addEventListener('change', apply));
  }

  wireFilters('stdout');
  wireFilters('stderr');

  if (logPauseInput) {
    logPauseInput.addEventListener('change', () => {
      logState.paused = !!logPauseInput.checked;
      if (!logState.paused) {
        for (const stream of ['stdout', 'stderr']) {
          logState.streams[stream].pendingCount = 0;
          renderStream(stream);
        }
      } else {
        for (const stream of ['stdout', 'stderr']) setPendingLabel(stream);
      }
    });
  }

  if (logBackBtn) {
    logBackBtn.addEventListener('click', () => closeLogDrawer());
  }

  window.addEventListener('popstate', () => {
    const url = new URL(window.location.href);
    const shellId = url.searchParams.get('log');
    if (shellId) {
      openLogDrawer(shellId, shellId, { fromPopState: true });
      return;
    }
    closeLogDrawer({ fromPopState: true });
  });

  document.addEventListener('submit', (e) => {
    const form = e.target;
    if (!form || !form.matches || !form.matches('form[data-fws-ajax="1"]')) return;

    e.preventDefault();

    const confirmText = form.getAttribute('data-confirm');
    if (confirmText && !window.confirm(confirmText)) return;

    postForm(form);
  });

  document.addEventListener('click', async (e) => {
    const toggle = e.target.closest('[data-collapse-toggle]');
    if (toggle) {
      e.preventDefault();
      const card = toggle.closest('[data-shell-id]');
      if (!card) return;
      const id = card.getAttribute('data-shell-id') || '';
      const nextCollapsed = !card.classList.contains('is-collapsed');
      if (id) collapseState.set(id, nextCollapsed);
      setCardCollapsed(card, nextCollapsed);
      updateToggleAllLabel();
      return;
    }

    const groupToggle = e.target.closest('[data-group-toggle]');
    if (groupToggle) {
      e.preventDefault();
      const card = groupToggle.closest('[data-group-id]');
      if (!card) return;
      const id = card.getAttribute('data-group-id') || '';
      if (!id) return;
      const currentlyCollapsed = card.classList.contains('is-collapsed');
      const expanded = currentlyCollapsed;
      groupExpanded[id] = expanded ? 1 : 0;
      persistGroupExpanded();
      setGroupCollapsed(card, !expanded);
      return;
    }

    if (toggleAllBtn && e.target.closest('#fws-toggle-all')) {
      e.preventDefault();
      const shouldCollapse = toggleAllBtn.textContent === 'Collapse All';
      setAllCollapsed(shouldCollapse);
      return;
    }

    if (e.target.closest('#fws-exited-toggle')) {
      e.preventDefault();
      const toggleBtn = e.target.closest('#fws-exited-toggle');
      const expanded = toggleBtn.getAttribute('aria-expanded') === 'true';
      setExitedExpanded(!expanded);
      if (!expanded) ensureExitedLoaded(false);
      return;
    }

    const logButton = e.target.closest('[data-log-open]');
    if (logButton) {
      e.preventDefault();
      openLogDrawer(logButton.getAttribute('data-log-open') || '', logButton.getAttribute('data-log-label') || '');
      return;
    }

    const copyButton = e.target.closest('.copy-overlay');
    const copyField = e.target.closest('.copy-field');
    if (!copyField) return;

    if (!copyButton && e.target.closest('a,button,form,input,textarea,select,label')) return;

    const value = copyField.getAttribute('data-copy') || '';
    await copyText(value);
    flashCopied(copyField);
  });

  document.addEventListener('keydown', async (e) => {
    const target = e.target;
    if (!target || !target.classList || !target.classList.contains('copy-field')) return;
    if (e.key !== 'Enter' && e.key !== ' ') return;
    e.preventDefault();
    const value = target.getAttribute('data-copy') || '';
    await copyText(value);
    flashCopied(target);
  });

  const scheme = window.location.protocol === 'https:' ? 'wss' : 'ws';
  const wsUrl = `${scheme}://${window.location.host}/ws/fws`;

  let ws = null;
  let reconnectTimer = null;

  function connect() {
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
    setStatus('Connecting...', false);
    try {
      ws = new WebSocket(wsUrl);
      ws.onopen = () => {
        setStatus('Live', true);
      };
      ws.onmessage = (event) => {
        try {
          const msg = JSON.parse(event.data);
          if (msg && msg.type === 'snapshot_html' && typeof msg.html === 'string') {
            if (content) {
              content.innerHTML = msg.html;
              applyCollapseState(content);
              applyGroupState(content);
              applyExitedSectionState();
            }
          }
        } catch (err) {
          // ignore
        }
      };
      ws.onclose = () => {
        setStatus('Disconnected', false);
        reconnectTimer = setTimeout(connect, 1500);
      };
      ws.onerror = () => {
        try {
          ws.close();
        } catch (err) {
          // ignore
        }
      };
    } catch (err) {
      reconnectTimer = setTimeout(connect, 2000);
    }
  }

  updateToggleAllLabel();
  connect();

  const initialLog = new URL(window.location.href).searchParams.get('log');
  if (initialLog) {
    openLogDrawer(initialLog, initialLog, { fromPopState: true });
  }
})();
