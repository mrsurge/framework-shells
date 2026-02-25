(function () {
  const content = document.getElementById('fws-content');
  const statusEl = document.getElementById('fws-status');
  const toggleAllBtn = document.getElementById('fws-toggle-all');
  const EXITED_EXPANDED_KEY = 'fws.exited.expanded';
  const EXITED_PAGE_SIZE = 50;
  const SUBGROUP_EXPANDED_KEY = 'fws.subgroup.expanded';

  const collapseState = new Map();
  let defaultCollapsed = true;
  let exitedVisibleCount = EXITED_PAGE_SIZE;
  let subgroupExpanded = {};

  try {
    const raw = localStorage.getItem(SUBGROUP_EXPANDED_KEY);
    if (raw) {
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === 'object') subgroupExpanded = parsed;
    }
  } catch (err) {
    subgroupExpanded = {};
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

  function persistSubgroupExpanded() {
    try {
      localStorage.setItem(SUBGROUP_EXPANDED_KEY, JSON.stringify(subgroupExpanded));
    } catch (err) {
      // ignore
    }
  }

  function setSubgroupCollapsed(card, collapsed) {
    if (!card) return;
    card.classList.toggle('is-collapsed', collapsed);
    const btn = card.querySelector('[data-subgroup-toggle]');
    if (btn) {
      btn.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
      btn.textContent = collapsed ? 'Expand' : 'Collapse';
    }
  }

  function applySubgroupState(root) {
    if (!root) return;
    const cards = root.querySelectorAll('[data-subgroup-id]');
    cards.forEach((card) => {
      const id = card.getAttribute('data-subgroup-id') || '';
      if (!id) return;
      const expanded = subgroupExpanded[id] === 1 || subgroupExpanded[id] === true || subgroupExpanded[id] === '1';
      setSubgroupCollapsed(card, !expanded);
    });
  }

  function getExitedExpandedDefault() {
    try {
      return localStorage.getItem(EXITED_EXPANDED_KEY) !== '0';
    } catch (err) {
      return true;
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

  function applyExitedPagination() {
    if (!content) return;
    const items = Array.from(content.querySelectorAll('[data-exited-item="1"]'));
    const moreBtn = content.querySelector('#fws-exited-more');
    if (!items.length) {
      if (moreBtn) moreBtn.style.display = 'none';
      return;
    }
    items.forEach((item, idx) => {
      item.style.display = idx < exitedVisibleCount ? '' : 'none';
    });
    if (!moreBtn) return;
    if (items.length <= exitedVisibleCount) {
      moreBtn.style.display = 'none';
      return;
    }
    moreBtn.style.display = '';
    const remaining = items.length - exitedVisibleCount;
    moreBtn.textContent = `More (${remaining})`;
  }

  function applyExitedSectionState() {
    setExitedExpanded(getExitedExpandedDefault());
    applyExitedPagination();
  }

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

    const subgroupToggle = e.target.closest('[data-subgroup-toggle]');
    if (subgroupToggle) {
      e.preventDefault();
      const card = subgroupToggle.closest('[data-subgroup-id]');
      if (!card) return;
      const id = card.getAttribute('data-subgroup-id') || '';
      if (!id) return;
      const currentlyCollapsed = card.classList.contains('is-collapsed');
      const expanded = currentlyCollapsed;
      subgroupExpanded[id] = expanded ? 1 : 0;
      persistSubgroupExpanded();
      setSubgroupCollapsed(card, !expanded);
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
      const expanded = e.target.closest('#fws-exited-toggle').getAttribute('aria-expanded') === 'true';
      setExitedExpanded(!expanded);
      return;
    }

    if (e.target.closest('#fws-exited-more')) {
      e.preventDefault();
      exitedVisibleCount += EXITED_PAGE_SIZE;
      applyExitedPagination();
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
              applySubgroupState(content);
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
})();
