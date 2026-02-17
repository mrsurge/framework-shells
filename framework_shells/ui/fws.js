(function () {
  const content = document.getElementById('fws-content');
  const statusEl = document.getElementById('fws-status');
  const toggleAllBtn = document.getElementById('fws-toggle-all');

  const collapseState = new Map();
  let defaultCollapsed = true;

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

    if (toggleAllBtn && e.target.closest('#fws-toggle-all')) {
      e.preventDefault();
      const shouldCollapse = toggleAllBtn.textContent === 'Collapse All';
      setAllCollapsed(shouldCollapse);
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
