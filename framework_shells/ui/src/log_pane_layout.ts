export type PaneId = 'stdin' | 'stdout' | 'stderr';
const IDS: PaneId[] = ['stdin', 'stdout', 'stderr'];
const PREFIX = 'fws.log.panes.v1.';
export interface PaneState {
  collapsed: Record<PaneId, boolean>;
  sizes: Record<string, number[]>;
}

export function readPaneState(raw: string | null): PaneState {
  const result: PaneState = {collapsed: {stdin: true, stdout: false, stderr: true}, sizes: {}};
  try {
    const value: unknown = JSON.parse(raw ?? 'null');
    if (!value || typeof value !== 'object') return result;
    const stored = value as Record<string, unknown>;
    if (stored.collapsed && typeof stored.collapsed === 'object') {
      const collapsed = stored.collapsed as Record<string, unknown>;
      for (const id of IDS) if (typeof collapsed[id] === 'boolean') result.collapsed[id] = collapsed[id];
    }
    if (stored.sizes && typeof stored.sizes === 'object') {
      for (const [key, weights] of Object.entries(stored.sizes)) {
        const ids = key.split(',');
        if (!ids.length || ids.length > 3 || new Set(ids).size !== ids.length || !ids.every(id => IDS.includes(id as PaneId))) continue;
        if (Array.isArray(weights) && weights.length === ids.length && weights.every((n: unknown) => typeof n === 'number' && Number.isFinite(n) && n > 0 && n <= 1000000)) {
          result.sizes[key] = weights as number[];
        }
      }
    }
  } catch { /* Invalid or unavailable storage uses the default layout. */ }
  return result;
}

export function resizePair(heights: number[], index: number, delta: number): number[] {
  const next = heights.slice();
  const a = next[index];
  const b = next[index + 1];
  if (a === undefined || b === undefined || !Number.isFinite(delta)) return next;
  const total = a + b;
  const minimum = Math.min(100, total / 4);
  next[index] = Math.max(minimum, Math.min(total - minimum, a + delta));
  next[index + 1] = total - next[index]!;
  return next;
}

export function bindLogPaneLayout(drawer: HTMLElement | null, stdin: HTMLFormElement | null) {
  const body = drawer?.querySelector<HTMLElement>('.log-drawer-body');
  const panes = new Map<PaneId, HTMLElement>();
  const toggles = new Map<PaneId, HTMLButtonElement>();
  let shell = '';
  let state = readPaneState(null);
  let available = false;
  let dragCancel: (() => void) | null = null;
  const get = (key: string): string | null => { try { return localStorage.getItem(key); } catch { return null; } };
  const put = (key: string, value: string): void => { try { localStorage.setItem(key, value); } catch { /* Storage can be disabled. */ } };
  const save = (): void => { if (shell) put(PREFIX + shell, JSON.stringify(state)); };

  if (body && drawer) {
    for (const id of IDS) {
      const pane = id === 'stdin' ? stdin : drawer.querySelector<HTMLElement>(`#${id}-container`)?.closest<HTMLElement>('.log-pane');
      if (!pane) continue;
      panes.set(id, pane);
      pane.dataset.logPane = id;
      pane.classList.add('log-pane');
      const header = pane.querySelector<HTMLElement>(id === 'stdin' ? '.stdin-injector-header' : '.log-pane-header');
      const title = header?.querySelector<HTMLElement>(id === 'stdin' ? '.stdin-injector-title' : '.log-pane-title');
      if (!header || !title) continue;
      const toggle = document.createElement('button');
      toggle.type = 'button';
      toggle.className = `${title.className} pane-toggle`;
      toggle.textContent = title.textContent;
      toggle.id = `fws-${id}-pane-toggle`;
      toggle.setAttribute('aria-controls', id === 'stdin' ? 'fws-stdin-input' : `${id}-container`);
      title.replaceWith(toggle);
      toggles.set(id, toggle);
      header.title = 'Tap the header to expand or collapse';
      header.addEventListener('click', event => {
        const target = event.target;
        if (!(target instanceof Element)) return;
        const control = target.closest('button,input,label,textarea,select,a,.filters');
        if (control && control !== toggle) return;
        state.collapsed[id] = !state.collapsed[id];
        apply();
        save();
      });
    }
    stdin?.remove();
  }

  function expanded(): PaneId[] {
    return IDS.filter(id => (id !== 'stdin' || available) && panes.has(id) && !state.collapsed[id]);
  }

  function applyWeights(ids: PaneId[], weights: number[]): void {
    ids.forEach((id, i) => panes.get(id)?.style.setProperty('flex-grow', String(weights[i] ?? 1)));
  }

  function apply(): void {
    if (!body) return;
    dragCancel?.();
    body.querySelectorAll('.log-pane-splitter').forEach(el => el.remove());
    const ids = expanded();
    for (const [id, pane] of panes) {
      pane.classList.toggle('is-collapsed', state.collapsed[id]);
      pane.style.flexGrow = state.collapsed[id] ? '0' : '1';
      toggles.get(id)?.setAttribute('aria-expanded', String(!state.collapsed[id]));
    }
    applyWeights(ids, state.sizes[ids.join(',')] ?? ids.map(() => 1));
    ids.slice(0, -1).forEach((id, index) => {
      const handle = document.createElement('div');
      handle.className = 'log-pane-splitter';
      handle.tabIndex = 0;
      handle.setAttribute('role', 'separator');
      handle.setAttribute('aria-orientation', 'horizontal');
      handle.setAttribute('aria-label', `Resize ${id} and ${ids[index + 1]}`);
      handle.setAttribute('aria-valuemin', '0');
      handle.setAttribute('aria-valuemax', '100');
      const heights = (): number[] => ids.map(key => panes.get(key)!.getBoundingClientRect().height);
      const update = (values: number[]): void => {
        state.sizes[ids.join(',')] = values;
        applyWeights(ids, values);
        handle.setAttribute('aria-valuenow', String(Math.round(100 * values[index]! / (values[index]! + values[index + 1]!))));
      };
      const weights = state.sizes[ids.join(',')] ?? ids.map(() => 1);
      handle.setAttribute('aria-valuenow', String(Math.round(100 * weights[index]! / (weights[index]! + weights[index + 1]!))));
      handle.addEventListener('pointerdown', event => {
        if (event.button !== 0) return;
        event.preventDefault();
        const initial = heights();
        const start = event.clientY;
        const move = (next: PointerEvent): void => { if (next.pointerId === event.pointerId) update(resizePair(initial, index, next.clientY - start)); };
        const finish = (): void => {
          handle.removeEventListener('pointermove', move);
          handle.removeEventListener('pointerup', end);
          handle.removeEventListener('pointercancel', end);
          handle.removeEventListener('lostpointercapture', finish);
          handle.classList.remove('is-dragging');
          dragCancel = null;
          save();
        };
        const end = (next: PointerEvent): void => { if (next.pointerId === event.pointerId) finish(); };
        dragCancel?.();
        dragCancel = finish;
        handle.classList.add('is-dragging');
        handle.setPointerCapture(event.pointerId);
        handle.addEventListener('pointermove', move);
        handle.addEventListener('pointerup', end);
        handle.addEventListener('pointercancel', end);
        handle.addEventListener('lostpointercapture', finish);
      });
      handle.addEventListener('keydown', event => {
        if (event.key !== 'ArrowUp' && event.key !== 'ArrowDown') return;
        event.preventDefault();
        update(resizePair(heights(), index, event.key === 'ArrowUp' ? -20 : 20));
        save();
      });
      panes.get(id)?.after(handle);
    });
  }

  const wrap = drawer?.querySelector<HTMLInputElement>('#fws-log-wrap');
  if (wrap && drawer) {
    wrap.checked = get('fws.log.wrap') !== 'false';
    drawer.classList.toggle('log-nowrap', !wrap.checked);
    if (stdin) stdin.querySelector('textarea')?.setAttribute('wrap', wrap.checked ? 'soft' : 'off');
    wrap.addEventListener('change', () => {
      drawer.classList.toggle('log-nowrap', !wrap.checked);
      if (stdin) stdin.querySelector('textarea')?.setAttribute('wrap', wrap.checked ? 'soft' : 'off');
      put('fws.log.wrap', String(wrap.checked));
      drawer.dispatchEvent(new Event('fws-wrap-change'));
    });
  }

  return {
    open(shellId: string): void {
      dragCancel?.();
      shell = shellId;
      state = readPaneState(get(PREFIX + shell));
      apply();
      if (drawer && get('fws.log.panes.hint') !== 'seen') {
        put('fws.log.panes.hint', 'seen');
        const hint = document.createElement('div');
        hint.className = 'log-pane-hint';
        hint.setAttribute('role', 'status');
        hint.textContent = 'Tap a header to expand or collapse. Drag dividers to resize.';
        drawer.appendChild(hint);
        hint.addEventListener('click', () => hint.remove());
        window.setTimeout(() => hint.remove(), 5000);
      }
    },
    setStdinAvailable(enabled: boolean): void {
      if (!body || !stdin || enabled === available) return;
      available = enabled;
      if (enabled) body.prepend(stdin);
      else stdin.remove();
      apply();
    },
  };
}
