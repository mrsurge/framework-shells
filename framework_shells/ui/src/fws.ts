import { connectSocketIo, type SocketIoSocket } from './socketio_client';
import { initFwsConsoleBridge } from './te2_console_bridge';
import {
  advanceAnsiStyle,
  cloneAnsiStyle,
  createDefaultAnsiStyle,
  renderLogLine,
  type AnsiStyle,
  type TextHighlightSpec,
} from './ansi_json_log_renderer';
import {
  buildClientRequest,
  type ClientRequestMap,
  type ClientRequestMethod,
  type DashboardProcessPayload,
  type DashboardShellPayload,
  type DashboardStatePayload,
  type IncomingJsonRpcMessage,
  type LogStreamName,
  type RequestResultMap,
  type ServerNotification,
  coerceIncomingJsonRpcMessage,
} from './protocol';

const LOG_STREAMS: LogStreamName[] = ['stdout', 'stderr'];
const EXITED_EXPANDED_KEY = 'fws.exited.expanded';
const GROUP_EXPANDED_KEY = 'fws.group.expanded';
const LOG_RENDER_OPTIONS_KEY = 'fws.log.render.options';
const EXITED_PAGE_SIZE = 50;
const CSS_COLOR_RE = /^[#()0-9a-zA-Z.,%\s-]+$/;

type FilterMode = 'regex' | 'exact';

interface StreamState {
  container: HTMLElement | null;
  lines: string[];
  partial: string;
  pendingCount: number;
  ansiStyle: AnsiStyle;
  prettyJson: boolean;
}

interface LogState {
  shellId: string;
  shellLabel: string;
  paused: boolean;
  streams: Record<LogStreamName, StreamState>;
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

interface StreamRenderOptions {
  prettyJson: boolean;
}

interface StoredShellLogRenderOptions {
  stdout?: StreamRenderOptions;
  stderr?: StreamRenderOptions;
}

interface SubgroupStyle {
  bg?: string;
  border?: string;
  color?: string;
}

type Matcher = (line: string) => boolean;
type SubgroupStyleMap = Record<string, SubgroupStyle>;
type DashboardStateResult = RequestResultMap['fws.dashboard.open'] | RequestResultMap['fws.dashboard.refresh'];
type StoredLogRenderOptions = Record<string, StoredShellLogRenderOptions>;

const FWS_SOCKETIO_NAMESPACE = '/fws';
const FWS_SOCKETIO_PATH = '/fws_ws/socket.io';

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

function normalizeStreamRenderOptions(value: unknown): StreamRenderOptions {
  if (!isRecord(value)) {
    return { prettyJson: false };
  }
  return { prettyJson: normalizeStoredBoolean(value.prettyJson) };
}

function parseStoredLogRenderOptions(raw: string | null): StoredLogRenderOptions {
  if (!raw) {
    return {};
  }
  try {
    const parsed: unknown = JSON.parse(raw);
    if (!isRecord(parsed)) {
      return {};
    }
    const result: StoredLogRenderOptions = {};
    for (const [shellId, value] of Object.entries(parsed)) {
      if (!isRecord(value)) {
        continue;
      }
      const shellOptions: StoredShellLogRenderOptions = {};
      if ('stdout' in value) {
        shellOptions.stdout = normalizeStreamRenderOptions(value.stdout);
      }
      if ('stderr' in value) {
        shellOptions.stderr = normalizeStreamRenderOptions(value.stderr);
      }
      if (shellOptions.stdout || shellOptions.stderr) {
        result[shellId] = shellOptions;
      }
    }
    return result;
  } catch {
    return {};
  }
}

function getStoredStreamRenderOptions(
  store: StoredLogRenderOptions,
  shellId: string,
  stream: LogStreamName,
): StreamRenderOptions {
  return store[shellId]?.[stream] ?? { prettyJson: false };
}

function makeStreamState(containerId: string): StreamState {
  return {
    container: getElementById<HTMLElement>(containerId),
    lines: [],
    partial: '',
    pendingCount: 0,
    ansiStyle: createDefaultAnsiStyle(),
    prettyJson: false,
  };
}

function escapeHtml(value: unknown): string {
  return String(value ?? '')
    .split('&').join('&amp;')
    .split('<').join('&lt;')
    .split('>').join('&gt;')
    .split('"').join('&quot;')
    .split("'").join('&#39;');
}

function fmtBytes(value: unknown): string {
  if (typeof value !== 'number' || !Number.isFinite(value) || value <= 0) {
    return '0';
  }
  const mib = value / (1024 * 1024);
  if (mib >= 1024) {
    return `${(mib / 1024).toFixed(1)} GiB`;
  }
  return `${Math.round(mib)} MiB`;
}

function fmtCpu(value: unknown): string {
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) {
    return '-';
  }
  return `${value.toFixed(1)}%`;
}

function shellBackend(info: DashboardShellPayload): string {
  let backend = '';
  if (typeof info.backend === 'string' && info.backend) {
    backend = info.backend;
  } else if (info.uses_dtach) {
    backend = 'dtach';
  } else if (info.uses_pipes) {
    backend = 'pipe';
  } else if (info.uses_pty) {
    backend = 'pty';
  } else {
    backend = 'proc';
  }

  const engine = info.pipe_runtime?.engine;
  if (backend === 'pipe' && engine === 'native-pipe') {
    return 'pipe:native-pipe';
  }
  if (backend === 'pipe' && engine === 'native-terminal-pipe') {
    return 'pipe:native-terminal-pipe';
  }
  if (backend === 'pipe' && engine === 'python-terminal-pipe') {
    return 'pipe:python-terminal-pipe';
  }
  return backend;
}

function isShellLive(info: DashboardShellPayload): boolean {
  if (info.status !== 'running') {
    return false;
  }
  if (typeof info.pid !== 'number' || info.pid <= 0) {
    return false;
  }
  if (info.stats?.alive === false) {
    return false;
  }
  return true;
}

function safeCssValue(value: unknown): string {
  const text = String(value ?? '').trim();
  if (!text || !CSS_COLOR_RE.test(text)) {
    return '';
  }
  return text;
}

function globMatches(pattern: string, value: string): boolean {
  const escaped = pattern.replace(/[.+^${}()|[\]\\]/g, '\\$&');
  const normalized = escaped.split('*').join('.*').split('?').join('.');
  try {
    return new RegExp(`^${normalized}$`).test(value);
  } catch {
    return false;
  }
}

function collectSubgroupStyles(shells: DashboardShellPayload[]): SubgroupStyleMap {
  const merged: SubgroupStyleMap = {};
  for (const shell of shells) {
    if (!isRecord(shell.ui)) {
      continue;
    }
    const raw = shell.ui.subgroup_styles ?? shell.ui.subgroupStyles;
    if (!isRecord(raw)) {
      continue;
    }
    for (const [key, styleValue] of Object.entries(raw)) {
      if (!isRecord(styleValue)) {
        continue;
      }
      const normalized: SubgroupStyle = {};
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

function subgroupStyleFor(name: string, styles: SubgroupStyleMap): SubgroupStyle {
  if (!name) {
    return {};
  }
  if (styles[name]) {
    return styles[name];
  }
  let bestKey: string | null = null;
  for (const pattern of Object.keys(styles)) {
    if (pattern === name) {
      bestKey = pattern;
      break;
    }
    if ((pattern.includes('*') || pattern.includes('?')) && globMatches(pattern, name)) {
      if (bestKey === null || pattern.length > bestKey.length) {
        bestKey = pattern;
      }
    }
  }
  return bestKey ? (styles[bestKey] ?? {}) : {};
}

function cardStyleForSubgroups(subgroups: string[], styles: SubgroupStyleMap): SubgroupStyle {
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

function renderSubgroupPills(subgroups: string[], styles: SubgroupStyleMap): string {
  const pills: string[] = [];
  for (const subgroup of subgroups) {
    const name = subgroup.trim();
    if (!name) {
      continue;
    }
    const style = subgroupStyleFor(name, styles);
    const cssBits: string[] = [];
    if (style.bg) {
      cssBits.push(`background: ${style.bg};`);
    }
    if (style.border) {
      cssBits.push(`border-color: ${style.border};`);
    }
    if (style.color) {
      cssBits.push(`color: ${style.color};`);
    }
    const styleAttr = cssBits.length > 0 ? ` style="${cssBits.join(' ')}"` : '';
    pills.push(`<span class="pill"${styleAttr}>${escapeHtml(name)}</span>`);
  }
  if (pills.length === 0) {
    return '';
  }
  return `<div class="row">${pills.join('')}</div>`;
}

function renderCopyField(label: string, value: unknown, extraClasses = ''): string {
  const raw = String(value ?? '');
  const classes = extraClasses ? `copy-field ${extraClasses}` : 'copy-field';
  return (
    `<div class="${classes}" data-copy="${escapeHtml(raw)}" role="button" tabindex="0">` +
    `<div class="copy-field-label">${escapeHtml(label)}</div>` +
    `<div class="copy-field-value">${escapeHtml(raw)}</div>` +
    '<button class="copy-overlay" type="button" aria-label="Copy field value">Copy</button>' +
    '</div>'
  );
}

function exitedTimestamp(shell: DashboardShellPayload): number {
  if (typeof shell.updated_at === 'number') {
    return shell.updated_at;
  }
  if (typeof shell.created_at === 'number') {
    return shell.created_at;
  }
  return 0;
}

function fmtExitedTimestamp(timestamp: number): string {
  if (!(timestamp > 0)) {
    return 'Unknown time';
  }
  const dt = new Date(timestamp * 1000);
  const now = new Date();
  const sameDay =
    dt.getFullYear() === now.getFullYear() &&
    dt.getMonth() === now.getMonth() &&
    dt.getDate() === now.getDate();
  if (sameDay) {
    return dt.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', hour12: false });
  }
  return dt.toLocaleString([], {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  });
}

function hasLogPaths(shell: DashboardShellPayload): boolean {
  return Boolean((shell.stdout_log ?? '').trim() || (shell.stderr_log ?? '').trim());
}

function renderExitedContent(exited: DashboardShellPayload[], subgroupStyles: SubgroupStyleMap): string {
  if (exited.length === 0) {
    return '<div class="shell-card"><div class="shell-meta">No exited shells.</div></div>';
  }

  const parts: string[] = [];
  const sortedExited = exited.slice().sort((left, right) => exitedTimestamp(right) - exitedTimestamp(left));
  for (const shell of sortedExited) {
    const shellId = shell.id ?? '';
    const label = shell.label ?? shellId;
    const status = shell.status ?? 'exited';
    const exitCode = shell.exit_code;
    const timestamp = exitedTimestamp(shell);
    const timeLabel = fmtExitedTimestamp(timestamp);
    const subgroups = shell.subgroups ?? [];
    const style = cardStyleForSubgroups(subgroups, subgroupStyles);
    const styleBits: string[] = [];
    if (style.bg) {
      styleBits.push(`background: ${style.bg};`);
    }
    if (style.border) {
      styleBits.push(`border-color: ${style.border}; border-left: 4px solid ${style.border};`);
    }
    const styleAttr = styleBits.length > 0 ? ` style="${styleBits.join(' ')}"` : '';
    const statusMeta = exitCode === null || exitCode === undefined ? status : `${status} · exit: ${exitCode}`;
    const commandText = (shell.command ?? []).join(' ');

    parts.push(`<div class="exited-item" data-exited-item="1" data-exited-ts="${escapeHtml(timestamp)}">`);
    parts.push(`<div class="exited-ts">${escapeHtml(timeLabel)}</div>`);
    parts.push(`<div class="shell-card shell-entry is-collapsed"${styleAttr} data-shell-id="${escapeHtml(shellId)}">`);
    parts.push('<div class="shell-header">');
    parts.push(`<div class="shell-title">${escapeHtml(label)}</div>`);
    parts.push('<div class="shell-actions">');
    parts.push(`<button class="btn btn-small" type="button" data-collapse-toggle="${escapeHtml(shellId)}" aria-expanded="false">Expand</button>`);
    if (hasLogPaths(shell)) {
      parts.push(
        `<button class="btn btn-small" type="button" data-log-open="${escapeHtml(shellId)}" data-log-label="${escapeHtml(label)}">Logs</button>`,
      );
    } else {
      parts.push('<button class="btn btn-small" type="button" disabled>Logs Purged</button>');
    }
    parts.push(
      `<form method="post" action="/fws/action/shell/${encodeURIComponent(shellId)}/purge" data-fws-ajax="1"><button class="btn btn-small" type="submit">Purge</button></form>`,
    );
    parts.push('</div>');
    parts.push('</div>');
    parts.push(`<div class="shell-details" data-collapse-content="${escapeHtml(shellId)}">`);
    parts.push(renderCopyField('Status', statusMeta));
    parts.push(renderCopyField('ID', shellId));
    parts.push(renderCopyField('Command', commandText, 'copy-field--multiline'));
    parts.push(renderCopyField('stdout log', shell.stdout_log ?? '', 'copy-field--path'));
    parts.push(renderCopyField('stderr log', shell.stderr_log ?? '', 'copy-field--path'));
    const pills = renderSubgroupPills(subgroups, subgroupStyles);
    if (pills) {
      parts.push(pills);
    }
    parts.push('</div>');
    parts.push('</div>');
    parts.push('</div>');
  }
  if (exited.length > EXITED_PAGE_SIZE) {
    parts.push('<div class="row exited-more-row">');
    parts.push('<button class="btn btn-small" type="button" id="fws-exited-more">More</button>');
    parts.push('</div>');
  }
  return parts.join('\n');
}

function renderDashboardContent(state: DashboardStatePayload): string {
  const shellPidSet = new Set<number>();
  for (const shell of state.shells) {
    if (typeof shell.pid === 'number') {
      shellPidSet.add(shell.pid);
    }
  }

  const childrenByParent = new Map<number, DashboardProcessPayload[]>();
  for (const process of state.processes) {
    if (typeof process.parent_pid !== 'number') {
      continue;
    }
    const siblings = childrenByParent.get(process.parent_pid) ?? [];
    siblings.push(process);
    childrenByParent.set(process.parent_pid, siblings);
  }

  const running = state.shells.filter((shell) => isShellLive(shell));
  const exited = state.shells.filter((shell) => !isShellLive(shell));
  const subgroupStyles = collectSubgroupStyles(state.shells);
  const parts: string[] = [];

  parts.push('<div class="section">');
  parts.push(`<div class="section-title">Running <span class="muted">(${running.length})</span></div>`);

  if (running.length === 0) {
    parts.push('<div class="shell-card"><div class="shell-meta">No running shells.</div></div>');
  } else {
    const groups = new Map<string, Map<string, DashboardShellPayload[]>>();
    for (const shell of running) {
      const normalized = (shell.subgroups ?? []).map((value) => value.trim()).filter((value) => value.length > 0);
      const umbrella = normalized[0] ?? '(ungrouped)';
      const subgroup = normalized[1] ?? '(root)';
      const subgroupMap = groups.get(umbrella) ?? new Map<string, DashboardShellPayload[]>();
      const shells = subgroupMap.get(subgroup) ?? [];
      shells.push(shell);
      subgroupMap.set(subgroup, shells);
      groups.set(umbrella, subgroupMap);
    }

    const umbrellas = Array.from(groups.keys()).sort((left, right) => {
      if (left === '(ungrouped)') {
        return 1;
      }
      if (right === '(ungrouped)') {
        return -1;
      }
      return left.localeCompare(right);
    });

    for (const umbrella of umbrellas) {
      const subgroupMap = groups.get(umbrella) ?? new Map<string, DashboardShellPayload[]>();
      const totalShells = Array.from(subgroupMap.values()).reduce((sum, shells) => sum + shells.length, 0);

      parts.push(`<div class="group-card is-collapsed" data-group-id="${escapeHtml(umbrella)}">`);
      parts.push('<div class="group-header">');
      parts.push(`<div class="group-title">${escapeHtml(umbrella)}</div>`);
      parts.push('<div class="shell-actions">');
      parts.push(
        `<button class="btn btn-small" type="button" data-group-toggle="${escapeHtml(umbrella)}" aria-expanded="false">Expand</button>`,
      );
      if (umbrella !== '(ungrouped)') {
        parts.push(
          `<form method="post" action="/fws/action/app/${encodeURIComponent(umbrella)}/shutdown" data-fws-ajax="1"><button class="btn btn-small btn-danger" type="submit">Shutdown Group</button></form>`,
        );
      }
      parts.push('</div>');
      parts.push('</div>');
      parts.push(`<div class="group-meta">Shells: ${escapeHtml(totalShells)} · Subgroups: ${escapeHtml(subgroupMap.size)}</div>`);
      parts.push(`<div class="group-content" data-group-content="${escapeHtml(umbrella)}">`);

      const subgroups = Array.from(subgroupMap.keys()).sort((left, right) => {
        if (left === 'app-worker') {
          return -1;
        }
        if (right === 'app-worker') {
          return 1;
        }
        return left.localeCompare(right);
      });

      for (const subgroup of subgroups) {
        const style = subgroupStyleFor(subgroup, subgroupStyles);
        const styleBits: string[] = [];
        if (style.bg) {
          styleBits.push(`background: ${style.bg};`);
        }
        if (style.border) {
          styleBits.push(`border-color: ${style.border}; border-left: 4px solid ${style.border};`);
        }
        const styleAttr = styleBits.length > 0 ? ` style="${styleBits.join(' ')}"` : '';
        const shellsInGroup = (subgroupMap.get(subgroup) ?? []).slice().sort((left, right) => {
          const leftLabel = left.label ?? '';
          const rightLabel = right.label ?? '';
          const leftRank = leftLabel.startsWith('app-worker:') ? 0 : 1;
          const rightRank = rightLabel.startsWith('app-worker:') ? 0 : 1;
          if (leftRank !== rightRank) {
            return leftRank - rightRank;
          }
          const labelCompare = leftLabel.localeCompare(rightLabel);
          if (labelCompare !== 0) {
            return labelCompare;
          }
          return (left.id ?? '').localeCompare(right.id ?? '');
        });

        parts.push(`<div class="subgroup-card"${styleAttr}>`);
        parts.push('<div class="subgroup-header">');
        parts.push(`<div class="subgroup-title">${escapeHtml(subgroup)}</div>`);
        parts.push(`<div class="subgroup-count muted">(${shellsInGroup.length})</div>`);
        parts.push('</div>');

        for (const shell of shellsInGroup) {
          const shellId = shell.id ?? '';
          const label = shell.label ?? shellId;
          const pid = shell.pid;
          const subgroupsForShell = shell.subgroups ?? [];
          const rowStyle = cardStyleForSubgroups(subgroupsForShell, subgroupStyles);
          const rowStyleBits: string[] = [];
          if (rowStyle.bg) {
            rowStyleBits.push(`background: ${rowStyle.bg};`);
          }
          if (rowStyle.border) {
            rowStyleBits.push(`border-left: 3px solid ${rowStyle.border};`);
          }
          const rowStyleAttr = rowStyleBits.length > 0 ? ` style="${rowStyleBits.join(' ')}"` : '';
          const commandText = (shell.command ?? []).join(' ');
          const cpu = fmtCpu(shell.stats?.cpu_percent);
          const rss = fmtBytes(shell.stats?.memory_rss);
          const status = shell.status ?? 'running';

          parts.push(`<div class="shell-card shell-entry is-collapsed"${rowStyleAttr} data-shell-id="${escapeHtml(shellId)}">`);
          parts.push('<div class="shell-header">');
          parts.push(`<div class="shell-title">${escapeHtml(label)}</div>`);
          parts.push('<div class="shell-actions">');
          parts.push(`<button class="btn btn-small" type="button" data-collapse-toggle="${escapeHtml(shellId)}" aria-expanded="false">Expand</button>`);
          parts.push(
            `<button class="btn btn-small" type="button" data-log-open="${escapeHtml(shellId)}" data-log-label="${escapeHtml(label)}">Logs</button>`,
          );
          parts.push(
            `<form method="post" action="/fws/action/shell/${encodeURIComponent(shellId)}/terminate" data-fws-ajax="1"><button class="btn btn-small btn-danger" type="submit">Stop</button></form>`,
          );
          parts.push('</div>');
          parts.push('</div>');
          parts.push(`<div class="shell-details" data-collapse-content="${escapeHtml(shellId)}">`);
          parts.push(renderCopyField('Status', status));
          parts.push(renderCopyField('PID', pid ?? ''));
          parts.push(renderCopyField('ID', shellId));
          parts.push(renderCopyField('Backend', shellBackend(shell)));
          parts.push(renderCopyField('CPU', cpu));
          parts.push(renderCopyField('RSS', rss));
          parts.push(renderCopyField('Command', commandText, 'copy-field--multiline'));
          parts.push(renderCopyField('stdout log', shell.stdout_log ?? '', 'copy-field--path'));
          parts.push(renderCopyField('stderr log', shell.stderr_log ?? '', 'copy-field--path'));
          const pills = renderSubgroupPills(subgroupsForShell, subgroupStyles);
          if (pills) {
            parts.push(pills);
          }

          if (typeof pid === 'number' && childrenByParent.has(pid)) {
            const children = (childrenByParent.get(pid) ?? []).filter((child) => {
              return typeof child.pid !== 'number' || !shellPidSet.has(child.pid);
            });
            if (children.length > 0) {
              const sortedChildren = children.slice().sort((left, right) => {
                const typeCompare = (left.type ?? '').localeCompare(right.type ?? '');
                if (typeCompare !== 0) {
                  return typeCompare;
                }
                return (left.pid ?? 0) - (right.pid ?? 0);
              });
              parts.push('<div class="children">');
              parts.push(`<div class="children-title">Child Processes (${sortedChildren.length})</div>`);
              for (const child of sortedChildren) {
                const childPid = child.pid ?? '';
                const childType = child.type ?? 'proc';
                const childLabel = child.label ?? childPid;
                parts.push('<div class="child-row child-row--proc">');
                parts.push('<div class="child-main">');
                parts.push(`<div class="child-label">${escapeHtml(childLabel)}</div>`);
                parts.push('<div class="child-meta-line">');
                parts.push(`<div class="child-meta">PID: ${escapeHtml(childPid)} · ${escapeHtml(childType)}</div>`);
                parts.push('<div class="row child-actions-inline">');
                parts.push(
                  `<form method="post" action="/fws/action/pid/${encodeURIComponent(String(childPid))}/terminate" data-fws-ajax="1"><button class="btn btn-small btn-danger" type="submit">Kill</button></form>`,
                );
                parts.push('</div>');
                parts.push('</div>');
                parts.push('</div>');
                parts.push('</div>');
              }
              parts.push('</div>');
            }
          }

          parts.push('</div>');
          parts.push('</div>');
        }

        parts.push('</div>');
      }

      parts.push('</div>');
      parts.push('</div>');
    }
  }

  parts.push('</div>');
  parts.push('<div class="section section-exited" id="fws-exited">');
  parts.push('<div class="section-title">');
  parts.push(`Exited <span class="muted">(${exited.length})</span>`);
  parts.push('<div class="shell-actions">');
  parts.push('<button class="btn btn-small" type="button" id="fws-exited-toggle" aria-expanded="false">Expand Exited</button>');
  if (exited.length > 0) {
    parts.push(
      '<form method="post" action="/fws/action/exited/purge" data-fws-ajax="1" data-confirm="Purge ALL exited shells (delete their logs + metadata)?"><button class="btn btn-small btn-danger" type="submit">Purge Exited</button></form>',
    );
  }
  parts.push('</div>');
  parts.push('</div>');
  parts.push(`<div class="exited-content is-collapsed" id="fws-exited-content" data-count="${escapeHtml(exited.length)}">`);
  parts.push(renderExitedContent(exited, subgroupStyles));
  parts.push('</div>');
  parts.push('</div>');

  return parts.join('\n');
}

(() => {
  void initFwsConsoleBridge();

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
  let defaultCollapsed = true;
  let groupExpanded = parseStoredGroupExpanded(window.localStorage.getItem(GROUP_EXPANDED_KEY));
  let logRenderOptions = parseStoredLogRenderOptions(window.localStorage.getItem(LOG_RENDER_OPTIONS_KEY));
  let exitedVisibleCount = EXITED_PAGE_SIZE;
  let dashboardRequestCounter = 0;
  let dashboardState: DashboardStatePayload = { shells: [], processes: [] };
  let fwsSocket: SocketIoSocket | null = null;
  const fwsSocketReady = connectSocketIo(FWS_SOCKETIO_NAMESPACE, {
    path: FWS_SOCKETIO_PATH,
    transports: ['websocket'],
  });

  const logState: LogState = {
    shellId: '',
    shellLabel: '',
    paused: false,
    streams: {
      stdout: makeStreamState('stdout-container'),
      stderr: makeStreamState('stderr-container'),
    },
  };

  function nextDashboardRequestId(): string {
    dashboardRequestCounter += 1;
    return `fws_req_${dashboardRequestCounter}`;
  }

  async function getFwsSocket(): Promise<SocketIoSocket> {
    if (fwsSocket) {
      return fwsSocket;
    }
    fwsSocket = await fwsSocketReady;
    return fwsSocket;
  }

  function isJsonRpcErrorMessage(
    message: IncomingJsonRpcMessage,
  ): message is Extract<IncomingJsonRpcMessage, { error: unknown }> {
    return 'error' in message;
  }

  function isJsonRpcResponseMessage(
    message: IncomingJsonRpcMessage,
  ): message is Extract<IncomingJsonRpcMessage, { id: string }> {
    return 'id' in message && typeof message.id === 'string';
  }

  function isServerNotificationMessage(message: IncomingJsonRpcMessage): message is ServerNotification {
    return 'method' in message && 'params' in message;
  }

  function hasDashboardStateResult(result: unknown): result is DashboardStateResult {
    return (
      isRecord(result) &&
      isRecord(result.state) &&
      Array.isArray(result.state.shells) &&
      Array.isArray(result.state.processes)
    );
  }

  function findShellLabel(shellId: string): string {
    const match = dashboardState.shells.find((shell) => shell.id === shellId);
    return match?.label ?? shellId;
  }

  function compareShells(left: DashboardShellPayload, right: DashboardShellPayload): number {
    const leftCreated = typeof left.created_at === 'number' ? left.created_at : 0;
    const rightCreated = typeof right.created_at === 'number' ? right.created_at : 0;
    if (leftCreated !== rightCreated) {
      return leftCreated - rightCreated;
    }
    return (left.id ?? '').localeCompare(right.id ?? '');
  }

  function pruneProcessesForShell(
    processes: DashboardProcessPayload[],
    shellId: string,
    rootPid?: number | null,
  ): DashboardProcessPayload[] {
    const blockedPids = new Set<number>();
    for (const process of processes) {
      if (process.shell_id === shellId && typeof process.pid === 'number') {
        blockedPids.add(process.pid);
      }
    }
    if (typeof rootPid === 'number') {
      blockedPids.add(rootPid);
    }
    if (blockedPids.size > 0) {
      const queue = Array.from(blockedPids);
      while (queue.length > 0) {
        const parentPid = queue.shift();
        if (parentPid === undefined) {
          continue;
        }
        for (const process of processes) {
          if (process.parent_pid !== parentPid || typeof process.pid !== 'number' || blockedPids.has(process.pid)) {
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
      return typeof process.pid !== 'number' || !blockedPids.has(process.pid);
    });
  }

  function applyShellDelta(nextShell: DashboardShellPayload): void {
    const shellId = String(nextShell.id || '').trim();
    if (!shellId) {
      return;
    }
    const previousShell = dashboardState.shells.find((shell) => shell.id === shellId);
    const nextShells = dashboardState.shells.filter((shell) => shell.id !== shellId);
    nextShells.push(nextShell);

    let nextProcesses = dashboardState.processes.slice();
    const previousPid = typeof previousShell?.pid === 'number' ? previousShell.pid : undefined;
    const nextPid = typeof nextShell.pid === 'number' ? nextShell.pid : undefined;
    if (previousPid !== nextPid || !isShellLive(nextShell)) {
      nextProcesses = pruneProcessesForShell(nextProcesses, shellId, previousPid ?? nextPid);
    }

    applyDashboardState({
      shells: nextShells.sort(compareShells),
      processes: nextProcesses,
    });
    setStatus('Live', true);
  }

  function removeShellDelta(shellId: string): void {
    const normalizedShellId = String(shellId || '').trim();
    if (!normalizedShellId) {
      return;
    }
    const previousShell = dashboardState.shells.find((shell) => shell.id === normalizedShellId);
    if (!previousShell) {
      return;
    }
    const previousPid = typeof previousShell.pid === 'number' ? previousShell.pid : undefined;
    applyDashboardState({
      shells: dashboardState.shells.filter((shell) => shell.id !== normalizedShellId),
      processes: pruneProcessesForShell(dashboardState.processes, normalizedShellId, previousPid),
    });
    setStatus('Live', true);
  }

  function applyDashboardState(nextState: DashboardStatePayload): void {
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
        logTitleEl.textContent = label || 'Shell Logs';
      }
    }
  }

  function routeDashboardNotification(message: ServerNotification): void {
    switch (message.method) {
      case 'fws.shell.created':
      case 'fws.shell.spawned':
      case 'fws.shell.updated':
      case 'fws.shell.exited':
        applyShellDelta(message.params.shell);
        return;
      case 'fws.shell.removed':
        removeShellDelta(message.params.shell_id);
        return;
      case 'fws.error':
        if (!message.params.shell_id) {
          setStatus(message.params.message, false);
        }
        return;
      default:
        return;
    }
  }

  async function sendDashboardRequest<M extends ClientRequestMethod>(
    method: M,
    params: ClientRequestMap[M],
  ): Promise<RequestResultMap[M]> {
    const requestId = nextDashboardRequestId();
    const request = buildClientRequest(method, requestId, params);
    const socket = await getFwsSocket();
    return await new Promise<RequestResultMap[M]>((resolve, reject) => {
      socket.emit('fws_request', request, (payload: unknown) => {
        const message = coerceIncomingJsonRpcMessage(payload);
        if (!message || !isJsonRpcResponseMessage(message)) {
          reject(new Error(`Invalid response for ${method}`));
          return;
        }
        if (isJsonRpcErrorMessage(message)) {
          reject(new Error(message.error.message));
          return;
        }
        resolve(message.result as RequestResultMap[M]);
      });
    });
  }

  async function submitActionForm(form: HTMLFormElement): Promise<void> {
    const action = form.getAttribute('action') || window.location.href;
    const url = new URL(action, window.location.href);
    const path = url.pathname;
    const formData = new FormData(form);

    if (path === '/fws/action/refresh') {
      const result = await sendDashboardRequest('fws.dashboard.refresh', {});
      if (hasDashboardStateResult(result)) {
        applyDashboardState(result.state);
      }
      return;
    }
    if (path === '/fws/action/logs/purge') {
      await sendDashboardRequest('fws.logs.truncate', {});
      return;
    }
    if (path === '/fws/action/exited/purge') {
      await sendDashboardRequest('fws.exited.purge', {});
      return;
    }

    const shellTerminateMatch = path.match(/^\/fws\/action\/shell\/([^/]+)\/terminate$/);
    if (shellTerminateMatch) {
      await sendDashboardRequest('fws.shell.terminate', { shell_id: decodeURIComponent(shellTerminateMatch[1] ?? '') });
      return;
    }

    const shellPurgeMatch = path.match(/^\/fws\/action\/shell\/([^/]+)\/purge$/);
    if (shellPurgeMatch) {
      await sendDashboardRequest('fws.shell.purge', { shell_id: decodeURIComponent(shellPurgeMatch[1] ?? '') });
      return;
    }

    const pidTerminateMatch = path.match(/^\/fws\/action\/pid\/([^/]+)\/terminate$/);
    if (pidTerminateMatch) {
      const pid = Number.parseInt(decodeURIComponent(pidTerminateMatch[1] ?? ''), 10);
      if (Number.isFinite(pid)) {
        await sendDashboardRequest('fws.pid.terminate', { pid });
      }
      return;
    }

    const appShutdownMatch = path.match(/^\/fws\/action\/app\/([^/]+)\/shutdown$/);
    if (appShutdownMatch) {
      await sendDashboardRequest('fws.app.shutdown', { app_id: decodeURIComponent(appShutdownMatch[1] ?? '') });
      return;
    }

    if (path === '/fws/action/shutdown') {
      const scopeValue = String(formData.get('scope') ?? 'tree');
      const scope = scopeValue === 'shells' ? 'shells' : 'tree';
      await sendDashboardRequest('fws.shutdown', { scope });
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

  function applyExitedSectionState(): void {
    const expanded = getExitedExpandedDefault();
    setExitedExpanded(expanded);
    applyExitedPagination();
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

  function getFilterHighlight(stream: LogStreamName): TextHighlightSpec | undefined {
    const cfg = getFilterConfig(stream);
    if (!cfg.includeQuery) {
      return undefined;
    }
    if (cfg.includeMode === 'exact') {
      return { kind: 'line' };
    }
    try {
      // Compile here so invalid regex filters do not reach the renderer.
      new RegExp(cfg.includeQuery);
    } catch {
      return undefined;
    }
    return { kind: 'regex', source: cfg.includeQuery, flags: 'g' };
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

  function buildLineNodes(stream: LogStreamName, lines: string[]): DocumentFragment {
    const fragment = document.createDocumentFragment();
    const wrapper = document.createElement('div');
    wrapper.className = 'log-lines';
    const renderOptions = {
      prettyJson: logState.streams[stream].prettyJson,
      highlight: getFilterHighlight(stream),
    };
    let renderStyle = createDefaultAnsiStyle();
    for (const line of lines) {
      const node = document.createElement('div');
      node.className = 'log-line';
      const rendered = renderLogLine(line, renderStyle, renderOptions);
      node.appendChild(rendered.fragment);
      renderStyle = rendered.finalStyle;
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
      container.appendChild(buildLineNodes(stream, lines));
    }
    if (pinned) {
      container.scrollTop = container.scrollHeight;
    }
    setPendingLabel(stream);
  }

  function appendLines(stream: LogStreamName, newLines: string[], partialLine: string, initialAnsiStyle: AnsiStyle): void {
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

    const previousPartialNode = wrapper.querySelector<HTMLElement>('.log-line.is-partial');
    previousPartialNode?.remove();

    let renderStyle = cloneAnsiStyle(initialAnsiStyle);
    const renderOptions = {
      prettyJson: state.prettyJson,
      highlight: getFilterHighlight(stream),
    };
    for (const line of newLines) {
      const node = document.createElement('div');
      node.className = 'log-line';
      const rendered = renderLogLine(line, renderStyle, renderOptions);
      node.appendChild(rendered.fragment);
      renderStyle = rendered.finalStyle;
      wrapper.appendChild(node);
    }

    if (partialLine) {
      const partialNode = document.createElement('div');
      partialNode.className = 'log-line is-partial';
      wrapper.appendChild(partialNode);
      partialNode.replaceChildren(renderLogLine(partialLine, renderStyle, renderOptions).fragment);
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
    state.ansiStyle = createDefaultAnsiStyle();
    for (const line of state.lines) {
      state.ansiStyle = advanceAnsiStyle(line, state.ansiStyle);
    }
    state.pendingCount = 0;
    setPendingLabel(stream);
  }

  function appendChunkToState(
    stream: LogStreamName,
    chunk: string,
  ): { newLines: string[]; partialLine: string; initialAnsiStyle: AnsiStyle } {
    const state = logState.streams[stream];
    const initialAnsiStyle = cloneAnsiStyle(state.ansiStyle);
    const text = `${state.partial}${String(chunk || '')}`;
    const parts = text.split('\n');
    state.partial = text.endsWith('\n') ? '' : parts.pop() || '';
    const newLines = parts;
    if (newLines.length > 0) {
      state.lines.push(...newLines);
      for (const line of newLines) {
        state.ansiStyle = advanceAnsiStyle(line, state.ansiStyle);
      }
    }
    return { newLines, partialLine: state.partial, initialAnsiStyle };
  }

  function resetStream(stream: LogStreamName): void {
    const state = logState.streams[stream];
    state.lines = [];
    state.partial = '';
    state.pendingCount = 0;
    state.ansiStyle = createDefaultAnsiStyle();
    renderStream(stream);
  }

  function saveLogRenderOptions(): void {
    try {
      window.localStorage.setItem(LOG_RENDER_OPTIONS_KEY, JSON.stringify(logRenderOptions));
    } catch {
      // Non-critical preference write.
    }
  }

  function setStoredPrettyJson(shellId: string, stream: LogStreamName, enabled: boolean): void {
    if (!shellId) {
      return;
    }
    const shellOptions = logRenderOptions[shellId] ?? {};
    shellOptions[stream] = { prettyJson: enabled };
    logRenderOptions[shellId] = shellOptions;
    saveLogRenderOptions();
  }

  function syncPrettyJsonToggle(stream: LogStreamName): void {
    const input = getElementById<HTMLInputElement>(`${stream}-pretty-json`);
    if (input) {
      input.checked = logState.streams[stream].prettyJson;
    }
  }

  function applyStoredLogRenderOptions(shellId: string): void {
    for (const stream of LOG_STREAMS) {
      const options = getStoredStreamRenderOptions(logRenderOptions, shellId, stream);
      logState.streams[stream].prettyJson = options.prettyJson;
      syncPrettyJsonToggle(stream);
    }
  }

  function renderLogError(message: string): void {
    for (const stream of LOG_STREAMS) {
      const state = logState.streams[stream];
      if (state.container) {
        state.container.innerHTML = `<div class="loading">${escapeHtml(message)}</div>`;
      }
    }
  }

  function routeLogNotification(message: ServerNotification): void {
    const currentShellId = logState.shellId;
    if (!currentShellId) {
      return;
    }

    switch (message.method) {
      case 'fws.logs.initial':
        if (message.params.shell_id !== currentShellId) {
          return;
        }
        parseTextIntoState('stdout', message.params.stdout);
        parseTextIntoState('stderr', message.params.stderr);
        renderStream('stdout');
        renderStream('stderr');
        return;
      case 'fws.logs.reset':
        if (message.params.shell_id !== currentShellId) {
          return;
        }
        resetStream(message.params.stream);
        return;
      case 'fws.logs.chunk': {
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
      case 'fws.error':
        if (message.params.shell_id !== undefined && message.params.shell_id !== currentShellId) {
          return;
        }
        renderLogError(message.params.message);
        setLogStatus('Error', false);
        return;
      default:
        return;
    }
  }

  async function openLogSubscription(shellId: string): Promise<void> {
    try {
      await sendDashboardRequest('fws.logs.open', { shell_id: shellId });
      if (logState.shellId === shellId) {
        setLogStatus('Connected', true);
      }
    } catch (error) {
      if (logState.shellId !== shellId) {
        return;
      }
      renderLogError(error instanceof Error ? error.message : String(error));
      setLogStatus('Error', false);
    }
  }

  async function closeLogSubscription(shellId: string): Promise<void> {
    try {
      await sendDashboardRequest('fws.logs.close', { shell_id: shellId });
    } catch {
      // Best-effort cleanup only.
    }
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
    logState.shellLabel = shellLabel || findShellLabel(nextShellId);
    applyStoredLogRenderOptions(nextShellId);
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

    void getFwsSocket().then((socket) => {
      if (logState.shellId !== nextShellId || !socket.connected) {
        return;
      }
      void openLogSubscription(nextShellId);
    });
  }

  function closeLogDrawer(options: DrawerOptions = {}): void {
    if (!logDrawer) {
      return;
    }
    const previousShellId = logState.shellId;
    logState.shellId = '';
    logState.shellLabel = '';
    logDrawer.classList.remove('is-open');
    logDrawer.setAttribute('aria-hidden', 'true');
    document.body.classList.remove('has-log-drawer');
    setLogStatus('Disconnected', false);
    if (previousShellId) {
      void closeLogSubscription(previousShellId);
    }
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

  function wirePrettyJsonToggle(stream: LogStreamName): void {
    const input = getElementById<HTMLInputElement>(`${stream}-pretty-json`);
    input?.addEventListener('change', () => {
      const enabled = input.checked;
      logState.streams[stream].prettyJson = enabled;
      setStoredPrettyJson(logState.shellId, stream, enabled);
      renderStream(stream);
    });
  }

  wireFilters('stdout');
  wireFilters('stderr');
  wirePrettyJsonToggle('stdout');
  wirePrettyJsonToggle('stderr');

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
      openLogDrawer(shellId, findShellLabel(shellId), { fromPopState: true });
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

    void submitActionForm(target).catch(() => {
      setStatus('Error', false);
    });
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
        applyExitedPagination();
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

  void fwsSocketReady
    .then((socket) => {
      fwsSocket = socket;

      const handleConnect = (): void => {
        setStatus('Connecting...', false);
        if (logState.shellId) {
          setLogStatus('Connecting...', false);
        }
        void sendDashboardRequest('fws.dashboard.open', { view: 'html' })
          .then((result) => {
            if (hasDashboardStateResult(result)) {
              applyDashboardState(result.state);
              setStatus('Live', true);
            } else {
              setStatus('Error', false);
            }
          })
          .catch(() => setStatus('Error', false));
        if (logState.shellId) {
          void openLogSubscription(logState.shellId);
        }
      };

      socket.on('connect', handleConnect);
      socket.on('fws_notification', (payload: unknown) => {
        const message = coerceIncomingJsonRpcMessage(payload);
        if (!message || !isServerNotificationMessage(message)) {
          return;
        }
        routeDashboardNotification(message);
        routeLogNotification(message);
      });
      socket.on('connect_error', () => {
        setStatus('Error', false);
        if (logState.shellId) {
          setLogStatus('Error', false);
        }
      });
      socket.on('disconnect', () => {
        setStatus('Disconnected', false);
        if (logState.shellId) {
          setLogStatus('Disconnected', false);
        }
      });

      if (socket.connected) {
        handleConnect();
      }
    })
    .catch(() => {
      setStatus('Error', false);
      if (logState.shellId) {
        setLogStatus('Error', false);
      }
    });

  updateToggleAllLabel();

  const initialLog = new URL(window.location.href).searchParams.get('log');
  if (initialLog) {
    openLogDrawer(initialLog, initialLog, { fromPopState: true });
  }
})();
