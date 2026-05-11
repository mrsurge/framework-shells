export interface AnsiStyle {
  fg?: string;
  bg?: string;
  bold: boolean;
  dim: boolean;
  italic: boolean;
  underline: boolean;
  inverse: boolean;
}

interface TextSegment {
  type: 'text';
  text: string;
  style: AnsiStyle;
}

interface ControlSegment {
  type: 'control';
  marker: string;
  kind: string;
  style: AnsiStyle;
}

type AnsiSegment = TextSegment | ControlSegment;

interface CsiSequence {
  params: number[];
  final: string;
  end: number;
}

interface JsonFragment {
  start: number;
  end: number;
  raw: string;
}

export interface RenderLogLineOptions {
  prettyJson?: boolean;
}

const MAX_JSON_FRAGMENT_CHARS = 200_000;
const DEFAULT_FG = '#c9d1d9';
const DEFAULT_BG = '#0d1117';

const ANSI_16_COLORS: Record<number, string> = {
  0: '#484f58',
  1: '#ff7b72',
  2: '#7ee787',
  3: '#d29922',
  4: '#79c0ff',
  5: '#d2a8ff',
  6: '#76e3ea',
  7: '#c9d1d9',
  8: '#6e7681',
  9: '#ffa198',
  10: '#56d364',
  11: '#e3b341',
  12: '#a5d6ff',
  13: '#d2a8ff',
  14: '#39c5cf',
  15: '#f0f6fc',
};

export function createDefaultAnsiStyle(): AnsiStyle {
  return {
    bold: false,
    dim: false,
    italic: false,
    underline: false,
    inverse: false,
  };
}

export function cloneAnsiStyle(style: AnsiStyle): AnsiStyle {
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

export function advanceAnsiStyle(text: string, initialStyle: AnsiStyle = createDefaultAnsiStyle()): AnsiStyle {
  return parseAnsiSegments(text, initialStyle).style;
}

export function renderLogLine(
  text: string,
  initialStyle: AnsiStyle = createDefaultAnsiStyle(),
  options: RenderLogLineOptions = {},
): { fragment: DocumentFragment; finalStyle: AnsiStyle } {
  const parsed = parseAnsiSegments(text, initialStyle);
  const fragment = document.createDocumentFragment();
  for (const segment of parsed.segments) {
    if (segment.type === 'control') {
      appendControlMarker(fragment, segment.marker, segment.kind, segment.style);
    } else {
      appendStyledText(fragment, segment.text, segment.style, options);
    }
  }
  return { fragment, finalStyle: parsed.style };
}

function parseAnsiSegments(text: string, initialStyle: AnsiStyle): { segments: AnsiSegment[]; style: AnsiStyle } {
  const segments: AnsiSegment[] = [];
  let style = cloneAnsiStyle(initialStyle);
  let buffer = '';

  const flush = (): void => {
    if (!buffer) {
      return;
    }
    segments.push({ type: 'text', text: buffer, style: cloneAnsiStyle(style) });
    buffer = '';
  };

  for (let index = 0; index < text.length; index += 1) {
    const code = text.charCodeAt(index);
    if (code === 0x1b) {
      const csi = parseCsiSequence(text, index);
      if (csi && csi.final === 'm') {
        flush();
        style = applySgrParams(style, csi.params);
        index = csi.end;
        continue;
      }
      flush();
      segments.push({ type: 'control', marker: '[ESC]', kind: 'esc', style: cloneAnsiStyle(style) });
      continue;
    }

    if (code === 0x08) {
      flush();
      segments.push({ type: 'control', marker: '[BS]', kind: 'backspace', style: cloneAnsiStyle(style) });
      continue;
    }
    if (code === 0x7f) {
      flush();
      segments.push({ type: 'control', marker: '[DEL]', kind: 'delete', style: cloneAnsiStyle(style) });
      continue;
    }
    if (code === 0x0d) {
      flush();
      segments.push({ type: 'control', marker: '[CR]', kind: 'carriage-return', style: cloneAnsiStyle(style) });
      continue;
    }
    if (code < 0x20 && code !== 0x09) {
      flush();
      segments.push({ type: 'control', marker: `[0x${code.toString(16).padStart(2, '0')}]`, kind: 'control', style: cloneAnsiStyle(style) });
      continue;
    }

    buffer += text[index] ?? '';
  }

  flush();
  return { segments, style };
}

function parseCsiSequence(text: string, start: number): CsiSequence | null {
  if (text[start] !== '\x1b' || text[start + 1] !== '[') {
    return null;
  }
  for (let index = start + 2; index < text.length && index < start + 80; index += 1) {
    const code = text.charCodeAt(index);
    if (code >= 0x40 && code <= 0x7e) {
      const rawParams = text.slice(start + 2, index);
      const params = rawParams.length === 0
        ? [0]
        : rawParams.split(';').map((part) => {
            const parsed = Number.parseInt(part || '0', 10);
            return Number.isFinite(parsed) ? parsed : 0;
          });
      return { params, final: text[index] ?? '', end: index };
    }
  }
  return null;
}

function applySgrParams(inputStyle: AnsiStyle, params: number[]): AnsiStyle {
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
      setColor(style, 'fg', ansi16Color(code - 30));
      continue;
    }
    if (code >= 40 && code <= 47) {
      setColor(style, 'bg', ansi16Color(code - 40));
      continue;
    }
    if (code >= 90 && code <= 97) {
      setColor(style, 'fg', ansi16Color(code - 90 + 8));
      continue;
    }
    if (code >= 100 && code <= 107) {
      setColor(style, 'bg', ansi16Color(code - 100 + 8));
      continue;
    }
    if (code === 38 || code === 48) {
      const target = code === 38 ? 'fg' : 'bg';
      const mode = effectiveParams[index + 1];
      if (mode === 5) {
        const color = effectiveParams[index + 2];
        if (typeof color === 'number') {
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

function isByte(value: number | undefined): value is number {
  return typeof value === 'number' && Number.isInteger(value) && value >= 0 && value <= 255;
}

function setColor(style: AnsiStyle, target: 'fg' | 'bg', color: string | null): void {
  if (!color) {
    return;
  }
  if (target === 'fg') {
    style.fg = color;
  } else {
    style.bg = color;
  }
}

function ansi16Color(index: number): string | null {
  return ANSI_16_COLORS[index] ?? null;
}

function ansi256Color(index: number): string | null {
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
  const green = Math.floor((offset % 36) / 6);
  const blue = offset % 6;
  return `rgb(${ansiCubeLevel(red)}, ${ansiCubeLevel(green)}, ${ansiCubeLevel(blue)})`;
}

function ansiCubeLevel(value: number): number {
  return value === 0 ? 0 : 55 + value * 40;
}

function appendControlMarker(parent: Node, marker: string, kind: string, style: AnsiStyle): void {
  const node = document.createElement('span');
  node.className = `ansi-control ansi-control-${kind}`;
  applyAnsiStyle(node, style);
  node.textContent = marker;
  parent.appendChild(node);
}

function appendStyledText(parent: Node, text: string, style: AnsiStyle, options: RenderLogLineOptions): void {
  if (!text) {
    return;
  }
  const target = hasVisibleAnsiStyle(style) ? document.createElement('span') : parent;
  if (target instanceof HTMLElement) {
    target.className = 'ansi-segment';
    applyAnsiStyle(target, style);
  }
  appendJsonHighlightedText(target, text, options);
  if (target !== parent) {
    parent.appendChild(target);
  }
}

function hasVisibleAnsiStyle(style: AnsiStyle): boolean {
  return Boolean(style.fg || style.bg || style.bold || style.dim || style.italic || style.underline || style.inverse);
}

function applyAnsiStyle(node: HTMLElement, style: AnsiStyle): void {
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
    node.classList.add('ansi-bold');
  }
  if (style.dim) {
    node.classList.add('ansi-dim');
  }
  if (style.italic) {
    node.classList.add('ansi-italic');
  }
  if (style.underline) {
    node.classList.add('ansi-underline');
  }
}

function appendJsonHighlightedText(parent: Node, text: string, options: RenderLogLineOptions): void {
  const fragments = findJsonFragments(text);
  if (fragments.length === 0) {
    parent.appendChild(document.createTextNode(text));
    return;
  }

  let cursor = 0;
  for (const fragment of fragments) {
    if (fragment.start > cursor) {
      parent.appendChild(document.createTextNode(text.slice(cursor, fragment.start)));
    }
    if (options.prettyJson) {
      appendPrettyJsonBlock(parent, fragment.raw);
    } else {
      appendJsonTokens(parent, fragment.raw);
    }
    cursor = fragment.end;
  }
  if (cursor < text.length) {
    parent.appendChild(document.createTextNode(text.slice(cursor)));
  }
}

function appendPrettyJsonBlock(parent: Node, raw: string): void {
  const node = document.createElement('span');
  node.className = 'json-pretty-block';
  try {
    appendJsonTokens(node, JSON.stringify(JSON.parse(raw), null, 2));
  } catch {
    appendJsonTokens(node, raw);
  }
  parent.appendChild(node);
}

function findJsonFragments(text: string): JsonFragment[] {
  const fragments: JsonFragment[] = [];
  let start = -1;
  let stack: string[] = [];
  let inString = false;
  let escaped = false;

  for (let index = 0; index < text.length; index += 1) {
    const ch = text[index] ?? '';
    if (start < 0) {
      if (ch === '{' || ch === '[') {
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
      if (ch === '\\') {
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
    if (ch === '{' || ch === '[') {
      stack.push(ch);
      continue;
    }
    if (ch !== '}' && ch !== ']') {
      continue;
    }

    const opener = stack[stack.length - 1];
    if ((opener === '{' && ch !== '}') || (opener === '[' && ch !== ']')) {
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

function isValidJson(raw: string): boolean {
  try {
    JSON.parse(raw);
    return true;
  } catch {
    return false;
  }
}

function appendJsonTokens(parent: Node, raw: string): void {
  let index = 0;
  while (index < raw.length) {
    const ch = raw[index] ?? '';
    if (isWhitespace(ch)) {
      const next = scanWhile(raw, index, isWhitespace);
      parent.appendChild(document.createTextNode(raw.slice(index, next)));
      index = next;
      continue;
    }
    if (ch === '"') {
      const end = scanStringEnd(raw, index);
      const after = skipWhitespace(raw, end);
      appendToken(parent, raw.slice(index, end), raw[after] === ':' ? 'key' : 'string');
      index = end;
      continue;
    }
    if (isNumberStart(ch)) {
      const end = scanJsonNumberEnd(raw, index);
      appendToken(parent, raw.slice(index, end), 'number');
      index = end;
      continue;
    }
    if (raw.startsWith('true', index)) {
      appendToken(parent, 'true', 'boolean');
      index += 4;
      continue;
    }
    if (raw.startsWith('false', index)) {
      appendToken(parent, 'false', 'boolean');
      index += 5;
      continue;
    }
    if (raw.startsWith('null', index)) {
      appendToken(parent, 'null', 'null');
      index += 4;
      continue;
    }
    appendToken(parent, ch, 'punctuation');
    index += 1;
  }
}

function appendToken(parent: Node, text: string, kind: string): void {
  const node = document.createElement('span');
  node.className = `json-token json-token-${kind}`;
  node.textContent = text;
  parent.appendChild(node);
}

function scanStringEnd(raw: string, start: number): number {
  let escaped = false;
  for (let index = start + 1; index < raw.length; index += 1) {
    const ch = raw[index] ?? '';
    if (escaped) {
      escaped = false;
      continue;
    }
    if (ch === '\\') {
      escaped = true;
      continue;
    }
    if (ch === '"') {
      return index + 1;
    }
  }
  return raw.length;
}

function scanJsonNumberEnd(raw: string, start: number): number {
  let index = start;
  while (index < raw.length && /[-+0-9.eE]/.test(raw[index] ?? '')) {
    index += 1;
  }
  return index;
}

function scanWhile(raw: string, start: number, predicate: (ch: string) => boolean): number {
  let index = start;
  while (index < raw.length && predicate(raw[index] ?? '')) {
    index += 1;
  }
  return index;
}

function skipWhitespace(raw: string, start: number): number {
  return scanWhile(raw, start, isWhitespace);
}

function isWhitespace(ch: string): boolean {
  return ch === ' ' || ch === '\t' || ch === '\n' || ch === '\r';
}

function isNumberStart(ch: string): boolean {
  return ch === '-' || (ch >= '0' && ch <= '9');
}
