(async () => {
  const {html, css, script} = window.__paneLayoutSource;
  delete window.__paneLayoutSource;
  const frame = document.createElement('iframe');
  frame.style.cssText = 'position:fixed;left:-10000px;top:0;width:521px;height:946px;border:0;';
  document.body.appendChild(frame);
  const saved = new Map();
  const key = 'fws.log.panes.v1.__layout_probe__';
  for (const name of [key, 'fws.log.wrap', 'fws.log.panes.hint']) saved.set(name, localStorage.getItem(name));
  try {
    localStorage.removeItem(key);
    localStorage.removeItem('fws.log.wrap');
    localStorage.setItem('fws.log.panes.hint', 'seen');
    const doc = frame.contentDocument;
    doc.open();
    doc.write(html.replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, '').replace(/<link\b[^>]*>/gi, ''));
    doc.close();
    const style = doc.createElement('style'); style.textContent = css; doc.head.append(style);
    const preview = frame.contentWindow.eval(script + '\n;PaneLayoutPreview;');
    const drawer = doc.querySelector('.log-drawer');
    drawer.classList.add('is-open');
    const binding = preview.bindLogPaneLayout(drawer, doc.querySelector('#fws-stdin-form'));
    for (const header of doc.querySelectorAll('.log-pane-header')) {
      const nav = doc.createElement('div');
      nav.className = 'log-window-controls';
      nav.innerHTML = '<span class="log-window-info">Records 1000000-1000200 of 1000300</span><div class="log-window-actions"><button class="btn btn-small">Older</button><button class="btn btn-small">Newer</button><button class="btn btn-small">Jump to live</button></div>';
      header.append(nav);
    }
    binding.open('__layout_probe__');
    const wait = () => Promise.resolve();
    const height = id => doc.querySelector('[data-log-pane="' + id + '"]')?.getBoundingClientRect().height;
    const out = {};
    await wait();
    out.initial = {stdinAbsent: !doc.querySelector('#fws-stdin-form'), stdout:height('stdout'), stderr:height('stderr'), stderrHidden:doc.querySelector('#stderr-container').getBoundingClientRect().height === 0};
    doc.querySelector('#fws-stderr-pane-toggle').click();
    await wait();
    out.two = {stdout:height('stdout'), stderr:height('stderr'), handles:doc.querySelectorAll('.log-pane-splitter').length};
    doc.querySelector('.log-pane-splitter').dispatchEvent(new frame.contentWindow.KeyboardEvent('keydown', {key:'ArrowDown', bubbles:true}));
    await wait();
    out.resized = {stdout:height('stdout'), stderr:height('stderr'), saved:JSON.parse(localStorage.getItem(key))};
    binding.open('__layout_probe__');
    await wait();
    out.restored = {stdout:height('stdout'), stderr:height('stderr')};
    binding.setStdinAvailable(true);
    doc.querySelector('#fws-stdin-pane-toggle').click();
    await wait();
    out.three = {stdin:height('stdin'), stdout:height('stdout'), stderr:height('stderr'), handles:doc.querySelectorAll('.log-pane-splitter').length};
    doc.querySelector('#stdout-pretty-json').click();
    out.controlDoesNotCollapse = doc.querySelector('#fws-stdout-pane-toggle').getAttribute('aria-expanded');
    doc.querySelector('#fws-log-wrap').click();
    const line = doc.createElement('div'); line.className='log-lines';
    const record = doc.createElement('div'); record.className='log-line log-projected-record'; record.textContent='A sample record\n'.repeat(100); line.append(record); doc.querySelector('#stdout-container').append(line);
    await wait();
    out.record = {maxHeight:frame.contentWindow.getComputedStyle(record).maxHeight,height:record.getBoundingClientRect().height,whiteSpace:frame.contentWindow.getComputedStyle(line).whiteSpace};
    doc.querySelector('#fws-log-wrap').click();
    out.wrapRestored = frame.contentWindow.getComputedStyle(line).whiteSpace === 'pre-wrap' && doc.querySelector('#fws-stdin-input').getAttribute('wrap') === 'soft';
    if (!out.wrapRestored) throw new Error('Wrap did not apply to both output and input');
    binding.setStdinAvailable(false);
    out.stdinRemoved = !doc.querySelector('#fws-stdin-form');
    frame.style.width='360px';
    await wait();
    const nav = doc.querySelector('.log-window-controls').getBoundingClientRect();
    const info = doc.querySelector('.log-window-info').getBoundingClientRect();
    const actions = doc.querySelector('.log-window-actions').getBoundingClientRect();
    out.narrow = {width:doc.documentElement.clientWidth,scrollWidth:doc.documentElement.scrollWidth,stdout:height('stdout'),stderr:height('stderr'), navWidth:nav.width, infoBelowActions:info.top >= actions.bottom, actionsInside:actions.right <= nav.right};
    if (out.narrow.scrollWidth !== out.narrow.width || !out.narrow.infoBelowActions || !out.narrow.actionsInside) throw new Error(JSON.stringify(out));
    if (!out.initial.stdinAbsent || !out.initial.stderrHidden || Math.abs(out.two.stdout-out.two.stderr)>2 || out.two.handles!==1 || out.resized.stdout<=out.two.stdout || Math.abs(out.restored.stdout-out.resized.stdout)>2 || out.three.handles!==2 || Math.max(out.three.stdin,out.three.stdout,out.three.stderr)-Math.min(out.three.stdin,out.three.stdout,out.three.stderr)>2 || out.controlDoesNotCollapse!=='true' || out.record.maxHeight!=='none' || out.record.height<300 || out.record.whiteSpace!=='pre' || !out.stdinRemoved) throw new Error(JSON.stringify(out));
    return out;
  } finally {
    frame.remove();
    for (const [name,value] of saved) { if(value===null)localStorage.removeItem(name); else localStorage.setItem(name,value); }
  }
})()
