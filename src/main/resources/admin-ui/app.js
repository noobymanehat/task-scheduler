// Admin UI for the task scheduler. No framework and no build step: it polls the REST API
// every two seconds while the tab is visible and re-renders the visible tab.
//
// Everything that comes from the API (payloads, error messages, names) is inserted with
// textContent, never innerHTML, so job data cannot inject markup or script.
'use strict';

const POLL_MS = 2000;
const TABS = ['jobs', 'workers', 'dlq', 'schedules', 'submit'];
const PAYLOAD_PRESETS = {
  'demo.noop': '{}',
  'demo.sleep': '{"millis": 20000}',
  'demo.flaky': '{"succeedOnAttempt": 3}',
};
const TYPE_HELP = {
  'demo.noop': 'Succeeds immediately.',
  'demo.sleep': 'Sleeps for payload "millis", then succeeds. Long sleeps are good for killing workers mid-job.',
  'demo.flaky': 'Fails until the attempt number reaches "succeedOnAttempt". Without it, fails every time and ends in the DLQ.',
};

const state = {
  tab: 'jobs',
  expanded: new Map(),      // job id -> latest detail (null until loaded)
  workers: new Map(),       // worker id -> worker, to show hostnames in the jobs table
  heartbeatTimeout: null,   // seconds, from /api/v1/stats
  timer: null,
  inFlight: false,
};

// ---- helpers ----

const $ = (selector) => document.querySelector(selector);

/** el('td', {className: 'x'}, 'text', childNode, ...) */
function el(tag, props, ...children) {
  const node = document.createElement(tag);
  if (props) {
    for (const [key, value] of Object.entries(props)) {
      if (key === 'onclick') node.addEventListener('click', value);
      else if (key === 'dataset') Object.assign(node.dataset, value);
      else node[key] = value;
    }
  }
  for (const child of children.flat()) {
    if (child === null || child === undefined) continue;
    node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return node;
}

class ApiError extends Error {}

async function api(method, path, body) {
  const options = { method, headers: { Accept: 'application/json' } };
  if (body !== undefined) {
    options.headers['Content-Type'] = 'application/json';
    options.body = JSON.stringify(body);
  }
  const response = await fetch(path, options);
  const text = await response.text();
  const data = text ? JSON.parse(text) : null;
  if (!response.ok) {
    // RFC 9457 problem detail, possibly with per-field validation errors.
    let message = (data && (data.detail || data.title)) || `${response.status} ${response.statusText}`;
    if (data && data.errors) {
      message += '\n' + Object.entries(data.errors).map(([field, error]) => `${field}: ${error}`).join('\n');
    }
    throw new ApiError(message);
  }
  return data;
}

let toastTimer;
function toast(message, isError = false) {
  const box = $('#toast');
  box.textContent = message;
  box.className = isError ? 'toast error' : 'toast';
  box.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { box.hidden = true; }, isError ? 8000 : 3000);
}

/** Runs an action from a button, reports the outcome, and refreshes. */
async function act(button, action, successMessage) {
  button.disabled = true;
  try {
    await action();
    if (successMessage) toast(successMessage);
  } catch (e) {
    toast(e.message, true);
  } finally {
    button.disabled = false;
    refresh();
  }
}

const shortId = (id) => (id ? id.slice(0, 8) : '');

function formatTime(iso) {
  if (!iso) return '–';
  const date = new Date(iso);
  const sameDay = date.toDateString() === new Date().toDateString();
  return sameDay ? date.toLocaleTimeString() : date.toLocaleString();
}

function formatSeconds(seconds) {
  if (seconds === null || seconds === undefined) return '–';
  if (seconds < 10) return `${seconds.toFixed(1)} s`;
  if (seconds < 60) return `${Math.round(seconds)} s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ${String(Math.round(seconds % 60)).padStart(2, '0')}s`;
  return `${Math.floor(minutes / 60)}h ${String(minutes % 60).padStart(2, '0')}m`;
}

const badge = (text, extraClass) => el('span', { className: `badge ${extraClass || text}` }, text);

function emptyRow(columns, text) {
  return el('tr', null, el('td', { className: 'empty', colSpan: columns }, text));
}

function workerLabel(workerId) {
  if (!workerId) return '–';
  const worker = state.workers.get(workerId);
  return worker ? `${worker.hostname} (${shortId(workerId)})` : shortId(workerId);
}

// ---- header stats ----

function renderStats(stats) {
  state.heartbeatTimeout = stats.heartbeatTimeoutSeconds;
  for (const node of document.querySelectorAll('[data-stat]')) {
    const value = stats[node.dataset.stat];
    node.textContent = node.dataset.stat === 'oldestReadySeconds' ? formatSeconds(value) : String(value);
  }
}

// ---- jobs ----

async function loadWorkers() {
  const workers = await api('GET', '/api/v1/workers');
  state.workers = new Map(workers.map((w) => [w.id, w]));
  return workers;
}

async function refreshJobs() {
  const params = new URLSearchParams({ limit: '50' });
  const status = $('#job-status').value;
  const type = $('#job-type').value.trim();
  if (status) params.set('status', status);
  if (type) params.set('type', type);
  const [page] = await Promise.all([api('GET', `/api/v1/jobs?${params}`), loadWorkers()]);
  await Promise.all([...state.expanded.keys()].map(async (id) => {
    try {
      state.expanded.set(id, await api('GET', `/api/v1/jobs/${id}`));
    } catch {
      state.expanded.delete(id);
    }
  }));
  // An opened job stays visible even after newer jobs push it off the first page.
  const listed = new Set(page.items.map((job) => job.id));
  const pinned = [...state.expanded.values()].filter((detail) => detail && !listed.has(detail.id));
  renderJobs([...pinned, ...page.items]);
}

function renderJobs(jobs) {
  const body = $('#jobs-body');
  const rows = [];
  if (jobs.length === 0) rows.push(emptyRow(8, 'No jobs match.'));
  for (const job of jobs) {
    const expanded = state.expanded.has(job.id);
    const cancel = job.status === 'PENDING'
      ? el('button', {
          className: 'small danger',
          onclick: (event) => {
            event.stopPropagation();
            act(event.currentTarget, () => api('POST', `/api/v1/jobs/${job.id}/cancel`), `Cancelled ${shortId(job.id)}`);
          },
        }, 'Cancel')
      : null;
    rows.push(el('tr', {
      className: expanded ? 'clickable expanded' : 'clickable',
      title: 'Show attempt history',
      onclick: () => toggleJob(job.id),
    },
      el('td', { className: 'mono' }, shortId(job.id)),
      el('td', null, job.type),
      el('td', null, badge(job.status)),
      el('td', { className: 'num', title: 'Claims so far, and the most that count against the retry budget. Excused attempts (released at shutdown or before a requeue) do not count.' },
        `${job.attempts}`, job.attemptsExcused ? ` (${job.attemptsExcused} excused)` : '', ` / max ${job.maxRetries + 1}`),
      el('td', { className: 'mono' }, workerLabel(job.lockedBy)),
      el('td', null, formatTime(job.runAt)),
      el('td', null, formatTime(job.updatedAt)),
      el('td', { className: 'actions' }, cancel),
    ));
    if (expanded) rows.push(renderJobDetail(job, state.expanded.get(job.id)));
  }
  body.replaceChildren(...rows);
}

function renderJobDetail(job, detail) {
  const cell = el('td', { colSpan: 8 });
  if (!detail) {
    cell.append('Loading…');
  } else {
    cell.append(
      el('div', { className: 'mono payload' }, `id ${detail.id}   payload ${JSON.stringify(detail.payload)}`),
      detail.lastError ? el('div', { className: 'mono payload' }, `last error: ${detail.lastError}`) : '',
    );
    const attempts = detail.attemptHistory.map((a) => el('tr', null,
      el('td', { className: 'num' }, `#${a.attempt}`),
      el('td', { className: 'mono' }, workerLabel(a.workerId)),
      el('td', null, a.outcome ? badge(a.outcome) : badge('RUNNING')),
      el('td', null, formatTime(a.startedAt)),
      el('td', null, formatTime(a.finishedAt)),
      el('td', { className: 'error' }, a.error || ''),
    ));
    cell.append(el('table', null,
      el('thead', null, el('tr', null,
        ['Attempt', 'Worker', 'Outcome', 'Started', 'Finished', 'Error'].map((h) => el('th', null, h)))),
      el('tbody', null, attempts.length ? attempts : emptyRow(6, 'Not claimed yet.')),
    ));
  }
  return el('tr', { className: 'detail' }, cell);
}

function toggleJob(id) {
  if (state.expanded.has(id)) state.expanded.delete(id);
  else state.expanded.set(id, null);
  refresh();
}

// ---- workers ----

async function refreshWorkers() {
  const workers = await loadWorkers();
  const aliveOnly = $('#workers-alive-only').checked;
  renderWorkers(aliveOnly ? workers.filter((w) => w.status === 'ALIVE') : workers);
}

function heartbeatClass(worker) {
  const timeout = state.heartbeatTimeout;
  if (worker.status !== 'ALIVE' || !timeout) return 'age';
  if (worker.heartbeatAgeSeconds >= timeout) return 'age expired';
  if (worker.heartbeatAgeSeconds >= timeout / 3) return 'age late';
  return 'age';
}

function renderWorkers(workers) {
  const timeout = state.heartbeatTimeout;
  $('#workers-hint').textContent = timeout
    ? `A worker is declared dead ${formatSeconds(timeout)} after its last heartbeat; its running jobs then move to other workers.`
    : '';
  const rows = workers.map((w) => {
    const age = w.status === 'ALIVE'
      ? formatSeconds(w.heartbeatAgeSeconds)
      : `last seen ${formatSeconds(w.heartbeatAgeSeconds)} ago`;
    const overdue = w.status === 'ALIVE' && timeout && w.heartbeatAgeSeconds >= timeout;
    return el('tr', null,
      el('td', { className: 'mono' }, w.hostname),
      el('td', { className: 'mono' }, shortId(w.id)),
      el('td', null, badge(w.status)),
      el('td', { className: heartbeatClass(w) }, age, overdue ? ' (overdue: about to be reaped)' : ''),
      el('td', { className: 'num' }, String(w.runningJobs)),
      el('td', null, formatTime(w.startedAt)),
    );
  });
  $('#workers-body').replaceChildren(...(rows.length ? rows : [emptyRow(6, 'No workers.')]));
}

// ---- dead-letter queue ----

async function refreshDlq() {
  const page = await api('GET', '/api/v1/dlq?limit=50');
  const rows = page.items.map((job) => el('tr', null,
    el('td', { className: 'mono' }, shortId(job.id)),
    el('td', null, job.type),
    el('td', { className: 'num' }, String(job.attempts)),
    el('td', { className: 'error' }, job.lastError || ''),
    el('td', null, formatTime(job.finishedAt)),
    el('td', { className: 'actions' }, el('button', {
      className: 'small',
      onclick: (event) => act(event.currentTarget,
        () => api('POST', `/api/v1/dlq/${job.id}/requeue`), `Requeued ${shortId(job.id)}`),
    }, 'Requeue')),
  ));
  $('#dlq-body').replaceChildren(...(rows.length ? rows : [emptyRow(6, 'The dead-letter queue is empty.')]));
}

// ---- schedules ----

async function refreshSchedules() {
  const schedules = await api('GET', '/api/v1/schedules');
  const rows = schedules.map((s) => {
    const path = `/api/v1/schedules/${s.id}`;
    return el('tr', null,
      el('td', null, s.name),
      el('td', { className: 'mono' }, s.cron),
      el('td', null, s.timezone),
      el('td', null, s.jobType),
      el('td', null, s.enabled ? badge('enabled') : badge('paused', 'warn')),
      el('td', null, s.enabled ? formatTime(s.nextFireAt) : '–'),
      el('td', null, formatTime(s.lastFiredAt)),
      el('td', { className: 'actions' },
        el('button', {
          className: 'small secondary',
          onclick: (event) => act(event.currentTarget,
            () => api('POST', `${path}/${s.enabled ? 'pause' : 'resume'}`),
            `${s.enabled ? 'Paused' : 'Resumed'} ${s.name}`),
        }, s.enabled ? 'Pause' : 'Resume'),
        el('button', {
          className: 'small danger',
          onclick: (event) => {
            if (confirm(`Delete schedule "${s.name}"? Jobs it already created are kept.`)) {
              act(event.currentTarget, () => api('DELETE', path), `Deleted ${s.name}`);
            }
          },
        }, 'Delete'),
      ),
    );
  });
  $('#schedules-body').replaceChildren(...(rows.length ? rows : [emptyRow(8, 'No schedules yet.')]));
}

// ---- forms ----

function parsePayload(text) {
  try {
    const value = JSON.parse(text.trim() || '{}');
    if (typeof value !== 'object' || value === null || Array.isArray(value)) throw new Error();
    return value;
  } catch {
    throw new ApiError('Payload must be a JSON object, e.g. {"millis": 5000}');
  }
}

const numberOrUndefined = (value) => (value === '' ? undefined : Number(value));

function setupSubmitForm() {
  const form = $('#submit-form');
  const showHelp = () => {
    const type = form.type.value.trim();
    $('#type-help').textContent = TYPE_HELP[type] || 'Any type with a registered handler; unknown types go straight to the DLQ.';
  };
  form.type.addEventListener('input', () => {
    const preset = PAYLOAD_PRESETS[form.type.value.trim()];
    // Replace the payload only if the user has not written their own.
    if (preset && Object.values(PAYLOAD_PRESETS).includes(form.payload.value.trim())) form.payload.value = preset;
    showHelp();
  });
  showHelp();

  form.addEventListener('submit', (event) => {
    event.preventDefault();
    const button = form.querySelector('button');
    act(button, async () => {
      const count = Math.max(1, Math.min(500, Number(form.count.value) || 1));
      const key = form.idempotencyKey.value.trim();
      if (key && count > 1) throw new ApiError('An idempotency key identifies one job: submit 1, or clear the key.');
      const body = {
        type: form.type.value.trim(),
        payload: parsePayload(form.payload.value),
        priority: numberOrUndefined(form.priority.value),
        maxRetries: numberOrUndefined(form.maxRetries.value),
        timeoutSeconds: numberOrUndefined(form.timeoutSeconds.value),
        idempotencyKey: key || undefined,
      };
      // At most 8 requests in flight, so a batch of 500 does not flood the API.
      let next = 0;
      const submitted = [];
      await Promise.all(Array.from({ length: Math.min(8, count) }, async () => {
        while (next < count) {
          next++;
          submitted.push(await api('POST', '/api/v1/jobs', body));
        }
      }));
      toast(count === 1 ? `Submitted job ${shortId(submitted[0].id)}` : `Submitted ${count} jobs`);
      location.hash = '#jobs';
    });
  });
}

function setupScheduleForm() {
  const form = $('#schedule-form');
  form.addEventListener('submit', (event) => {
    event.preventDefault();
    const button = form.querySelector('button');
    act(button, async () => {
      const created = await api('POST', '/api/v1/schedules', {
        name: form.name.value.trim(),
        cron: form.cron.value.trim(),
        timezone: form.timezone.value.trim() || undefined,
        jobType: form.jobType.value.trim(),
        payload: parsePayload(form.payload.value),
        priority: numberOrUndefined(form.priority.value),
        maxRetries: numberOrUndefined(form.maxRetries.value),
      });
      form.name.value = '';
      toast(`Created schedule ${created.name}; next fire ${formatTime(created.nextFireAt)}`);
    });
  });
}

// ---- tabs and polling ----

/** Hash routes: #jobs, #workers, ... and #jobs/<id>, which opens that job's history. */
function showTab() {
  const [requested, jobId] = location.hash.slice(1).split('/');
  state.tab = TABS.includes(requested) ? requested : 'jobs';
  if (state.tab === 'jobs' && jobId && !state.expanded.has(jobId)) state.expanded.set(jobId, null);
  for (const tab of TABS) {
    $(`#tab-${tab}`).hidden = tab !== state.tab;
    $(`[data-tab="${tab}"]`).classList.toggle('active', tab === state.tab);
  }
  refresh();
}

const REFRESHERS = {
  jobs: refreshJobs,
  workers: refreshWorkers,
  dlq: refreshDlq,
  schedules: refreshSchedules,
  submit: async () => {},
};

/** One poll: header stats plus the visible tab. Never overlaps itself. */
async function refresh() {
  clearTimeout(state.timer);
  if (state.inFlight) {
    state.timer = setTimeout(refresh, 100);
    return;
  }
  state.inFlight = true;
  const live = $('#live');
  try {
    const [stats] = await Promise.all([api('GET', '/api/v1/stats'), REFRESHERS[state.tab]()]);
    renderStats(stats);
    live.className = 'live on';
    live.textContent = `live · updated ${new Date().toLocaleTimeString()}`;
  } catch (e) {
    live.className = 'live off';
    live.textContent = `cannot reach the API: ${e.message}`;
  } finally {
    state.inFlight = false;
    if (!document.hidden) state.timer = setTimeout(refresh, POLL_MS);
  }
}

document.addEventListener('DOMContentLoaded', () => {
  setupSubmitForm();
  setupScheduleForm();
  let typeDebounce;
  $('#job-status').addEventListener('change', refresh);
  $('#job-type').addEventListener('input', () => {
    clearTimeout(typeDebounce);
    typeDebounce = setTimeout(refresh, 300);
  });
  $('#workers-alive-only').addEventListener('change', refresh);
  window.addEventListener('hashchange', showTab);
  // Stop polling in background tabs; catch up immediately when the tab is shown again.
  document.addEventListener('visibilitychange', () => {
    if (document.hidden) clearTimeout(state.timer);
    else refresh();
  });
  showTab();
});
