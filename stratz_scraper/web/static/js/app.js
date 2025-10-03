const state = {
  running: false,
  maxBackoff: 86_400_000,
  tokenCounter: 0,
  tokens: [],
};

const TOKEN_LOG_MAX_ENTRIES = 200;
const DAY_IN_MS = 86_400_000;
const NO_TASK_RETRY_DELAY_MS = 100;

const elements = {
  tokenList: document.getElementById("tokenList"),
  addToken: document.getElementById("addToken"),
  exportTokens: document.getElementById("exportTokens"),
  importTokens: document.getElementById("importTokens"),
  importTokensFile: document.getElementById("importTokensFile"),
  toggleTokens: document.getElementById("toggleTokens"),
  begin: document.getElementById("begin"),
  stop: document.getElementById("stop"),
  progress: document.getElementById("progress"),
  best: document.getElementById("best"),
  tokenSummary: document.getElementById("tokenSummary"),
  seedBtn: document.getElementById("seedBtn"),
  seedStart: document.getElementById("seedStart"),
  seedEnd: document.getElementById("seedEnd"),
  log: document.getElementById("log"),
  bestTable: document.getElementById("bestTable"),
  progressText: document.getElementById("progressText"),
  backoffText: document.getElementById("backoffText"),
  requestsRemaining: document.getElementById("requestsRemaining"),
  avgTaskTimeGlobal: document.getElementById("avgTaskTimeGlobal"),
  tasksPerDayGlobal: document.getElementById("tasksPerDayGlobal"),
};

function getCookie(name) {
  const cname = `${name}=`;
  return document.cookie
    .split(";")
    .map((c) => c.trim())
    .find((c) => c.startsWith(cname))
    ?.slice(cname.length);
}

function clearCookie(name) {
  document.cookie = `${name}=; expires=Thu, 01 Jan 1970 00:00:00 GMT; path=/`;
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseRetryAfterHeader(value) {
  if (typeof value !== "string") {
    return null;
  }

  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }

  const seconds = Number.parseFloat(trimmed);
  if (Number.isFinite(seconds) && seconds >= 0) {
    return Math.ceil(seconds * 1000);
  }

  const retryDateMs = Date.parse(trimmed);
  if (Number.isFinite(retryDateMs)) {
    const delta = retryDateMs - Date.now();
    if (delta > 0) {
      return Math.ceil(delta);
    }
  }

  return null;
}

function getRetryAfterMsFromError(error) {
  if (!error) {
    return null;
  }

  const value = error.retryAfterMs;
  if (!Number.isFinite(value)) {
    return null;
  }

  const clamped = Math.max(0, Math.ceil(value));
  return Number.isFinite(clamped) && clamped > 0 ? clamped : null;
}

const HTML_ESCAPE_MAP = {
  "&": "&amp;",
  "<": "&lt;",
  ">": "&gt;",
  '"': "&quot;",
  "'": "&#39;",
  "`": "&#96;",
};

function escapeHtml(value) {
  if (typeof value !== "string" || value.length === 0) {
    return "";
  }
  return value.replace(/[&<>"'`]/g, (char) => HTML_ESCAPE_MAP[char] ?? char);
}

function safeText(value) {
  if (value === null || value === undefined) {
    return "";
  }
  return escapeHtml(String(value));
}

function formatCell(value, fallback = "—") {
  const safe = safeText(value);
  return safe || fallback;
}

function normalizeSteamAccountId(value) {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value) || value <= 0) {
      return null;
    }
    return Math.trunc(value).toString();
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) {
      return null;
    }
    if (/^\d+$/.test(trimmed)) {
      return trimmed;
    }
    const parsed = Number(trimmed);
    if (Number.isFinite(parsed) && parsed > 0) {
      return Math.trunc(parsed).toString();
    }
  }
  return null;
}

function formatWinRate(wins, matches) {
  const winsNumber = Number(wins);
  const matchesNumber = Number(matches);
  if (!Number.isFinite(winsNumber) || !Number.isFinite(matchesNumber) || matchesNumber <= 0) {
    return "—";
  }
  const clampedWins = Math.max(0, Math.min(winsNumber, matchesNumber));
  const rate = (clampedWins / matchesNumber) * 100;
  if (!Number.isFinite(rate)) {
    return "—";
  }
  return `${rate.toFixed(1)}%`;
}

function appendLogLine(element, line, {
  maxLength = 50_000,
  retainLength = 40_000,
} = {}) {
  if (!element) return;

  element.textContent += `${line}\n`;
  if (element.textContent.length > maxLength) {
    element.textContent = element.textContent.slice(-retainLength);
  }
  element.scrollTop = element.scrollHeight;
}

function log(message) {
  const timestamp = new Date().toLocaleTimeString();
  appendLogLine(elements.log, `[${timestamp}] ${message}`);
}

function formatDuration(ms) {
  if (!Number.isFinite(ms) || ms <= 0) return "—";
  if (ms < 1000) return `${ms} ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)} s`;
  const minutes = Math.round(ms / 60_000);
  return `${minutes} min`;
}

function getNowMs() {
  return Date.now();
}

function getTokenRuntimeMs(token) {
  if (!token) {
    return 0;
  }
  const total = Number.isFinite(token.totalRuntimeMs) ? token.totalRuntimeMs : 0;
  const active =
    token.running && typeof token.lastStartMs === "number"
      ? Math.max(0, getNowMs() - token.lastStartMs)
      : 0;
  return total + active;
}

function getTokenAverageTaskMs(token) {
  if (!token) {
    return NaN;
  }
  const completed = Number.isFinite(token.completedTasks) ? token.completedTasks : 0;
  if (completed <= 0) {
    return NaN;
  }
  const runtime = getTokenRuntimeMs(token);
  if (runtime <= 0) {
    return NaN;
  }
  return runtime / completed;
}

function formatAverageTaskTime(avgMs) {
  if (!Number.isFinite(avgMs) || avgMs <= 0) {
    return "—";
  }
  return formatDuration(Math.round(avgMs));
}

function getTokenTasksPerDay(token) {
  const averageMs = getTokenAverageTaskMs(token);
  if (!Number.isFinite(averageMs) || averageMs <= 0) {
    return NaN;
  }
  const tasksPerDay = DAY_IN_MS / averageMs;
  if (!Number.isFinite(tasksPerDay) || tasksPerDay <= 0) {
    return NaN;
  }
  return tasksPerDay;
}

function formatTasksPerDay(tasksPerDay) {
  if (!Number.isFinite(tasksPerDay) || tasksPerDay <= 0) {
    return "—";
  }
  if (tasksPerDay >= 100) {
    return `~${Math.round(tasksPerDay).toLocaleString()}`;
  }
  if (tasksPerDay >= 10) {
    return `~${tasksPerDay.toFixed(1)}`;
  }
  return `~${tasksPerDay.toFixed(2)}`;
}

function formatTokenLabel(token) {
  if (!token) {
    return "Token";
  }
  const index = Number.isFinite(token.displayIndex) && token.displayIndex > 0
    ? token.displayIndex
    : state.tokens.indexOf(token) + 1;
  const prefix = index > 0 ? `Token ${index}` : "Token";
  const rawValue = typeof token.value === "string" ? token.value.trim() : "";
  if (!rawValue) {
    return prefix;
  }
  const compact = rawValue.replace(/\s+/g, "");
  if (compact.length <= 8) {
    return `${prefix} • ${compact}`;
  }
  const head = compact.slice(0, 4);
  const tail = compact.slice(-4);
  return `${prefix} • ${head}…${tail}`;
}

function formatTokenSummaryMeta(token) {
  if (!token) {
    return "";
  }
  const parts = [];
  if (typeof token.requestsRemaining === "number" && Number.isFinite(token.requestsRemaining)) {
    const remaining = Math.max(0, token.requestsRemaining);
    const label = remaining === 1 ? "request" : "requests";
    parts.push(`${remaining.toLocaleString()} ${label} left`);
  } else {
    parts.push("∞ requests left");
  }

  const completed = Number.isFinite(token.completedTasks) ? token.completedTasks : 0;
  parts.push(`${completed.toLocaleString()} done`);

  const averageText = formatAverageTaskTime(getTokenAverageTaskMs(token));
  if (averageText !== "—") {
    parts.push(`${averageText}/task`);
  }

  return parts.join(" • ");
}

function updateTokenSummary() {
  if (!elements.tokenSummary) {
    return;
  }
  const total = state.tokens.length;
  if (total === 0) {
    elements.tokenSummary.textContent = "No tokens configured.";
    return;
  }

  const running = state.tokens.filter((token) => token.running && !token.stopRequested).length;
  const stopping = state.tokens.filter((token) => token.running && token.stopRequested).length;
  const idle = total - running - stopping;
  const pieces = [];
  pieces.push(`${total} ${total === 1 ? "token" : "tokens"}`);
  if (running) {
    pieces.push(`${running} running`);
  }
  if (stopping) {
    pieces.push(`${stopping} stopping`);
  }
  if (idle) {
    pieces.push(`${idle} idle`);
  }
  if (pieces.length === 1) {
    pieces.push("All idle");
  }
  elements.tokenSummary.textContent = pieces.join(" · ");
}

function updateGlobalMetrics() {
  if (!elements.avgTaskTimeGlobal && !elements.tasksPerDayGlobal) {
    return;
  }

  let totalRuntime = 0;
  let totalTasks = 0;
  let totalTasksPerDay = 0;
  let hasTasksPerDay = false;

  state.tokens.forEach((token) => {
    const completed = Number.isFinite(token?.completedTasks) ? token.completedTasks : 0;
    if (!completed) {
      return;
    }
    const runtime = getTokenRuntimeMs(token);
    if (runtime <= 0) {
      return;
    }

    totalRuntime += runtime;
    totalTasks += completed;

    const tasksPerDay = getTokenTasksPerDay(token);
    if (Number.isFinite(tasksPerDay) && tasksPerDay > 0) {
      totalTasksPerDay += tasksPerDay;
      hasTasksPerDay = true;
    }
  });

  const averageMs = totalTasks > 0 ? totalRuntime / totalTasks : NaN;
  const expectedTasksPerDay = hasTasksPerDay ? totalTasksPerDay : NaN;

  if (elements.avgTaskTimeGlobal) {
    elements.avgTaskTimeGlobal.textContent = formatAverageTaskTime(averageMs);
  }

  if (elements.tasksPerDayGlobal) {
    elements.tasksPerDayGlobal.textContent = formatTasksPerDay(expectedTasksPerDay);
  }
}

function updateBackoffDisplay() {
  const runningTokens = state.tokens.filter((token) => token.running);
  if (!runningTokens.length) {
    elements.backoffText.textContent = "—";
    return;
  }
  const minBackoff = Math.min(...runningTokens.map((token) => token.backoff));
  elements.backoffText.textContent = formatDuration(minBackoff);
}

function updateRequestsRemainingDisplay() {
  if (!elements.requestsRemaining) return;
  const runningTokens = state.tokens.filter((token) => token.running);
  if (!runningTokens.length) {
    elements.requestsRemaining.textContent = "—";
    return;
  }

  if (runningTokens.some((token) => token.requestsRemaining === null)) {
    elements.requestsRemaining.textContent = "∞";
    return;
  }

  const total = runningTokens.reduce(
    (sum, token) => sum + (token.requestsRemaining ?? 0),
    0,
  );
  elements.requestsRemaining.textContent = total;
}

function setAllTokensExpanded(expanded) {
  state.tokens.forEach((token) => {
    token.expanded = expanded;
    if (token.dom?.row) {
      token.dom.row.open = expanded;
    }
  });
  updateCollapseAllButton();
}

function updateCollapseAllButton() {
  if (!elements.toggleTokens) {
    return;
  }
  const total = state.tokens.length;
  elements.toggleTokens.disabled = total === 0;
  if (total === 0) {
    elements.toggleTokens.textContent = "Collapse all";
    return;
  }

  const allCollapsed = state.tokens.every((token) => token.expanded === false);
  elements.toggleTokens.textContent = allCollapsed ? "Expand all" : "Collapse all";
}

function updateButtons() {
  const hasReadyToken = state.tokens.some(
    (token) => token.value.trim().length > 0 && !token.running && !token.stopRequested,
  );
  elements.begin.disabled = !hasReadyToken;
  elements.stop.disabled = !state.running;
}

function parseMaxRequests(value) {
  const max = parseInt(value, 10);
  return Number.isFinite(max) && max > 0 ? max : null;
}

async function executeStratzQuery(query, variables, token) {
  const activeToken = typeof token === "string" ? token.trim() : "";
  if (!activeToken) {
    throw new Error("Stratz token is not set");
  }

  const response = await fetch("https://api.stratz.com/graphql", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${activeToken}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ query, variables }),
  });

  if (!response.ok) {
    const error = new Error(`Stratz API returned ${response.status}`);
    if (response.status === 429) {
      const retryAfterHeader = response.headers.get("retry-after");
      const retryAfterMs = parseRetryAfterHeader(retryAfterHeader);
      if (retryAfterMs !== null) {
        error.retryAfterMs = retryAfterMs;
      }
    }
    throw error;
  }

  const payload = await response.json();

  if (payload && Array.isArray(payload.errors) && payload.errors.length > 0) {
    const message = payload.errors
      .map((error) => (typeof error?.message === "string" ? error.message : null))
      .find((msg) => msg);
    throw new Error(message ?? "Stratz API returned errors");
  }

  return payload;
}

function persistTokens() {
  const payload = getPersistableTokens();

  if (!payload.length) {
    try {
      localStorage.removeItem("stratz_tokens");
    } catch (error) {
      console.warn("Failed to remove saved tokens from localStorage", error);
    }
    clearCookie("stratz_tokens");
    clearCookie("stratz_token");
    return;
  }

  try {
    localStorage.setItem("stratz_tokens", JSON.stringify(payload));
  } catch (error) {
    console.warn("Failed to persist tokens to localStorage", error);
  }
  clearCookie("stratz_tokens");
  clearCookie("stratz_token");
}

function getPersistableTokens() {
  return state.tokens
    .map((token) => ({
      token: token.value.trim(),
      maxRequests: parseMaxRequests(token.maxRequests),
    }))
    .filter((entry) => entry.token.length > 0);
}

function downloadTokens() {
  const payload = getPersistableTokens();
  if (!payload.length) {
    alert("There are no tokens to export.");
    return;
  }

  const blob = new Blob([`${JSON.stringify(payload, null, 2)}\n`], {
    type: "application/json",
  });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  anchor.href = url;
  anchor.download = `stratz-tokens-${timestamp}.json`;
  document.body.appendChild(anchor);
  anchor.click();
  document.body.removeChild(anchor);
  URL.revokeObjectURL(url);
  log(`Exported ${payload.length} token${payload.length === 1 ? "" : "s"}.`);
}

function normaliseImportedTokens(data) {
  if (!Array.isArray(data)) {
    throw new Error("Invalid token export format: expected an array.");
  }
  const normalised = [];
  const seen = new Set();

  data.forEach((entry) => {
    let tokenValue = "";
    let maxRaw = null;

    if (typeof entry === "string") {
      tokenValue = entry;
    } else if (entry && typeof entry === "object") {
      if (typeof entry.token === "string") {
        tokenValue = entry.token;
      } else if (typeof entry.value === "string") {
        tokenValue = entry.value;
      }
      if ("maxRequests" in entry) {
        maxRaw = entry.maxRequests;
      } else if ("maxRequest" in entry) {
        maxRaw = entry.maxRequest;
      } else if ("max" in entry) {
        maxRaw = entry.max;
      }
    }

    const trimmedToken = tokenValue.trim();
    if (!trimmedToken || seen.has(trimmedToken)) {
      return;
    }
    seen.add(trimmedToken);

    let maxValue = "";
    if (typeof maxRaw === "number") {
      if (Number.isFinite(maxRaw) && maxRaw > 0) {
        maxValue = String(Math.floor(maxRaw));
      }
    } else if (typeof maxRaw === "string") {
      const trimmedMax = maxRaw.trim();
      if (trimmedMax) {
        const parsed = parseInt(trimmedMax, 10);
        if (Number.isFinite(parsed) && parsed > 0) {
          maxValue = String(parsed);
        } else {
          maxValue = trimmedMax;
        }
      }
    }

    normalised.push({ value: trimmedToken, maxRequests: maxValue });
  });

  if (!normalised.length) {
    throw new Error("No valid tokens found in import file.");
  }

  return normalised;
}

function replaceTokens(newTokens) {
  state.tokens.splice(0, state.tokens.length);
  state.tokenCounter = 0;
  newTokens.forEach((entry) => {
    addTokenRow(
      { value: entry.value, maxRequests: entry.maxRequests },
      { skipPersist: true, fromStorage: true },
    );
  });
  if (!newTokens.length) {
    renderTokens();
  }
  persistTokens();
  updateRunningState();
  log(`Imported ${newTokens.length} token${newTokens.length === 1 ? "" : "s"}.`);
}

function handleImportFile(file) {
  if (!file) {
    return;
  }
  const reader = new FileReader();
  reader.onload = () => {
    try {
      const text = typeof reader.result === "string" ? reader.result : "";
      const parsed = JSON.parse(text);
      const tokens = normaliseImportedTokens(parsed);
      replaceTokens(tokens);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      alert(`Failed to import tokens: ${message}`);
      log(`Token import failed: ${message}`);
    } finally {
      if (elements.importTokensFile) {
        elements.importTokensFile.value = "";
      }
    }
  };
  reader.onerror = () => {
    alert("Failed to read the selected file.");
    log("Token import failed: unable to read file.");
    if (elements.importTokensFile) {
      elements.importTokensFile.value = "";
    }
  };
  reader.readAsText(file);
}

function getTokenLabel(token) {
  const index = state.tokens.indexOf(token);
  return index >= 0 ? index + 1 : token.id;
}

function logToken(token, message) {
  const timestamp = new Date().toLocaleTimeString();
  const prefix = `Token #${getTokenLabel(token)}: ${message}`;
  appendLogLine(elements.log, `[${timestamp}] ${prefix}`);

  if (!token) {
    return;
  }

  if (!Array.isArray(token.logEntries)) {
    token.logEntries = [];
  }

  const entry = `[${timestamp}] ${message}`;
  token.logEntries.push(entry);
  if (token.logEntries.length > TOKEN_LOG_MAX_ENTRIES) {
    token.logEntries.splice(0, token.logEntries.length - TOKEN_LOG_MAX_ENTRIES);
  }

  if (token.dom?.log) {
    token.dom.log.textContent = token.logEntries.join("\n");
    token.dom.log.scrollTop = token.dom.log.scrollHeight;
  }
}

function updateTokenDisplay(token) {
  if (!token?.dom) return;

  const {
    row,
    tokenInput,
    maxInput,
    startBtn,
    stopBtn,
    removeBtn,
    statusValue,
    backoffValue,
    requestsValue,
    summaryTitle,
    summaryStatus,
    summaryMeta,
  } = token.dom;

  tokenInput.value = token.value;
  tokenInput.disabled = token.running || token.stopRequested;

  maxInput.value = token.maxRequests;
  maxInput.disabled = token.running || token.stopRequested;

  if (summaryTitle) {
    summaryTitle.textContent = formatTokenLabel(token);
  }

  const trimmed = token.value.trim();
  startBtn.disabled = token.running || token.stopRequested || trimmed.length === 0;
  stopBtn.disabled = !token.running || token.stopRequested;
  removeBtn.disabled = token.running || token.stopRequested;

  let status = "Idle";
  if (token.running) {
    status = token.stopRequested ? "Stopping…" : "Running";
  } else if (token.stopRequested) {
    status = "Stopping…";
  }
  statusValue.textContent = status;
  if (summaryStatus) {
    summaryStatus.textContent = status;
  }

  const showBackoff = token.running || token.stopRequested;
  backoffValue.textContent = showBackoff ? formatDuration(token.backoff) : "—";

  if (token.requestsRemaining === null || token.requestsRemaining === undefined) {
    requestsValue.textContent = "∞";
  } else {
    requestsValue.textContent = token.requestsRemaining;
  }
  if (summaryMeta) {
    summaryMeta.textContent = formatTokenSummaryMeta(token);
  }

  const averageMs = getTokenAverageTaskMs(token);
  if (token.dom.avgTaskTimeValue) {
    token.dom.avgTaskTimeValue.textContent = formatAverageTaskTime(averageMs);
  }

  if (token.dom.tasksPerDayValue) {
    const tasksPerDay = getTokenTasksPerDay(token);
    token.dom.tasksPerDayValue.textContent = formatTasksPerDay(tasksPerDay);
  }

  row.classList.toggle("token-row-running", token.running && !token.stopRequested);
  row.classList.toggle("token-row-stopping", token.running && token.stopRequested);
}

function updateRunningState() {
  state.running = state.tokens.some((token) => token.running);
  updateButtons();
  updateBackoffDisplay();
  updateRequestsRemainingDisplay();
  state.tokens.forEach((token) => updateTokenDisplay(token));
  updateGlobalMetrics();
  updateTokenSummary();
  updateCollapseAllButton();
}

function removeToken(id) {
  const index = state.tokens.findIndex((token) => token.id === id);
  if (index === -1) return;
  const [token] = state.tokens.splice(index, 1);
  if (token) {
    token.running = false;
    token.dom = null;
  }
  renderTokens();
  persistTokens();
  updateRunningState();
}

function startToken(token) {
  if (!token || token.running || token.stopRequested) {
    return;
  }
  const trimmed = token.value.trim();
  if (!trimmed.length) {
    return;
  }
  workLoopForToken(token).catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    logToken(token, `Failed to start worker: ${message}`);
  });
}

function requestStopForToken(token, { silent = false } = {}) {
  if (!token || !token.running || token.stopRequested) {
    return;
  }
  token.stopRequested = true;
  token.expanded = true;
  if (token.dom?.row) {
    token.dom.row.open = true;
  }
  updateCollapseAllButton();
  updateTokenDisplay(token);
  updateButtons();
  if (!silent) {
    logToken(token, "Stop requested.");
  }
  updateGlobalMetrics();
  updateTokenSummary();
}

function addTokenRow(initial = {}, options = {}) {
  const initialValue = initial.value ?? initial.token ?? "";
  const rawMax =
    initial.maxRequests === null || initial.maxRequests === undefined
      ? ""
      : String(initial.maxRequests);
  const token = {
    id: `token-${state.tokenCounter++}`,
    value: initialValue,
    maxRequests: rawMax,
    running: false,
    backoff: 1000,
    requestsRemaining: parseMaxRequests(rawMax),
    activeToken: null,
    stopRequested: false,
    dom: null,
    logEntries: [],
    totalRuntimeMs: 0,
    lastStartMs: null,
    completedTasks: 0,
    expanded: Boolean(initial.expanded),
    displayIndex: state.tokens.length + 1,
  };
  state.tokens.push(token);
  if (!options.fromStorage) {
    token.expanded = true;
  }
  renderTokens();
  if (!options.skipPersist) {
    persistTokens();
  }
  updateButtons();
  return token;
}

function renderTokens() {
  if (!elements.tokenList) return;
  elements.tokenList.innerHTML = "";

  if (!state.tokens.length) {
    const message = document.createElement("p");
    message.className = "muted";
    message.textContent = "No tokens configured.";
    elements.tokenList.appendChild(message);
    updateGlobalMetrics();
    updateTokenSummary();
    updateCollapseAllButton();
    return;
  }

  state.tokens.forEach((token, index) => {
    token.displayIndex = index + 1;
    const row = document.createElement("details");
    row.className = "token-row";
    row.dataset.tokenId = token.id;

    if (typeof token.expanded !== "boolean") {
      token.expanded = token.running;
    }
    row.open = Boolean(token.expanded);
    row.addEventListener("toggle", () => {
      token.expanded = row.open;
      updateCollapseAllButton();
    });

    const summary = document.createElement("summary");
    summary.className = "token-summary";

    const caret = document.createElement("span");
    caret.className = "token-summary-caret";

    const summaryContent = document.createElement("div");
    summaryContent.className = "token-summary-content";

    const summaryTitle = document.createElement("span");
    summaryTitle.className = "token-summary-title";
    summaryTitle.textContent = formatTokenLabel(token);

    const summaryStatus = document.createElement("span");
    summaryStatus.className = "token-summary-status";
    summaryStatus.textContent = "Idle";

    const summaryMeta = document.createElement("span");
    summaryMeta.className = "token-summary-meta";
    summaryMeta.textContent = formatTokenSummaryMeta(token);

    summaryContent.append(summaryTitle, summaryStatus, summaryMeta);
    summary.append(caret, summaryContent);

    const body = document.createElement("div");
    body.className = "token-body";

    const topRow = document.createElement("div");
    topRow.className = "token-top";

    const fields = document.createElement("div");
    fields.className = "token-fields";

    const tokenInput = document.createElement("input");
    tokenInput.type = "text";
    tokenInput.placeholder = "Paste Stratz API token";
    tokenInput.autocomplete = "off";
    tokenInput.className = "token-input";
    tokenInput.value = token.value;
    tokenInput.disabled = token.running || token.stopRequested;
    tokenInput.addEventListener("input", () => {
      token.value = tokenInput.value;
      persistTokens();
      updateButtons();
      updateTokenDisplay(token);
    });

    const maxInput = document.createElement("input");
    maxInput.type = "number";
    maxInput.min = "1";
    maxInput.placeholder = "Max requests (optional)";
    maxInput.className = "max-input";
    maxInput.value = token.maxRequests;
    maxInput.disabled = token.running || token.stopRequested;
    maxInput.addEventListener("input", () => {
      token.maxRequests = maxInput.value;
      if (!token.running) {
        token.requestsRemaining = parseMaxRequests(maxInput.value);
        updateRequestsRemainingDisplay();
      }
      persistTokens();
      updateTokenDisplay(token);
    });

    fields.append(tokenInput, maxInput);

    const actions = document.createElement("div");
    actions.className = "token-actions";

    const startBtn = document.createElement("button");
    startBtn.type = "button";
    startBtn.className = "token-start";
    startBtn.textContent = "Start";
    startBtn.addEventListener("click", () => {
      startToken(token);
    });

    const stopBtn = document.createElement("button");
    stopBtn.type = "button";
    stopBtn.className = "token-stop";
    stopBtn.textContent = "Stop";
    stopBtn.addEventListener("click", () => {
      requestStopForToken(token);
    });

    const removeBtn = document.createElement("button");
    removeBtn.type = "button";
    removeBtn.className = "remove";
    removeBtn.textContent = "Remove";
    removeBtn.disabled = token.running || token.stopRequested;
    removeBtn.addEventListener("click", () => {
      if (token.running || token.stopRequested) {
        return;
      }
      removeToken(token.id);
    });

    actions.append(startBtn, stopBtn, removeBtn);

    topRow.append(fields, actions);

    const meta = document.createElement("div");
    meta.className = "token-meta";

    const createMetaItem = (label) => {
      const container = document.createElement("div");
      const labelEl = document.createElement("span");
      labelEl.className = "label";
      labelEl.textContent = label;
      const valueEl = document.createElement("span");
      valueEl.className = "token-meta-value";
      container.append(labelEl, valueEl);
      return { container, valueEl };
    };

    const statusItem = createMetaItem("Status");
    const backoffItem = createMetaItem("Backoff");
    const requestsItem = createMetaItem("Requests remaining");
    const avgTaskItem = createMetaItem("Avg task time");
    const projectionItem = createMetaItem("Expected in 24h");

    meta.append(
      statusItem.container,
      backoffItem.container,
      requestsItem.container,
      avgTaskItem.container,
      projectionItem.container,
    );

    const logContainer = document.createElement("div");
    logContainer.className = "token-log-container";

    const logLabel = document.createElement("span");
    logLabel.className = "label token-log-label";
    logLabel.textContent = "Activity";

    const logView = document.createElement("pre");
    logView.className = "token-log";
    logView.setAttribute("aria-live", "polite");
    const existingEntries = Array.isArray(token.logEntries) ? token.logEntries : [];
    logView.textContent = existingEntries.join("\n");
    logView.scrollTop = logView.scrollHeight;

    logContainer.append(logLabel, logView);

    body.append(topRow, meta, logContainer);
    row.append(summary, body);
    elements.tokenList.appendChild(row);

    token.dom = {
      row,
      summaryTitle,
      summaryStatus,
      summaryMeta,
      tokenInput,
      maxInput,
      startBtn,
      stopBtn,
      removeBtn,
      statusValue: statusItem.valueEl,
      backoffValue: backoffItem.valueEl,
      requestsValue: requestsItem.valueEl,
      log: logView,
      avgTaskTimeValue: avgTaskItem.valueEl,
      tasksPerDayValue: projectionItem.valueEl,
    };

    updateTokenDisplay(token);
  });
  updateGlobalMetrics();
  updateTokenSummary();
  updateCollapseAllButton();
}

function recordTaskCompletion(token) {
  if (!token) {
    return;
  }
  const completed = Number.isFinite(token.completedTasks) ? token.completedTasks : 0;
  token.completedTasks = completed + 1;
  updateTokenDisplay(token);
  updateGlobalMetrics();
}

async function getTask() {
  const response = await fetch("/task", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ client: "browser" }),
  });
  if (!response.ok) {
    throw new Error(`Task request failed with status ${response.status}`);
  }
  const payload = await response.json();
  return payload.task;
}

async function resetTask(task) {
  if (!task) return;
  const response = await fetch("/task/reset", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      steamAccountId: task.steamAccountId,
      type: task.type,
    }),
  });
  if (!response.ok) {
    throw new Error(`Reset failed with status ${response.status}`);
  }
}

async function fetchPlayerHeroes(playerId, token) {
  if (!token) {
    throw new Error('Stratz token is not set');
  }

  const query = `
    query HeroPerf($id: Long!) {
      player(steamAccountId: $id) {
        heroesPerformance(request: { take: 999999, gameModeIds: [1, 22] }, take: 200) {
          heroId
          matchCount
          winCount
        }
      }
    }
  `;
  const response = await fetch('https://api.stratz.com/graphql', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ query, variables: { id: playerId } }),
  });

  if (!response.ok) {
    const error = new Error(`Stratz API returned ${response.status}`);
    if (response.status === 429) {
      const retryAfterHeader = response.headers.get("retry-after");
      const retryAfterMs = parseRetryAfterHeader(retryAfterHeader);
      if (retryAfterMs !== null) {
        error.retryAfterMs = retryAfterMs;
      }
    }
    throw error;
  }

  const data = await response.json();
  const heroes = data?.data?.player?.heroesPerformance;
  if (!Array.isArray(heroes)) {
    return [];
  }

  return heroes.map((hero) => ({
    heroId: hero.heroId,
    games: hero.matchCount,
    wins: hero.winCount,
  }));
}

async function discoverMatches(playerId, token, { take = 100, skip = 0 } = {}) {
  const pageSizeCandidate = Number.isFinite(take) && take > 0 ? Math.floor(take) : 100;
  const pageSize = Math.max(1, pageSizeCandidate);
  const startingSkip = Number.isFinite(skip) && skip > 0 ? Math.floor(skip) : 0;

  const query = `
    query PlayerMatches($steamAccountId: Long!, $take: Int!, $skip: Int!) {
      player(steamAccountId: $steamAccountId) {
        matches(request: { take: $take, skip: $skip }) {
          id
          players {
            steamAccountId
          }
        }
      }
    }
  `;

  const discovered = new Map();
  let nextSkip = startingSkip;

  while (true) {
    const payload = await executeStratzQuery(
      query,
      { steamAccountId: playerId, take: pageSize, skip: nextSkip },
      token,
    );

    const matches = payload?.data?.player?.matches;
    if (!Array.isArray(matches) || matches.length === 0) {
      break;
    }

    matches.forEach((match) => {
      if (!Array.isArray(match?.players)) {
        return;
      }
      match.players.forEach((participant) => {
        const rawId = participant?.steamAccountId;
        const id =
          typeof rawId === "number"
            ? rawId
            : typeof rawId === "string"
              ? Number.parseInt(rawId, 10)
              : null;
        if (Number.isFinite(id) && id !== playerId) {
          const previous = discovered.get(id) ?? 0;
          discovered.set(id, previous + 1);
        }
      });
    });

    if (matches.length < pageSize) {
      break;
    }

    nextSkip += matches.length;
  }

  return Array.from(discovered, ([steamAccountId, count]) => ({
    steamAccountId,
    count,
  }));
}

async function submitHeroStats(playerId, heroes) {
  const response = await fetch("/submit", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      type: "fetch_hero_stats",
      steamAccountId: playerId,
      heroes,
      task: true,
    }),
  });
  if (!response.ok) {
    throw new Error(`Submit failed with status ${response.status}`);
  }
  const payload = await response.json();
  return payload?.task ?? null;
}

async function submitDiscovery(playerId, discovered, depth) {
  const payload = {
    type: "discover_matches",
    steamAccountId: playerId,
    discovered,
    task: true,
  };
  if (Number.isFinite(depth)) {
    payload.depth = depth;
  }
  const response = await fetch("/submit", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    throw new Error(`Submit failed with status ${response.status}`);
  }
  const responsePayload = await response.json();
  return responsePayload?.task ?? null;
}

const PROGRESS_REFRESH_INTERVAL = 10000;

const progressState = {
  lastPayload: null,
  lastFetchTime: 0,
  inflight: null,
};

function updateProgressDisplay(payload) {
  if (!payload) {
    return;
  }
  const heroLine = `${payload.hero_done} / ${payload.players_total}`;
  const discoverLine = `${payload.discover_done} / ${payload.players_total}`;
  elements.progressText.textContent = `Hero: ${heroLine} • Discover: ${discoverLine}`;
}

async function refreshProgress(options = {}) {
  const { force = false } = options;
  const now = Date.now();

  if (!force) {
    if (progressState.inflight) {
      const payload = await progressState.inflight;
      updateProgressDisplay(payload);
      return payload;
    }
    if (
      progressState.lastPayload &&
      now - progressState.lastFetchTime < PROGRESS_REFRESH_INTERVAL
    ) {
      updateProgressDisplay(progressState.lastPayload);
      return progressState.lastPayload;
    }
  }

  const fetchPromise = (async () => {
    const response = await fetch("/progress");
    if (!response.ok) {
      throw new Error(`Progress failed with status ${response.status}`);
    }
    const payload = await response.json();
    progressState.lastFetchTime = Date.now();
    progressState.lastPayload = payload;
    updateProgressDisplay(payload);
    return payload;
  })();

  progressState.inflight = fetchPromise;
  try {
    return await fetchPromise;
  } finally {
    if (progressState.inflight === fetchPromise) {
      progressState.inflight = null;
    }
  }
}

function setBestTableRefreshing(isRefreshing) {
  if (!elements.bestTable) {
    return;
  }
  elements.bestTable.classList.toggle("refreshing", Boolean(isRefreshing));
  if (isRefreshing) {
    elements.bestTable.setAttribute("aria-busy", "true");
  } else {
    elements.bestTable.removeAttribute("aria-busy");
  }
}

function renderBestTable(rows) {
  if (!elements.bestTable) {
    return;
  }
  const safeRows = Array.isArray(rows) ? rows : [];
  if (!safeRows.length) {
    elements.bestTable.innerHTML = '<p class="muted">No data yet.</p>';
    return;
  }
  const header = `
    <table>
      <thead>
        <tr>
          <th>Hero</th>
          <th>Steam Account ID</th>
          <th>Matches</th>
          <th>Wins</th>
          <th>Win Rate</th>
        </tr>
      </thead>
      <tbody>
  `;
  const body = safeRows
    .map((row) => {
      const heroName = formatCell(row?.hero_name);
      const slug = typeof row?.hero_slug === "string" ? row.hero_slug : "";
      const href = slug ? `/leaderboards/${encodeURIComponent(slug)}` : "";
      const heroCell = href ? `<a href="${href}">${heroName}</a>` : heroName;
      const normalizedSteamId = normalizeSteamAccountId(row?.player_id);
      const playerText = formatCell(row?.player_id);
      const playerCell = normalizedSteamId
        ? `<a href="https://stratz.com/players/${normalizedSteamId}" target="_blank" rel="noopener">${playerText}</a>`
        : playerText;
      const winRateCell = formatCell(formatWinRate(row?.wins, row?.matches));
      return `
        <tr>
          <td>${heroCell}</td>
          <td>${playerCell}</td>
          <td>${formatCell(row?.matches)}</td>
          <td>${formatCell(row?.wins)}</td>
          <td>${winRateCell}</td>
        </tr>
      `;
    })
    .join("");
  elements.bestTable.innerHTML = `${header}${body}</tbody></table>`;
}

async function loadBest() {
  setBestTableRefreshing(true);
  try {
    const response = await fetch("/best");
    if (!response.ok) {
      throw new Error(`Best request failed with status ${response.status}`);
    }
    const rows = await response.json();
    renderBestTable(rows);
    return rows;
  } finally {
    setBestTableRefreshing(false);
  }
}

async function seedRange() {
  const start = parseInt(elements.seedStart.value, 10);
  const end = parseInt(elements.seedEnd.value, 10);
  if (!Number.isFinite(start) || !Number.isFinite(end) || end < start) {
    alert("Please enter a valid start and end player ID.");
    return;
  }
  const response = await fetch(`/seed?start=${start}&end=${end}`);
  if (!response.ok) {
    throw new Error(`Seed failed with status ${response.status}`);
  }
  const payload = await response.json();
  log(`Seeded IDs ${payload.seeded[0]} - ${payload.seeded[1]}`);
}

async function workLoopForToken(token) {
  token.running = true;
  token.stopRequested = false;
  token.activeToken = token.value.trim();
  token.backoff = 1000;
  token.requestsRemaining = parseMaxRequests(token.maxRequests);
  token.lastStartMs = getNowMs();
  const shouldExpand = token.expanded !== false;
  token.expanded = shouldExpand;
  if (token.dom?.row) {
    token.dom.row.open = shouldExpand;
  }
  updateCollapseAllButton();
  updateTokenDisplay(token);
  updateRunningState();
  logToken(token, "Worker started.");
  if (typeof token.requestsRemaining === "number") {
    logToken(token, `Request limit ${token.requestsRemaining}.`);
  }

  let task = null;
  while (!token.stopRequested) {
    try {
      if (!task) {
        task = await getTask();
      }
      if (!task) {
        const wait = 60_000;
        logToken(token, "No tasks available. Waiting 60 seconds before retrying.");
        token.backoff = wait;
        updateBackoffDisplay();
        updateTokenDisplay(token);
        await delay(wait);
        if (token.stopRequested) {
          break;
        }
        continue;
      }
      if (token.stopRequested) {
        await resetTask(task).catch(() => {});
        break;
      }
      const taskId = task.steamAccountId;
      let nextTask = null;
      if (task.type === "fetch_hero_stats") {
        logToken(token, `Hero stats task for ${taskId}.`);
        const heroes = await fetchPlayerHeroes(taskId, token.activeToken);
        logToken(token, `Fetched ${heroes.length} heroes for ${taskId}.`);
        nextTask = await submitHeroStats(taskId, heroes);
        logToken(token, `Submitted ${heroes.length} heroes for ${taskId}.`);
      } else if (task.type === "discover_matches") {
        logToken(
          token,
          `Discovery task for ${taskId} (depth ${task.depth ?? 0}).`,
        );
        const discovered = await discoverMatches(taskId, token.activeToken);
        logToken(
          token,
          `Discovered ${discovered.length} accounts from ${taskId}.`,
        );
        nextTask = await submitDiscovery(taskId, discovered, task.depth);
        logToken(token, `Submitted discovery results for ${taskId}.`);
      } else {
        logToken(
          token,
          `Received unknown task type ${task.type}. Resetting task ${taskId}.`,
        );
        await resetTask(task).catch(() => {});
        break;
      }
      refreshProgress().catch((error) => {
        const message = error instanceof Error ? error.message : String(error);
        logToken(token, `Progress refresh failed: ${message}`);
      });
      recordTaskCompletion(token);
      if (token.requestsRemaining !== null) {
        token.requestsRemaining = Math.max(0, token.requestsRemaining - 1);
        updateRequestsRemainingDisplay();
        updateTokenDisplay(token);
        if (token.requestsRemaining === 0) {
          if (nextTask) {
            try {
              await resetTask(nextTask);
            } catch (resetError) {
              const resetMessage =
                resetError instanceof Error ? resetError.message : String(resetError);
              logToken(
                token,
                `Failed to reset next task ${nextTask?.steamAccountId ?? "?"}: ${resetMessage}`,
              );
            }
          }
          logToken(token, "Reached request limit. Stopping worker.");
          break;
        }
      }
      task = nextTask ?? null;
      token.backoff = 10000;
      updateBackoffDisplay();
      updateTokenDisplay(token);
      if (!task) {
        await delay(NO_TASK_RETRY_DELAY_MS);
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const activeId = task?.steamAccountId;
      logToken(token, `Error${activeId ? ` for ${activeId}` : ""}: ${message}`);
      const hadTask = Boolean(task);
      if (task) {
        try {
          await resetTask(task);
          logToken(token, `Reset task ${task.steamAccountId}.`);
        } catch (resetError) {
          const resetMessage =
            resetError instanceof Error ? resetError.message : String(resetError);
          logToken(
            token,
            `Failed to reset ${task?.steamAccountId ?? "?"}: ${resetMessage}`,
          );
        }
      }
      task = null;
      if (!hadTask) {
        const wait = 60_000;
        token.backoff = wait;
        updateBackoffDisplay();
        updateTokenDisplay(token);
        await delay(wait);
      } else {
        const retryAfterMs = getRetryAfterMsFromError(error);
        let waitMs = token.backoff;
        if (retryAfterMs !== null) {
          waitMs = Math.min(retryAfterMs, state.maxBackoff);
        }

        waitMs = Math.max(0, waitMs);
        token.backoff = waitMs;
        updateBackoffDisplay();
        updateTokenDisplay(token);

        await delay(waitMs);

        if (retryAfterMs === null) {
          token.backoff = Math.min(
            Math.ceil(token.backoff * 1.2),
            state.maxBackoff,
          );
          updateBackoffDisplay();
          updateTokenDisplay(token);
        }
      }
    }
  }

  if (typeof token.lastStartMs === "number") {
    token.totalRuntimeMs += Math.max(0, getNowMs() - token.lastStartMs);
    token.lastStartMs = null;
  }

  token.running = false;
  token.backoff = 10000;
  token.activeToken = null;
  token.stopRequested = false;
  token.requestsRemaining = parseMaxRequests(token.maxRequests);
  updateTokenDisplay(token);
  logToken(token, "Worker stopped.");
  updateRunningState();
}

function loadTokensFromStorage() {
  let saved = null;
  try {
    saved = localStorage.getItem("stratz_tokens");
  } catch (error) {
    console.warn("Failed to access localStorage", error);
  }

  if (saved) {
    try {
      const decoded = JSON.parse(saved);
      if (Array.isArray(decoded)) {
        decoded.forEach((entry) => {
          addTokenRow(
            {
              value: entry.token ?? "",
              maxRequests:
                entry.maxRequests === null || entry.maxRequests === undefined
                  ? ""
                  : entry.maxRequests,
            },
            { skipPersist: true, fromStorage: true },
          );
        });
      }
    } catch (error) {
      console.warn("Failed to load saved tokens from localStorage", error);
    }
  } else {
    const cookieSaved = getCookie("stratz_tokens");
    if (cookieSaved) {
      try {
        const decoded = JSON.parse(decodeURIComponent(cookieSaved));
        if (Array.isArray(decoded)) {
          decoded.forEach((entry) => {
            addTokenRow(
              {
                value: entry.token ?? "",
                maxRequests:
                  entry.maxRequests === null || entry.maxRequests === undefined
                    ? ""
                    : entry.maxRequests,
              },
              { skipPersist: true, fromStorage: true },
            );
          });
          persistTokens();
          log("Migrated saved tokens from cookies to local storage.");
        }
      } catch (error) {
        console.warn("Failed to migrate saved tokens from cookies", error);
      }
      clearCookie("stratz_tokens");
    } else {
      const legacy = getCookie("stratz_token");
      if (legacy) {
        addTokenRow({ value: legacy }, { skipPersist: true, fromStorage: true });
        persistTokens();
        clearCookie("stratz_token");
        log("Migrated saved token to local storage.");
      }
    }
  }

  if (!state.tokens.length) {
    renderTokens();
  }
  updateButtons();
}

function initialise() {
  loadTokensFromStorage();
  updateBackoffDisplay();
  updateRequestsRemainingDisplay();
  updateGlobalMetrics();
  updateTokenSummary();
  refreshProgress().catch((error) => log(error.message));
  loadBest().catch((error) => log(error.message));
}

if (elements.addToken) {
  elements.addToken.addEventListener("click", () => {
    addTokenRow();
  });
}

if (elements.exportTokens) {
  elements.exportTokens.addEventListener("click", () => {
    try {
      downloadTokens();
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      log(`Token export failed: ${message}`);
    }
  });
}

if (elements.importTokens) {
  elements.importTokens.addEventListener("click", () => {
    if (state.running) {
      alert("Stop all active tokens before importing.");
      return;
    }
    if (elements.importTokensFile) {
      elements.importTokensFile.click();
    }
  });
}

if (elements.importTokensFile) {
  elements.importTokensFile.addEventListener("change", (event) => {
    const target = event.target;
    const files = target?.files;
    const file = files && files.length > 0 ? files[0] : null;
    handleImportFile(file);
  });
}

if (elements.toggleTokens) {
  elements.toggleTokens.addEventListener("click", () => {
    if (!state.tokens.length) {
      return;
    }
    const shouldExpand = state.tokens.some((token) => token.expanded === false);
    setAllTokensExpanded(shouldExpand);
  });
}

elements.begin.addEventListener("click", () => {
  const readyTokens = state.tokens.filter(
    (token) => !token.running && !token.stopRequested && token.value.trim().length > 0,
  );
  if (!readyTokens.length) {
    alert("Add a Stratz token first.");
    return;
  }
  readyTokens.forEach((token) => {
    startToken(token);
  });
});

elements.stop.addEventListener("click", () => {
  const active = state.tokens.filter((token) => token.running && !token.stopRequested);
  if (!active.length) {
    return;
  }
  active.forEach((token) => {
    requestStopForToken(token, { silent: true });
  });
  log("Stop requested for all active tokens.");
});

elements.progress.addEventListener("click", () => {
  refreshProgress({ force: true }).catch((error) => log(error.message));
});

elements.best.addEventListener("click", () => {
  loadBest()
    .then(() => log("Best heroes updated."))
    .catch((error) => log(error.message));
});

if (elements.seedBtn) {
  elements.seedBtn.addEventListener("click", () => {
    seedRange().catch((error) => log(error.message));
  });
}

initialise();
