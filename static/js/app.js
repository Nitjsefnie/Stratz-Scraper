const state = {
  running: false,
  maxBackoff: 86_400_000,
  tokenCounter: 0,
  tokens: [],
};

const elements = {
  tokenList: document.getElementById("tokenList"),
  addToken: document.getElementById("addToken"),
  begin: document.getElementById("begin"),
  stop: document.getElementById("stop"),
  progress: document.getElementById("progress"),
  best: document.getElementById("best"),
  seedBtn: document.getElementById("seedBtn"),
  seedStart: document.getElementById("seedStart"),
  seedEnd: document.getElementById("seedEnd"),
  log: document.getElementById("log"),
  bestTable: document.getElementById("bestTable"),
  progressText: document.getElementById("progressText"),
  backoffText: document.getElementById("backoffText"),
  statusChip: document.getElementById("runStatus"),
  requestsRemaining: document.getElementById("requestsRemaining"),
};

function setCookie(name, value, days) {
  const date = new Date();
  date.setTime(date.getTime() + days * 86400000);
  document.cookie = `${name}=${value}; expires=${date.toUTCString()}; path=/`;
}

function getCookie(name) {
  const cname = `${name}=`;
  return document.cookie
    .split(";")
    .map((c) => c.trim())
    .find((c) => c.startsWith(cname))
    ?.slice(cname.length);
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function log(message) {
  const timestamp = new Date().toLocaleTimeString();
  elements.log.textContent += `[${timestamp}] ${message}\n`;
  if (elements.log.textContent.length > 50_000) {
    elements.log.textContent = elements.log.textContent.slice(-40_000);
  }
  elements.log.scrollTop = elements.log.scrollHeight;
}

function formatDuration(ms) {
  if (!Number.isFinite(ms) || ms <= 0) return "—";
  if (ms < 1000) return `${ms} ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)} s`;
  const minutes = Math.round(ms / 60_000);
  return `${minutes} min`;
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

function refreshStatusChip() {
  if (state.running) {
    elements.statusChip.textContent = "Running";
    elements.statusChip.classList.add("running");
    elements.statusChip.classList.remove("error");
  } else {
    elements.statusChip.textContent = "Idle";
    elements.statusChip.classList.remove("running", "error");
  }
}

function showErrorStatus(message) {
  elements.statusChip.textContent = message;
  elements.statusChip.classList.add("error");
  elements.statusChip.classList.remove("running");
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

function clearCookie(name) {
  document.cookie = `${name}=; expires=Thu, 01 Jan 1970 00:00:00 GMT; path=/`;
}

function persistTokens() {
  const payload = state.tokens
    .map((token) => ({
      token: token.value.trim(),
      maxRequests: parseMaxRequests(token.maxRequests),
    }))
    .filter((entry) => entry.token.length > 0);

  if (!payload.length) {
    clearCookie("stratz_tokens");
    return;
  }

  setCookie("stratz_tokens", encodeURIComponent(JSON.stringify(payload)), 30);
}

function getTokenLabel(token) {
  const index = state.tokens.indexOf(token);
  return index >= 0 ? index + 1 : token.id;
}

function updateRunningState() {
  state.running = state.tokens.some((token) => token.running);
  refreshStatusChip();
  updateButtons();
  updateBackoffDisplay();
  updateRequestsRemainingDisplay();
}

function removeToken(id) {
  const index = state.tokens.findIndex((token) => token.id === id);
  if (index === -1) return;
  const [token] = state.tokens.splice(index, 1);
  token.running = false;
  renderTokens();
  persistTokens();
  updateRunningState();
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
  };
  state.tokens.push(token);
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
    return;
  }

  state.tokens.forEach((token) => {
    const row = document.createElement("div");
    row.className = "token-row";

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

    row.append(tokenInput, maxInput, removeBtn);
    elements.tokenList.appendChild(row);
  });
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

async function resetTask(playerId) {
  const response = await fetch("/task/reset", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ player_id: playerId }),
  });
  if (!response.ok) {
    throw new Error(`Reset failed with status ${response.status}`);
  }
}

async function fetchPlayerHeroes(playerId, token) {
  if (!token) {
    throw new Error("Stratz token is not set");
  }

  const query = `
    query HeroPerf($id: Long!) {
      player(steamAccountId: $id) {
        steamAccountId
        heroesPerformance(request: { take: 999999, gameModeIds: [1, 22] }, take: 200) {
          heroId
          matchCount
          winCount
        }
      }
    }
  `;
  const response = await fetch("https://api.stratz.com/graphql", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ query, variables: { id: playerId } }),
  });
  if (!response.ok) {
    throw new Error(`Stratz API returned ${response.status}`);
  }
  const data = await response.json();
  if (!data?.data?.player) {
    return [];
  }
  return data.data.player.heroesPerformance.map((hero) => ({
    hero_id: hero.heroId,
    games: hero.matchCount,
    wins: hero.winCount,
  }));
}

async function submitBulk(playerId, heroes) {
  const response = await fetch("/submit", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ player_id: playerId, heroes }),
  });
  if (!response.ok) {
    throw new Error(`Submit failed with status ${response.status}`);
  }
}

async function refreshProgress() {
  const response = await fetch("/progress");
  if (!response.ok) {
    throw new Error(`Progress failed with status ${response.status}`);
  }
  const payload = await response.json();
  elements.progressText.textContent = `${payload.done} / ${payload.total}`;
  return payload;
}

function renderBestTable(rows) {
  if (!rows.length) {
    elements.bestTable.innerHTML = '<p class="muted">No data yet.</p>';
    return;
  }
  const header = `
    <table>
      <thead>
        <tr>
          <th>Hero</th>
          <th>Hero ID</th>
          <th>Player ID</th>
          <th>Matches</th>
          <th>Wins</th>
        </tr>
      </thead>
      <tbody>
  `;
  const body = rows
    .map(
      (row) => `
        <tr>
          <td>${row.hero_name}</td>
          <td>${row.hero_id}</td>
          <td>${row.player_id}</td>
          <td>${row.matches}</td>
          <td>${row.wins}</td>
        </tr>
      `,
    )
    .join("");
  elements.bestTable.innerHTML = `${header}${body}</tbody></table>`;
}

async function loadBest() {
  const response = await fetch("/best");
  if (!response.ok) {
    throw new Error(`Best request failed with status ${response.status}`);
  }
  const rows = await response.json();
  renderBestTable(rows);
  return rows;
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
  const label = getTokenLabel(token);
  token.running = true;
  token.stopRequested = false;
  token.activeToken = token.value.trim();
  token.backoff = 1000;
  token.requestsRemaining = parseMaxRequests(token.maxRequests);
  updateRunningState();
  renderTokens();
  log(`Token #${label}: worker started.`);
  if (typeof token.requestsRemaining === "number") {
    log(`Token #${label}: request limit ${token.requestsRemaining}.`);
  }

  while (!token.stopRequested) {
    let taskId = null;
    try {
      taskId = await getTask();
      if (!taskId) {
        log(`Token #${label}: no tasks left. Stopping worker.`);
        break;
      }
      if (token.stopRequested) {
        await resetTask(taskId).catch(() => {});
        break;
      }
      log(`Token #${label}: fetched task ${taskId}.`);
      const heroes = await fetchPlayerHeroes(taskId, token.activeToken);
      log(`Token #${label}: fetched ${heroes.length} heroes for ${taskId}.`);
      await submitBulk(taskId, heroes);
      log(`Token #${label}: submitted ${heroes.length} heroes for ${taskId}.`);
      await refreshProgress();
      if (token.requestsRemaining !== null) {
        token.requestsRemaining = Math.max(0, token.requestsRemaining - 1);
        updateRequestsRemainingDisplay();
        if (token.requestsRemaining === 0) {
          log(`Token #${label}: reached request limit. Stopping worker.`);
          break;
        }
      }
      token.backoff = 1000;
      updateBackoffDisplay();
      await delay(500);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      log(`Token #${label}: error${taskId ? ` for ${taskId}` : ""}: ${message}`);
      showErrorStatus("Retrying");
      if (taskId !== null) {
        try {
          await resetTask(taskId);
          log(`Token #${label}: reset task ${taskId}.`);
        } catch (resetError) {
          const resetMessage =
            resetError instanceof Error ? resetError.message : String(resetError);
          log(`Token #${label}: failed to reset ${taskId}: ${resetMessage}`);
        }
      }
      await delay(token.backoff);
      token.backoff = Math.min(Math.ceil(token.backoff * 1.1), state.maxBackoff);
      updateBackoffDisplay();
      if (!token.stopRequested) {
        refreshStatusChip();
      }
    }
  }

  token.running = false;
  token.backoff = 1000;
  token.activeToken = null;
  token.stopRequested = false;
  token.requestsRemaining = parseMaxRequests(token.maxRequests);
  log(`Token #${label}: worker stopped.`);
  renderTokens();
  updateRunningState();
}

function loadTokensFromCookie() {
  const saved = getCookie("stratz_tokens");
  if (saved) {
    try {
      const decoded = JSON.parse(decodeURIComponent(saved));
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
            { skipPersist: true },
          );
        });
      }
    } catch (error) {
      console.warn("Failed to load saved tokens", error);
    }
  } else {
    const legacy = getCookie("stratz_token");
    if (legacy) {
      addTokenRow({ value: legacy }, { skipPersist: true });
      clearCookie("stratz_token");
      persistTokens();
      log("Migrated saved token to new format.");
    }
  }

  if (!state.tokens.length) {
    renderTokens();
  }
  updateButtons();
}

function initialise() {
  loadTokensFromCookie();
  updateBackoffDisplay();
  updateRequestsRemainingDisplay();
  refreshStatusChip();
  refreshProgress().catch((error) => log(error.message));
  loadBest().catch((error) => log(error.message));
}

if (elements.addToken) {
  elements.addToken.addEventListener("click", () => {
    addTokenRow();
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
    if (!token.running) {
      workLoopForToken(token).catch((error) => {
        log(
          `Token #${getTokenLabel(token)}: failed to start worker: ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
      });
    }
  });
});

elements.stop.addEventListener("click", () => {
  const active = state.tokens.filter((token) => token.running && !token.stopRequested);
  if (!active.length) {
    return;
  }
  active.forEach((token) => {
    token.stopRequested = true;
  });
  renderTokens();
  updateButtons();
  log("Stop requested for all active tokens.");
});

elements.progress.addEventListener("click", () => {
  refreshProgress().catch((error) => log(error.message));
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
