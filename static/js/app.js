const state = {
  running: false,
  backoff: 1000,
  maxBackoff: 86_400_000,
  requestsRemaining: null,
};

const elements = {
  token: document.getElementById("token"),
  saveToken: document.getElementById("saveToken"),
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
  maxRequests: document.getElementById("maxRequests"),
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
  if (!state.running) return "—";
  if (ms < 1000) return `${ms} ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)} s`;
  const minutes = Math.round(ms / 60_000);
  return `${minutes} min`;
}

function updateBackoffDisplay() {
  elements.backoffText.textContent = formatDuration(state.backoff);
}

function updateRequestsRemainingDisplay() {
  if (!elements.requestsRemaining) return;
  if (typeof state.requestsRemaining === "number") {
    elements.requestsRemaining.textContent = state.requestsRemaining;
  } else {
    elements.requestsRemaining.textContent = "—";
  }
}

function configureRequestLimit() {
  if (!elements.maxRequests) {
    state.requestsRemaining = null;
    updateRequestsRemainingDisplay();
    return;
  }

  const max = parseInt(elements.maxRequests.value, 10);
  if (Number.isFinite(max) && max > 0) {
    state.requestsRemaining = max;
  } else {
    state.requestsRemaining = null;
  }
  updateRequestsRemainingDisplay();
}

function decrementRequestsRemaining() {
  if (typeof state.requestsRemaining === "number") {
    state.requestsRemaining = Math.max(0, state.requestsRemaining - 1);
    updateRequestsRemainingDisplay();
  }
}

function setRunning(running) {
  state.running = running;
  if (!running) {
    elements.statusChip.textContent = "Idle";
    elements.statusChip.classList.remove("running", "error");
    state.backoff = 1000;
    updateBackoffDisplay();
  } else {
    elements.statusChip.textContent = "Running";
    elements.statusChip.classList.add("running");
    elements.statusChip.classList.remove("error");
    updateBackoffDisplay();
  }
  updateButtons();
  updateRequestsRemainingDisplay();
}

function setErrorState(message) {
  elements.statusChip.textContent = message;
  elements.statusChip.classList.add("error");
  elements.statusChip.classList.remove("running");
}

function updateButtons() {
  elements.begin.disabled = state.running;
  elements.stop.disabled = !state.running;
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

async function fetchPlayerHeroes(playerId) {
  const token = getCookie("stratz_token");
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

async function workLoop() {
  setRunning(true);
  log("Worker started.");
  if (typeof state.requestsRemaining === "number") {
    log(`Request limit: ${state.requestsRemaining}.`);
  }
  updateBackoffDisplay();

  while (state.running) {
    let taskId = null;
    try {
      taskId = await getTask();
      if (!taskId) {
        log("No tasks left. Stopping worker.");
        setRunning(false);
        break;
      }
      log(`Fetched task ${taskId}.`);
      const heroes = await fetchPlayerHeroes(taskId);
      log(`Fetched ${heroes.length} heroes for ${taskId}.`);
      await submitBulk(taskId, heroes);
      log(`Submitted ${heroes.length} heroes for ${taskId}.`);
      await refreshProgress();
      decrementRequestsRemaining();
      if (typeof state.requestsRemaining === "number" && state.requestsRemaining === 0) {
        log("Reached request limit. Stopping worker.");
        setRunning(false);
        break;
      }
      state.backoff = 1000;
      updateBackoffDisplay();
      await delay(500);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      log(`Error${taskId ? ` for ${taskId}` : ""}: ${message}`);
      setErrorState("Retrying");
      if (taskId !== null) {
        try {
          await resetTask(taskId);
          log(`Reset task ${taskId}.`);
        } catch (resetError) {
          const resetMessage =
            resetError instanceof Error ? resetError.message : String(resetError);
          log(`Failed to reset ${taskId}: ${resetMessage}`);
        }
      }
      await delay(state.backoff);
      state.backoff = Math.min(Math.ceil(state.backoff * 1.1), state.maxBackoff);
      updateBackoffDisplay();
      elements.statusChip.textContent = "Running";
      elements.statusChip.classList.remove("error");
      elements.statusChip.classList.add("running");
    }
  }

  if (!state.running) {
    log("Worker stopped.");
  }
  updateButtons();
}

function initialise() {
  const savedToken = getCookie("stratz_token");
  if (savedToken) {
    elements.token.value = savedToken;
  }
  updateButtons();
  updateBackoffDisplay();
  refreshProgress().catch((error) => log(error.message));
  loadBest().catch((error) => log(error.message));
}

elements.saveToken.addEventListener("click", () => {
  const token = elements.token.value.trim();
  if (!token) {
    alert("Enter a Stratz token first.");
    return;
  }
  setCookie("stratz_token", token, 30);
  log("Token saved to cookie.");
});

elements.begin.addEventListener("click", () => {
  if (!state.running) {
    workLoop();
  }
});

elements.stop.addEventListener("click", () => {
  if (state.running) {
    setRunning(false);
  }
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

if (elements.maxRequests) {
  elements.maxRequests.addEventListener("input", () => {
    if (!state.running) {
      configureRequestLimit();
    }
  });
}

initialise();
