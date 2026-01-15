const state = {
  jobs: [],
};

const jobsList = document.getElementById("jobsList");
const jobsCount = document.getElementById("jobsCount");
const connectionStatus = document.getElementById("connectionStatus");

const statRunning = document.getElementById("statRunning");
const statPaused = document.getElementById("statPaused");
const statStopped = document.getElementById("statStopped");
const statPages = document.getElementById("statPages");

const startForm = document.getElementById("startForm");
const urlInput = document.getElementById("urlInput");
const maxPagesInput = document.getElementById("maxPagesInput");
const keywordsInput = document.getElementById("keywordsInput");

const formatNumber = (value) => {
  if (Number.isNaN(value) || value === null || value === undefined) {
    return "0";
  }
  return value.toLocaleString();
};

const formatRate = (value) => {
  if (!value || Number.isNaN(value)) {
    return "0.00";
  }
  return value.toFixed(2);
};

const formatDuration = (seconds) => {
  if (!seconds || Number.isNaN(seconds)) return "0s";
  const mins = Math.floor(seconds / 60);
  const secs = Math.floor(seconds % 60);
  if (mins <= 0) return `${secs}s`;
  return `${mins}m ${secs}s`;
};

const updateOverview = () => {
  const running = state.jobs.filter((job) => job.status === "running").length;
  const paused = state.jobs.filter((job) => job.status === "paused").length;
  const stopped = state.jobs.filter((job) => ["stopped", "done", "error"].includes(job.status)).length;
  const totalPages = state.jobs.reduce((sum, job) => sum + (job.pages_success || 0), 0);

  statRunning.textContent = formatNumber(running);
  statPaused.textContent = formatNumber(paused);
  statStopped.textContent = formatNumber(stopped);
  statPages.textContent = formatNumber(totalPages);
  jobsCount.textContent = `${state.jobs.length} job${state.jobs.length === 1 ? "" : "s"}`;
};

const buildJobCard = (job) => {
  const uptime = job.start_time ? (Date.now() / 1000 - job.start_time) : 0;
  const lastError = job.last_error ? `<div class="job-stat"><span>Last error</span><strong class="mono">${job.last_error}</strong></div>` : "";

  const pauseLabel = job.status === "paused" ? "Resume" : "Pause";
  const pauseAction = job.status === "paused" ? "resume" : "pause";

  return `
    <div class="job" data-job-id="${job.job_id}">
      <div class="job-head">
        <div>
          <h3 class="job-title">${job.url}</h3>
          <div class="job-meta">job ${job.job_id} · max ${job.max_pages} pages</div>
        </div>
        <div class="actions">
          <span class="badge ${job.status}">${job.status}</span>
          <button class="action-btn" data-action="${pauseAction}">${pauseLabel}</button>
          <button class="action-btn stop" data-action="stop">Stop</button>
        </div>
      </div>
      <div class="job-grid">
        <div class="job-stat"><span>Pages/sec</span><strong>${formatRate(job.pages_per_sec)}</strong></div>
        <div class="job-stat"><span>Attempted</span><strong>${formatNumber(job.pages_attempted)}</strong></div>
        <div class="job-stat"><span>Collected</span><strong>${formatNumber(job.pages_success)}</strong></div>
        <div class="job-stat"><span>Errors</span><strong>${formatNumber(job.errors)}</strong></div>
        <div class="job-stat"><span>Queue</span><strong>${formatNumber(job.queue_size)}</strong></div>
        <div class="job-stat"><span>Uptime</span><strong>${formatDuration(uptime)}</strong></div>
        <div class="job-stat"><span>Last URL</span><strong class="mono">${job.last_url || "-"}</strong></div>
        ${lastError}
      </div>
    </div>
  `;
};

const renderJobs = () => {
  jobsList.innerHTML = state.jobs.map(buildJobCard).join("");
  updateOverview();
};

const updateJobs = (incoming) => {
  const map = new Map(state.jobs.map((job) => [job.job_id, job]));
  incoming.forEach((job) => {
    map.set(job.job_id, { ...map.get(job.job_id), ...job });
  });
  state.jobs = Array.from(map.values()).sort((a, b) => (b.last_update || 0) - (a.last_update || 0));
  renderJobs();
};

const postAction = async (endpoint, payload) => {
  await fetch(endpoint, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
};

startForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const url = urlInput.value.trim();
  if (!url) return;

  const contentTypes = Array.from(startForm.querySelectorAll("input[type=checkbox]:checked")).map(
    (checkbox) => checkbox.value
  );
  const maxPages = parseInt(maxPagesInput.value, 10) || 5;
  const keywords = (keywordsInput.value || "")
    .split(",")
    .map((word) => word.trim())
    .filter(Boolean);

  const response = await fetch("/api/crawl/start", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      url,
      max_pages: maxPages,
      content_types: contentTypes.length ? contentTypes : ["html"],
      keywords,
    }),
  });

  if (response.ok) {
    urlInput.value = "";
    keywordsInput.value = "";
  }
});

jobsList.addEventListener("click", async (event) => {
  const button = event.target.closest("button[data-action]");
  if (!button) return;
  const card = event.target.closest(".job");
  if (!card) return;
  const jobId = card.dataset.jobId;
  const action = button.dataset.action;

  if (action === "pause") {
    await postAction("/api/crawl/pause", { job_id: jobId });
  } else if (action === "resume") {
    await postAction("/api/crawl/resume", { job_id: jobId });
  } else if (action === "stop") {
    await postAction("/api/crawl/stop", { job_id: jobId });
  }
});

const connectStream = () => {
  const source = new EventSource("/api/stream");
  connectionStatus.textContent = "Live";
  connectionStatus.classList.add("live");

  source.onmessage = (event) => {
    const payload = JSON.parse(event.data);
    if (payload.type === "snapshot") {
      state.jobs = payload.jobs || [];
      renderJobs();
    }
    if (payload.type === "stats") {
      updateJobs(payload.jobs || []);
    }
  };

  source.onerror = () => {
    connectionStatus.textContent = "Offline";
    connectionStatus.classList.remove("live");
    source.close();
    setTimeout(connectStream, 1500);
  };
};

connectStream();
