const state = {
  jobs: [],
  reporting: {
    sessions: [],
    currentSessionId: null,
    summary: null,
  },
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

// Reporting elements
const sessionSelect = document.getElementById("sessionSelect");
const refreshSessionsBtn = document.getElementById("refreshSessions");
const contentTypeList = document.getElementById("contentTypeList");
const domainList = document.getElementById("domainList");
const keywordList = document.getElementById("keywordList");
const latestItems = document.getElementById("latestItems");
const sessionMeta = document.getElementById("sessionMeta");
const reportOutput = document.getElementById("reportOutput");
const reportInstructions = document.getElementById("reportInstructions");
const generateReportBtn = document.getElementById("generateReport");
const domainCount = document.getElementById("domainCount");
const keywordCount = document.getElementById("keywordCount");
const latestCount = document.getElementById("latestCount");
const contentTypeChart = document.getElementById("contentTypeChart");
const domainChart = document.getElementById("domainChart");
const keywordChart = document.getElementById("keywordChart");
const timeChart = document.getElementById("timeChart");
const gqmPanel = document.getElementById("gqmPanel");
const pageSelect = document.getElementById("pageSelect");
const analyzePageBtn = document.getElementById("analyzePage");
const pageSummary = document.getElementById("pageSummary");
const pageInsights = document.getElementById("pageInsights");
const pageCharts = document.getElementById("pageCharts");

let pageChartInstances = [];

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

const formatDate = (iso) => {
  if (!iso) return "n/a";
  const date = new Date(iso);
  return date.toLocaleString();
};

const escapeHtml = (text = "") =>
  text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");

const fetchJson = async (url, options = {}) => {
  const response = await fetch(url, options);
  const payload = await response.json();
  if (!response.ok) {
    const message = payload.error || "Request failed";
    throw new Error(message);
  }
  return payload;
};

const formatMetricValue = (metric) => {
  if (!metric) return "n/a";
  const value = metric.value;
  if (value === null || value === undefined || Number.isNaN(value)) return "n/a";
  const format = metric.format || "number";
  if (format === "percent") {
    return `${(value * 100).toFixed(1)}%`;
  }
  if (format === "hours") {
    return `${value.toFixed(2)}h`;
  }
  if (format === "float1") {
    return value.toFixed(1);
  }
  if (format === "float2") {
    return value.toFixed(2);
  }
  if (format === "float3") {
    return value.toFixed(3);
  }
  return formatNumber(value);
};

const renderGqm = (gqm, container = gqmPanel) => {
  if (!gqm || !gqm.goals || !gqm.goals.length) {
    container.innerHTML = '<span class="muted">Generate a report to render GQM metrics.</span>';
    return;
  }

  container.innerHTML = gqm.goals
    .map(
      (goal) => `
      <div class="gqm-card">
        <h5>${escapeHtml(goal.title || "Goal")}</h5>
        <div class="gqm-goal">${escapeHtml(goal.goal || "")}</div>
        ${goal.questions
          .map(
            (question) => `
            <div class="gqm-question">${escapeHtml(question.text || "")}</div>
            ${(question.metrics || [])
              .map(
                (metric) => `
                <div class="gqm-metric">
                  <span>${escapeHtml(metric.label || metric.key || "")}</span>
                  <strong>${escapeHtml(formatMetricValue(metric))}${
                    metric.unit && metric.format !== "percent" ? ` ${escapeHtml(metric.unit)}` : ""
                  }</strong>
                </div>
              `
              )
              .join("")}
          `
          )
          .join("")}
      </div>
    `
    )
    .join("");
};

const renderPageInsights = (summaryText, insights) => {
  pageSummary.textContent = summaryText || "No summary available for this page.";
  if (!insights || !insights.length) {
    pageInsights.innerHTML = '<span class="muted">No notable insights extracted.</span>';
    return;
  }

  pageInsights.innerHTML = insights
    .map((insight) => {
      if (typeof insight === "string") {
        return `<div class="insight-card"><h5>Insight</h5><div class="muted">${escapeHtml(insight)}</div></div>`;
      }
      const title = insight.title || "Insight";
      const detail = insight.detail || insight.description || "";
      const evidence = insight.evidence ? `<div class="muted">${escapeHtml(insight.evidence)}</div>` : "";
      return `
        <div class="insight-card">
          <h5>${escapeHtml(title)}</h5>
          <div class="muted">${escapeHtml(detail)}</div>
          ${evidence}
        </div>
      `;
    })
    .join("");
};

const destroyPageCharts = () => {
  pageChartInstances.forEach((chart) => chart.destroy());
  pageChartInstances = [];
  pageCharts.innerHTML = "";
};

const renderPageCharts = (charts = []) => {
  destroyPageCharts();
  if (!charts.length) {
    pageCharts.innerHTML = '<span class="muted">No chartable data found on this page.</span>';
    return;
  }

  charts.slice(0, 3).forEach((chart, index) => {
    const card = document.createElement("div");
    card.className = "chart-card";
    const title = document.createElement("h5");
    title.textContent = chart.title || `Chart ${index + 1}`;
    const canvas = document.createElement("canvas");
    const chartId = `page-chart-${index}`;
    canvas.id = chartId;
    card.appendChild(title);
    card.appendChild(canvas);
    pageCharts.appendChild(card);

    const chartType = (chart.type || "bar").toLowerCase();
    const series = Array.isArray(chart.series) ? chart.series : [];

    const normalizePoints = (points) =>
      (points || [])
        .map((point) => {
          const xValue = point.x ?? point.label ?? point.year ?? point.category ?? "";
          const yValue = Number(point.y);
          if (Number.isNaN(yValue)) return null;
          return { x: String(xValue), y: yValue };
        })
        .filter(Boolean);

    let config = {};
    if (chartType === "pie") {
      const pieSeries = series[0] || { data: [] };
      const dataPoints = normalizePoints(pieSeries.data);
      config = {
        type: "pie",
        data: {
          labels: dataPoints.map((point) => point.x),
          datasets: [
            {
              data: dataPoints.map((point) => point.y),
              backgroundColor: dataPoints.map((_, idx) => palette[idx % palette.length]),
            },
          ],
        },
        options: {
          responsive: true,
          plugins: { legend: { position: "bottom" } },
        },
      };
    } else {
      const datasets = series.map((serie, idx) => ({
        label: serie.name || `Series ${idx + 1}`,
        data: normalizePoints(serie.data),
        borderColor: palette[idx % palette.length],
        backgroundColor: palette[idx % palette.length],
        fill: chartType === "area",
        tension: 0.3,
      }));

      config = {
        type: chartType === "area" ? "line" : chartType,
        data: { datasets },
        options: {
          responsive: true,
          plugins: { legend: { position: "bottom" } },
          scales: {
            x: {
              type: "category",
              title: { display: !!chart.x_label, text: chart.x_label || "" },
            },
            y: {
              beginAtZero: true,
              title: { display: !!chart.y_label, text: chart.y_label || "" },
            },
          },
        },
      };
    }

    try {
      pageChartInstances.push(new Chart(canvas.getContext("2d"), config));
    } catch {
      card.appendChild(document.createTextNode("Unable to render chart."));
    }
  });
};

const palette = ["#f05d5e", "#f8b45b", "#7fb7be", "#c5d86d", "#9b5de5", "#4d908e", "#ff7b00"];

const buildSvg = (width, height, content) =>
  `<svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Chart">${content}</svg>`;

const renderPieChart = (items, container, labelKey, valueKey) => {
  if (!items || !items.length) {
    container.innerHTML = '<span class="muted">No data</span>';
    return;
  }
  const width = 260;
  const height = 200;
  const cx = width / 2;
  const cy = height / 2;
  const radius = 70;
  const total = items.reduce((sum, item) => sum + (item[valueKey] || 0), 0) || 1;

  let startAngle = -Math.PI / 2;
  let slices = "";
  let legend = "";

  items.slice(0, 6).forEach((item, index) => {
    const value = item[valueKey] || 0;
    const angle = (value / total) * Math.PI * 2;
    const endAngle = startAngle + angle;
    const x1 = cx + radius * Math.cos(startAngle);
    const y1 = cy + radius * Math.sin(startAngle);
    const x2 = cx + radius * Math.cos(endAngle);
    const y2 = cy + radius * Math.sin(endAngle);
    const largeArc = angle > Math.PI ? 1 : 0;
    const color = palette[index % palette.length];
    const path = `M ${cx} ${cy} L ${x1} ${y1} A ${radius} ${radius} 0 ${largeArc} 1 ${x2} ${y2} Z`;
    slices += `<path d="${path}" fill="${color}" />`;
    legend += `<text class="chart-label" x="155" y="${30 + index * 18}">${escapeHtml(
      String(item[labelKey] || "unknown")
    )} ${formatNumber(value)} (${Math.round((value / total) * 100)}%)</text>`;
    startAngle = endAngle;
  });

  if (items.length === 1) {
    const label = `${items[0][labelKey]} ${formatNumber(items[0][valueKey])} (100%)`;
    container.innerHTML = buildSvg(
      width,
      height,
      slices +
        `<text class="chart-title" x="${cx}" y="${cy}" text-anchor="middle">${escapeHtml(label)}</text>`
    );
    return;
  }

  container.innerHTML = buildSvg(width, height, slices + legend);
};

const renderHorizontalBarChart = (items, container, labelKey, valueKey, title = "", maxItems = 6) => {
  if (!items || !items.length) {
    container.innerHTML = '<span class="muted">No data</span>';
    return;
  }
  const list = items.slice(0, maxItems);
  const width = 260;
  const padding = 14;
  const labelWidth = 110;
  const valueWidth = 36;
  const barHeight = 16;
  const barGap = 10;
  const chartHeight = list.length * (barHeight + barGap) - barGap;
  const height = Math.max(140, chartHeight + padding * 2 + (title ? 14 : 0));
  const maxValue = Math.max(...list.map((item) => item[valueKey] || 0), 1);
  const barMaxWidth = width - padding * 2 - labelWidth - valueWidth;
  const startY = padding + (title ? 14 : 0);

  let bars = "";
  let labels = "";
  let valuesText = "";
  list.forEach((item, index) => {
    const value = item[valueKey] || 0;
    const barWidth = (value / maxValue) * barMaxWidth;
    const x = padding + labelWidth;
    const y = startY + index * (barHeight + barGap);
    const color = palette[index % palette.length];
    const rawLabel = String(item[labelKey] || "unknown");
    const shortLabel = rawLabel.length > 16 ? `${rawLabel.slice(0, 14)}…` : rawLabel;
    bars += `<rect x="${x}" y="${y}" width="${barMaxWidth}" height="${barHeight}" fill="#f3ebe3" rx="6" />`;
    bars += `<rect x="${x}" y="${y}" width="${barWidth}" height="${barHeight}" fill="${color}" rx="6" />`;
    labels += `<text class="chart-label" x="${padding}" y="${y + barHeight - 2}">${escapeHtml(
      shortLabel
    )}</text>`;
    valuesText += `<text class="chart-label" x="${width - padding}" y="${y + barHeight - 2}" text-anchor="end">${formatNumber(
      value
    )}</text>`;
  });

  const titleText = title
    ? `<text class="chart-title" x="${padding}" y="${12}">${escapeHtml(title)}</text>`
    : "";

  container.innerHTML = buildSvg(width, height, titleText + bars + labels + valuesText);
};

const renderHistogram = (items, container) => {
  if (!items || !items.length) {
    container.innerHTML = '<span class="muted">No data</span>';
    return;
  }
  const width = 260;
  const height = 200;
  const padding = 24;
  const chartWidth = width - padding * 2;
  const chartHeight = height - padding * 2;
  const sliced = items.slice(-8);

  const labels = sliced.map((item) => item.bucket);
  const values = sliced.map((item) => item.count || 0);
  const maxValue = Math.max(...values, 1);
  const barGap = 8;
  const barWidth = labels.length ? (chartWidth - barGap * (labels.length - 1)) / labels.length : 0;

  let bars = "";
  let xLabels = "";
  let valuesText = "";
  labels.forEach((label, index) => {
    const value = values[index];
    const barHeight = (value / maxValue) * chartHeight;
    const x = padding + index * (barWidth + barGap);
    const y = height - padding - barHeight;
    bars += `<rect x="${x}" y="${y}" width="${barWidth}" height="${barHeight}" fill="#7fb7be" rx="6" />`;
    valuesText += `<text class="chart-label" x="${x + barWidth / 2}" y="${
      y - 6
    }" text-anchor="middle">${formatNumber(value)}</text>`;
    xLabels += `<text class="chart-label" x="${x + barWidth / 2}" y="${
      height - 6
    }" text-anchor="middle">${escapeHtml(label.slice(11))}</text>`;
  });

  container.innerHTML = buildSvg(width, height, bars + valuesText + xLabels);
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
          <button class="action-btn" data-action="delete">Delete</button>
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
    .split(/[,\\s]+/)
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
  } else if (action === "delete") {
    await postAction("/api/crawl/delete", { job_id: jobId });
    state.jobs = state.jobs.filter((job) => job.job_id !== jobId);
    renderJobs();
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
    if (payload.type === "job_deleted") {
      state.jobs = state.jobs.filter((job) => job.job_id !== payload.job_id);
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

// ----- Reporting dashboard -----
const renderSessions = () => {
  sessionSelect.innerHTML = "";
  if (!state.reporting.sessions.length) {
    sessionSelect.innerHTML = '<option value="">No sessions</option>';
    return;
  }
  state.reporting.sessions.forEach((session, index) => {
    const option = document.createElement("option");
    option.value = session.session_id;
    option.textContent = `${session.session_id} · ${formatNumber(session.count)} docs`;
    if (index === 0 && !state.reporting.currentSessionId) {
      option.selected = true;
      state.reporting.currentSessionId = session.session_id;
    }
    if (session.session_id === state.reporting.currentSessionId) {
      option.selected = true;
    }
    sessionSelect.appendChild(option);
  });
};

const renderContentTypes = (contentTypes = []) => {
  if (!contentTypes.length) {
    contentTypeList.innerHTML = '<div class="muted">No data yet.</div>';
    return;
  }

  const total = contentTypes.reduce((sum, item) => sum + (item.count || 0), 0);
  contentTypeList.innerHTML = contentTypes
    .map((item) => {
      const pct = total > 0 ? Math.round((item.count / total) * 100) : 0;
      return `
        <div class="mini-card">
          <span>${escapeHtml(item.type || "unknown")}</span>
          <strong>${formatNumber(item.count)}</strong>
          <div class="bar"><div class="bar-fill" style="width:${pct}%"></div></div>
        </div>
      `;
    })
    .join("");
};

const renderChips = (items, container, labelKey, countKey) => {
  if (!items || !items.length) {
    container.innerHTML = '<span class="muted">No data</span>';
    return;
  }
  container.innerHTML = items
    .map(
      (item) =>
        `<span class="chip">${escapeHtml(item[labelKey] || "")}<strong>${formatNumber(
          item[countKey] || 0
        )}</strong></span>`
    )
    .join("");
};

const renderLatestItems = (items = []) => {
  if (!items.length) {
    latestItems.innerHTML = '<div class="muted">No recent documents.</div>';
    latestCount.textContent = "";
    return;
  }

  latestCount.textContent = `${items.length} shown`;
  latestItems.innerHTML = items
    .map(
      (item) => `
      <div class="timeline-item">
        <h5>${escapeHtml(item.title)}</h5>
        <div class="meta">${escapeHtml(item.content_type || "unknown")} · ${escapeHtml(item.url || "")}</div>
        <div class="muted">${escapeHtml(item.description || "").slice(0, 220)}</div>
        ${
          item.keywords && item.keywords.length
            ? `<div class="chips-list" style="margin-top:8px;">${item.keywords
                .slice(0, 6)
                .map((kw) => `<span class="chip">${escapeHtml(kw)}</span>`)
                .join("")}</div>`
            : ""
        }
      </div>
    `
    )
    .join("");
};

const renderSessionMeta = (summary) => {
  if (!summary) {
    sessionMeta.textContent = "Reporting unavailable";
    return;
  }

  const start = summary.time_window?.start ? formatDate(summary.time_window.start) : "n/a";
  const end = summary.time_window?.end ? formatDate(summary.time_window.end) : "n/a";
  sessionMeta.textContent = `Session ${summary.session_id} · ${formatNumber(
    summary.total_items
  )} docs · ${start} → ${end}`;
};

const loadSessions = async () => {
  try {
    sessionMeta.textContent = "Loading sessions…";
    const data = await fetchJson("/api/reports/sessions");
    state.reporting.sessions = data.sessions || [];
    renderSessions();
    const firstId = state.reporting.currentSessionId || (state.reporting.sessions[0] || {}).session_id;
    if (firstId) {
      await loadSessionSummary(firstId);
    } else {
      sessionMeta.textContent = "No crawl sessions found yet.";
    }
  } catch (error) {
    sessionMeta.textContent = `Reporting unavailable: ${error.message}`;
    contentTypeList.innerHTML = "";
    domainList.innerHTML = "";
    keywordList.innerHTML = "";
    latestItems.innerHTML = "";
  }
};

const loadSessionSummary = async (sessionId) => {
  if (!sessionId) return;
  try {
    sessionMeta.textContent = "Loading session data…";
    const query = sessionId ? `?session_id=${encodeURIComponent(sessionId)}` : "";
    const summary = await fetchJson(`/api/reports/session${query}`);
    state.reporting.currentSessionId = summary.session_id;
    state.reporting.summary = summary;

    renderSessionMeta(summary);
    renderChips(summary.top_domains, domainList, "domain", "count");
    renderChips(summary.top_topics, keywordList, "topic", "count");
    domainCount.textContent = `${summary.top_domains?.length || 0} domains`;
    keywordCount.textContent = `${summary.top_topics?.length || 0} topics`;
    renderLatestItems(summary.latest_items);
    reportOutput.textContent = "Ready. Add guidance and generate a report.";
    contentTypeChart.innerHTML = '<span class="muted">Generate a report to render charts.</span>';
    domainChart.innerHTML = '<span class="muted">Generate a report to render charts.</span>';
    keywordChart.innerHTML = '<span class="muted">Generate a report to render charts.</span>';
    timeChart.innerHTML = '<span class="muted">Generate a report to render charts.</span>';
    gqmPanel.innerHTML = '<span class="muted">Generate a report to render GQM metrics.</span>';
    pageSummary.textContent = "Select an article to analyze.";
    pageInsights.innerHTML = "";
    pageCharts.innerHTML = '<span class="muted">Select an article to analyze.</span>';
    populatePageSelect(summary.latest_items || []);
  } catch (error) {
    sessionMeta.textContent = `Error loading session: ${error.message}`;
    reportOutput.textContent = "Reporting unavailable.";
  }
};

const populatePageSelect = (items) => {
  pageSelect.innerHTML = "";
  if (!items || !items.length) {
    const option = document.createElement("option");
    option.value = "";
    option.textContent = "No pages";
    pageSelect.appendChild(option);
    return;
  }
  items.forEach((item) => {
    const option = document.createElement("option");
    option.value = item.url || "";
    option.textContent = item.title ? `${item.title.slice(0, 60)}` : item.url;
    pageSelect.appendChild(option);
  });
};

const runPageAnalysis = async () => {
  const url = pageSelect.value;
  if (!url) {
    pageSummary.textContent = "Select an article to analyze.";
    pageInsights.innerHTML = "";
    pageCharts.innerHTML = '<span class="muted">Select an article to analyze.</span>';
    return;
  }

  analyzePageBtn.disabled = true;
  pageSummary.textContent = "Analyzing page...";
  pageInsights.innerHTML = "";
  pageCharts.innerHTML = '<span class="muted">Analyzing page...</span>';

  try {
    const payload = {
      session_id: state.reporting.currentSessionId,
      url,
    };
    const data = await fetchJson("/api/reports/page", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    renderPageInsights(data.summary, data.insights || []);
    renderPageCharts(data.charts || []);
  } catch (error) {
    pageSummary.textContent = "Page analysis error.";
    pageInsights.innerHTML = `<span class="muted">${escapeHtml(error.message)}</span>`;
    pageCharts.innerHTML = "";
  } finally {
    analyzePageBtn.disabled = false;
  }
};

const runReport = async () => {
  if (!state.reporting.summary) {
    reportOutput.textContent = "Load a dataset first.";
    return;
  }

  reportOutput.textContent = "Generating report…";
  generateReportBtn.disabled = true;

  try {
    const sessionId = state.reporting.currentSessionId;
    const query = sessionId ? `?session_id=${encodeURIComponent(sessionId)}` : "";
    const summary = await fetchJson(`/api/reports/session${query}`);
    state.reporting.summary = summary;

    const payload = {
      session_id: sessionId,
      instructions: reportInstructions.value.trim(),
    };
    const data = await fetchJson("/api/reports/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    reportOutput.textContent = data.report || "LLM returned no content.";
    renderContentTypes(summary.content_types);
    renderPieChart(summary.content_types, contentTypeChart, "type", "count");
    renderHorizontalBarChart(summary.top_domains, domainChart, "domain", "count", "Domains");
    renderHorizontalBarChart(summary.top_topics, keywordChart, "topic", "count", "Topics");
    renderHistogram(summary.time_histogram || [], timeChart);
    renderGqm(summary.gqm);
  } catch (error) {
    reportOutput.textContent = `LLM error: ${error.message}`;
  } finally {
    generateReportBtn.disabled = false;
  }
};

refreshSessionsBtn.addEventListener("click", loadSessions);
sessionSelect.addEventListener("change", (event) => loadSessionSummary(event.target.value));
generateReportBtn.addEventListener("click", runReport);
analyzePageBtn.addEventListener("click", runPageAnalysis);

loadSessions();
