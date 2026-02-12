const AGENTS = [
  { stage: 1, name: "Analyst Agent", icon: "📋", desc: "Summarizes objective and translates to testable requirements" },
  { stage: 2, name: "Architect Agent", icon: "🏗️", desc: "Designs architecture with latency, security, and scale focus" },
  { stage: 3, name: "Developer Agent", icon: "💻", desc: "Creates development plan, then parallel implementation" },
  { stage: 4, name: "Database Engineer Agent", icon: "🗄️", desc: "Designs migration plans and executable database scripts" },
  { stage: 5, name: "Security Engineer Agent", icon: "🛡️", desc: "Performs threat modeling and security control recommendations" },
  { stage: 6, name: "Tester Agent", icon: "🧪", desc: "Runs executable unit/integration/load/security/E2E checks" },
  { stage: 7, name: "Analyst (Validation)", icon: "✅", desc: "Validates implementation against acceptance criteria" },
  { stage: 8, name: "Deployment Agent", icon: "🚀", desc: "Deploys locally or through cloud adapters" },
];

const STAGES = ["1", "2", "3", "4", "5", "6", "7", "8"];

const MODES = {
  HOME: "home",
  WORK: "work",
  TEAM: "team",
  HISTORY: "history",
};

const el = {
  navHome: document.getElementById("nav-home"),
  navWork: document.getElementById("nav-work"),
  navTeam: document.getElementById("nav-team"),
  navHistory: document.getElementById("nav-history"),
  homeWorkBtn: document.getElementById("home-work-btn"),
  homeTeamBtn: document.getElementById("home-team-btn"),
  homeHistoryBtn: document.getElementById("home-history-btn"),
  homeScreen: document.getElementById("home-screen"),
  workScreen: document.getElementById("work-screen"),
  teamScreen: document.getElementById("team-screen"),
  historyScreen: document.getElementById("history-screen"),

  workConfigPanel: document.getElementById("work-config-panel"),
  workIntakeStep: document.getElementById("work-intake-step"),
  workExecutionStep: document.getElementById("work-execution-step"),
  workRuntimePanels: document.getElementById("work-runtime-panels"),
  wizardContinue: document.getElementById("wizard-continue"),
  wizardBack: document.getElementById("wizard-back"),

  workTeamSelect: document.getElementById("work-team-select"),
  workSuggestTeam: document.getElementById("work-suggest-team"),
  workRefreshTeams: document.getElementById("work-refresh-teams"),
  workOpenTeamBuilder: document.getElementById("work-open-team-builder"),
  workApplyTeam: document.getElementById("work-apply-team"),
  workTeamReason: document.getElementById("work-team-reason"),
  workTeamRoster: document.getElementById("work-team-roster"),
  workOpenHistory: document.getElementById("work-open-history"),

  teamStageSelectors: document.getElementById("team-stage-selectors"),
  teamName: document.getElementById("team-name"),
  teamDescription: document.getElementById("team-description"),
  teamSaveBtn: document.getElementById("team-save-btn"),
  teamUseInWorkBtn: document.getElementById("team-use-in-work-btn"),
  teamRefreshBtn: document.getElementById("team-refresh-btn"),
  teamSaveMessage: document.getElementById("team-save-message"),
  cloneBaseAgent: document.getElementById("clone-base-agent"),
  cloneAgentName: document.getElementById("clone-agent-name"),
  cloneAgentPersona: document.getElementById("clone-agent-persona"),
  cloneAgentBtn: document.getElementById("clone-agent-btn"),
  cloneAgentMessage: document.getElementById("clone-agent-message"),
  teamAgentCatalog: document.getElementById("team-agent-catalog"),

  tasksRefresh: document.getElementById("tasks-refresh"),
  tasksList: document.getElementById("tasks-list"),

  provider: document.getElementById("provider"),
  model: document.getElementById("model"),
  apiKey: document.getElementById("api-key"),
  temperature: document.getElementById("temperature"),
  parallelAgents: document.getElementById("parallel-agents"),
  maxRetries: document.getElementById("max-retries"),
  taskType: document.getElementById("task-type"),
  modernizationPanel: document.getElementById("modernization-panel"),
  modernizationLanguage: document.getElementById("modernization-language"),
  databasePanel: document.getElementById("database-panel"),
  dbSource: document.getElementById("db-source"),
  dbTarget: document.getElementById("db-target"),
  dbSchema: document.getElementById("db-schema"),
  dbFile: document.getElementById("db-file"),
  uploadDb: document.getElementById("upload-db"),
  humanApproval: document.getElementById("human-approval"),
  strictSecurityMode: document.getElementById("strict-security-mode"),
  liveDeploy: document.getElementById("live-deploy"),
  deploymentTarget: document.getElementById("deployment-target"),
  cloudConfigBox: document.getElementById("cloud-config-box"),
  cloudPlatform: document.getElementById("cloud-platform"),
  cloudRegion: document.getElementById("cloud-region"),
  cloudServiceName: document.getElementById("cloud-service-name"),
  cloudProjectId: document.getElementById("cloud-project-id"),
  cloudResourceGroup: document.getElementById("cloud-resource-group"),
  cloudSubscriptionId: document.getElementById("cloud-subscription-id"),
  cloudPower: document.getElementById("cloud-power"),
  cloudScale: document.getElementById("cloud-scale"),
  cloudCredentials: document.getElementById("cloud-credentials"),
  cloudExtra: document.getElementById("cloud-extra"),
  clusterName: document.getElementById("cluster-name"),
  namespace: document.getElementById("namespace"),
  deployOutputDir: document.getElementById("deploy-output-dir"),
  objectives: document.getElementById("objectives"),
  objectivesFile: document.getElementById("objectives-file"),
  uploadObjectives: document.getElementById("upload-objectives"),
  legacyCode: document.getElementById("legacy-code"),
  legacyFile: document.getElementById("legacy-file"),
  uploadLegacy: document.getElementById("upload-legacy"),
  runPipeline: document.getElementById("run-pipeline"),
  taskSummaryCard: document.getElementById("task-summary-card"),
  taskSummaryUseCase: document.getElementById("task-summary-use-case"),
  taskSummaryObjective: document.getElementById("task-summary-objective"),
  taskSummaryDetails: document.getElementById("task-summary-details"),

  statusChips: document.getElementById("status-chips"),
  progressFill: document.getElementById("progress-fill"),
  progressMeta: document.getElementById("progress-meta"),
  pipelineStatusText: document.getElementById("pipeline-status-text"),
  retryPlanSection: document.getElementById("retry-plan-section"),
  retryPlanStatus: document.getElementById("retry-plan-status"),
  retryPlanContent: document.getElementById("retry-plan-content"),
  currentAgentPanel: document.getElementById("current-agent-panel"),
  agentTabs: document.getElementById("agent-tabs"),
  agentTabPanel: document.getElementById("agent-tab-panel"),
  liveLogs: document.getElementById("live-logs"),
  flowDiagramSection: document.getElementById("flow-diagram-section"),

  approvalPanel: document.getElementById("approval-panel"),
  approvalTitle: document.getElementById("approval-title"),
  approvalMessage: document.getElementById("approval-message"),
  approvalDeveloperOptions: document.getElementById("approval-developer-options"),
  approvalCloudOptions: document.getElementById("approval-cloud-options"),
  approveMicroservicesCount: document.getElementById("approve-microservices-count"),
  approveSplitStrategy: document.getElementById("approve-split-strategy"),
  approveTargetLanguage: document.getElementById("approve-target-language"),
  approveTargetPlatform: document.getElementById("approve-target-platform"),
  approveCloudPlatform: document.getElementById("approve-cloud-platform"),
  approveCloudRegion: document.getElementById("approve-cloud-region"),
  approveCloudServiceName: document.getElementById("approve-cloud-service-name"),
  approveCloudProjectId: document.getElementById("approve-cloud-project-id"),
  approveCloudResourceGroup: document.getElementById("approve-cloud-resource-group"),
  approveCloudSubscriptionId: document.getElementById("approve-cloud-subscription-id"),
  approveCloudPower: document.getElementById("approve-cloud-power"),
  approveCloudScale: document.getElementById("approve-cloud-scale"),
  approveCloudCredentials: document.getElementById("approve-cloud-credentials"),
  approveCloudExtra: document.getElementById("approve-cloud-extra"),
  approveStage: document.getElementById("approve-stage"),
  rejectStage: document.getElementById("reject-stage"),

  runHistory: document.getElementById("run-history"),
  loadRun: document.getElementById("load-run"),
  refreshRunHistory: document.getElementById("refresh-run-history"),
  artifactSelect: document.getElementById("artifact-select"),
  artifactPreview: document.getElementById("artifact-preview"),
  refreshArtifacts: document.getElementById("refresh-artifacts"),
  viewArtifact: document.getElementById("view-artifact"),

  outputModal: document.getElementById("output-modal"),
  closeModal: document.getElementById("close-modal"),
  modalTitle: document.getElementById("modal-title"),
  modalSummary: document.getElementById("modal-summary"),
  modalReadable: document.getElementById("modal-readable"),
  modalLogs: document.getElementById("modal-logs"),
  modalOutput: document.getElementById("modal-output"),
};

const state = {
  mode: MODES.HOME,
  wizardStep: 1,
  currentRunId: "",
  currentRun: null,
  eventSource: null,
  selectedStage: 1,
  artifacts: [],
  teams: [],
  agents: { premade: [], custom: [], all: [], by_stage: {} },
  tasks: [],
  teamSelection: {
    teamId: "",
    teamName: "",
    description: "",
    stageAgentIds: {},
    agentPersonas: {},
    reason: "",
  },
  teamBuilder: {
    stageAgentIds: {},
  },
};

let mermaidInitialized = false;

async function api(path, payload, method = "POST") {
  const options = {
    method: payload ? method : "GET",
    headers: payload ? { "Content-Type": "application/json" } : undefined,
    body: payload ? JSON.stringify(payload) : undefined,
  };
  const res = await fetch(path, options);
  const data = await res.json().catch(() => ({}));
  if (!res.ok || data.ok === false) {
    throw new Error(data.error || `HTTP ${res.status}`);
  }
  return data;
}

function escapeHtml(value) {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function parseKeyValueLines(text) {
  const out = {};
  String(text || "")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .forEach((line) => {
      const idx = line.indexOf("=");
      if (idx <= 0) return;
      const key = line.slice(0, idx).trim();
      const value = line.slice(idx + 1).trim();
      if (key) out[key] = value;
    });
  return out;
}

function setDefaultModelByProvider() {
  el.model.value = el.provider.value === "anthropic" ? "claude-sonnet-4-20250514" : "gpt-4o";
}

function currentUseCase() {
  return String(el.taskType?.value || "business_objectives");
}

function useCaseLabel(useCase) {
  if (useCase === "code_modernization") return "Code Modernization";
  if (useCase === "database_conversion") return "Database Conversion";
  return "Business Challenge";
}

function isCodeModernizationMode() {
  return currentUseCase() === "code_modernization";
}

function isDatabaseConversionMode() {
  return currentUseCase() === "database_conversion";
}

function buildModernizationObjective(language) {
  const target = String(language || "Python").trim() || "Python";
  return (
    `Modernize the provided legacy codebase into ${target}. ` +
    `Document current functionality and acceptance criteria, design architecture, implement rewritten services, ` +
    `run QA/security checks, validate requirements, and deploy for local Docker testing.`
  );
}

function buildDatabaseConversionObjective(sourceDb, targetDb) {
  const source = String(sourceDb || "legacy database").trim() || "legacy database";
  const target = String(targetDb || "target database").trim() || "target database";
  return (
    `Convert database schema and migration logic from ${source} to ${target}. ` +
    `Produce migration scripts, data validation checks, rollback scripts, security checks, and deploy migration-ready artifacts.`
  );
}

function setAutogeneratedObjective() {
  if (currentUseCase() === "code_modernization") {
    el.objectives.value = buildModernizationObjective(el.modernizationLanguage.value);
    el.objectives.dataset.autogen = "1";
    return;
  }
  if (currentUseCase() === "database_conversion") {
    el.objectives.value = buildDatabaseConversionObjective(el.dbSource.value, el.dbTarget.value);
    el.objectives.dataset.autogen = "1";
    return;
  }
}

function toggleUseCasePanel() {
  const codeMode = isCodeModernizationMode();
  const dbMode = isDatabaseConversionMode();

  el.modernizationPanel.classList.toggle("hidden", !codeMode);
  el.databasePanel.classList.toggle("hidden", !dbMode);

  if ((codeMode || dbMode) && ((el.objectives.value || "").trim() === "" || el.objectives.dataset.autogen === "1")) {
    setAutogeneratedObjective();
  }
}

function toggleCloudConfig() {
  el.cloudConfigBox.classList.toggle("hidden", el.deploymentTarget.value !== "cloud");
}

function setWizardStep(step) {
  state.wizardStep = step;
  const step1 = step === 1;
  el.workIntakeStep.classList.toggle("hidden", !step1);
  el.workExecutionStep.classList.toggle("hidden", step1);
  el.workConfigPanel.classList.toggle("hidden", step1);
  el.workRuntimePanels.classList.toggle("hidden", step1);
  if (step1) {
    el.workScreen.classList.remove("xl:grid-cols-[390px_1fr]");
    el.workScreen.classList.add("xl:grid-cols-1");
  } else {
    el.workScreen.classList.add("xl:grid-cols-[390px_1fr]");
    el.workScreen.classList.remove("xl:grid-cols-1");
  }
  renderTaskSummary();
}

function encodeMermaid(value) {
  try {
    return btoa(unescape(encodeURIComponent(String(value || ""))));
  } catch (_err) {
    return "";
  }
}

function decodeMermaid(value) {
  try {
    return decodeURIComponent(escape(atob(String(value || ""))));
  } catch (_err) {
    return "";
  }
}

function ensureMermaid() {
  if (mermaidInitialized) return;
  if (!window.mermaid) return;
  window.mermaid.initialize({ startOnLoad: false, securityLevel: "loose", theme: "neutral" });
  mermaidInitialized = true;
}

function mermaidBlock(title, diagram) {
  const source = String(diagram || "").trim();
  if (!source) return "";
  const b64 = encodeMermaid(source);
  if (!b64) return "";
  return `
    <div class="mt-2"><strong>${escapeHtml(title)}</strong></div>
    <div class="mt-1 overflow-auto rounded-lg border border-slate-300 bg-white p-2">
      <div data-mermaid-b64="${b64}" class="min-w-[320px]"></div>
    </div>
  `;
}

async function renderMermaidBlocks(scope = document) {
  ensureMermaid();
  if (!window.mermaid) return;
  const hosts = scope.querySelectorAll("[data-mermaid-b64]");
  for (let i = 0; i < hosts.length; i += 1) {
    const host = hosts[i];
    const b64 = host.getAttribute("data-mermaid-b64") || "";
    const source = decodeMermaid(b64);
    if (!source.trim() || host.getAttribute("data-rendered") === "1") continue;
    try {
      const id = `mmd-${Date.now()}-${i}-${Math.floor(Math.random() * 100000)}`;
      const result = await window.mermaid.render(id, source);
      host.innerHTML = result.svg;
      host.setAttribute("data-rendered", "1");
    } catch (_err) {
      host.innerHTML = `<pre class="mono text-[11px] text-slate-700">${escapeHtml(source)}</pre><p class="mt-1 text-[11px] text-rose-700">Diagram render failed</p>`;
      host.setAttribute("data-rendered", "1");
    }
  }
}

function stageAgentLookup(stage, agentId) {
  const options = state.agents.by_stage?.[String(stage)] || [];
  return options.find((a) => String(a.id) === String(agentId)) || null;
}

function derivePersonasFromStageMap(stageAgentIds) {
  const personas = {};
  STAGES.forEach((stage) => {
    const agent = stageAgentLookup(stage, stageAgentIds?.[stage] || "");
    personas[stage] = {
      agent_id: agent?.id || "",
      display_name: agent?.display_name || "",
      persona: agent?.persona || "",
    };
  });
  return personas;
}

function applyTeamSelection(team, personas, reason = "") {
  const stageAgentIds = (team?.stage_agent_ids && typeof team.stage_agent_ids === "object") ? team.stage_agent_ids : {};
  state.teamSelection = {
    teamId: String(team?.id || ""),
    teamName: String(team?.name || "Ad-hoc Team"),
    description: String(team?.description || ""),
    stageAgentIds,
    agentPersonas: personas && typeof personas === "object" ? personas : derivePersonasFromStageMap(stageAgentIds),
    reason: String(reason || ""),
  };
  state.teamBuilder.stageAgentIds = { ...stageAgentIds };
  renderWorkTeamSelection();
  renderTeamBuilderSelectors();
  renderTaskSummary();
}

function defaultBuilderMap() {
  const out = {};
  STAGES.forEach((stage) => {
    const options = state.agents.by_stage?.[stage] || [];
    out[stage] = options[0]?.id || "";
  });
  return out;
}

function renderWorkTeamSelection() {
  const s = state.teamSelection;
  if (el.workTeamSelect && s.teamId) {
    const exists = [...el.workTeamSelect.options].some((o) => o.value === s.teamId);
    if (exists) el.workTeamSelect.value = s.teamId;
  }
  el.workTeamReason.textContent = s.reason ? `Suggested rationale: ${s.reason}` : `Active team: ${s.teamName || "(none)"}`;

  el.workTeamRoster.innerHTML = STAGES.map((stage) => {
    const persona = s.agentPersonas?.[stage] || {};
    const stageName = AGENTS.find((a) => a.stage === Number(stage))?.name || `Stage ${stage}`;
    return `
      <div class="rounded-lg border border-slate-300 bg-white p-2 text-xs text-slate-800">
        <div class="font-semibold text-slate-900">Stage ${stage}: ${escapeHtml(stageName)}</div>
        <div class="mt-1 text-slate-900">${escapeHtml(persona.display_name || "Unassigned")}</div>
        <div class="mt-1 text-slate-700">${escapeHtml(persona.persona || "")}</div>
      </div>
    `;
  }).join("");
}

function stageSelectorHtml(stage, selectedId) {
  const stageInfo = AGENTS.find((a) => a.stage === Number(stage));
  const options = state.agents.by_stage?.[String(stage)] || [];
  const selectOptions = options
    .map((a) => `<option value="${escapeHtml(a.id)}" ${String(a.id) === String(selectedId) ? "selected" : ""}>${escapeHtml(a.display_name)}${a.is_custom ? " (custom)" : ""}</option>`)
    .join("");
  const current = options.find((o) => String(o.id) === String(selectedId)) || options[0] || {};
  return `
    <div class="rounded-lg border border-slate-300 bg-slate-50 p-3" data-stage="${stage}">
      <label class="mb-1 block text-xs font-semibold uppercase tracking-[0.14em] text-slate-700">Stage ${stage}: ${escapeHtml(stageInfo?.name || "")}</label>
      <select data-team-stage="${stage}" class="w-full rounded-md border border-slate-300 bg-white px-2 py-2 text-xs text-slate-900">${selectOptions}</select>
      <p class="mt-2 text-xs text-slate-700" data-team-persona="${stage}">${escapeHtml(current.persona || "")}</p>
    </div>
  `;
}

function renderTeamBuilderSelectors() {
  if (!state.teamBuilder.stageAgentIds || !Object.keys(state.teamBuilder.stageAgentIds).length) {
    state.teamBuilder.stageAgentIds = defaultBuilderMap();
  }
  el.teamStageSelectors.innerHTML = STAGES.map((stage) => stageSelectorHtml(stage, state.teamBuilder.stageAgentIds[stage])).join("");
  el.teamStageSelectors.querySelectorAll("[data-team-stage]").forEach((node) => {
    node.addEventListener("change", () => {
      const stage = String(node.getAttribute("data-team-stage"));
      state.teamBuilder.stageAgentIds[stage] = node.value;
      const selected = stageAgentLookup(stage, node.value);
      const personaBox = el.teamStageSelectors.querySelector(`[data-team-persona=\"${stage}\"]`);
      if (personaBox) personaBox.textContent = selected?.persona || "";
    });
  });
}

function renderAgentCatalog() {
  const all = state.agents.all || [];
  el.teamAgentCatalog.innerHTML = all.length ? all.map((agent) => `
    <div class="mb-2 rounded-md border border-slate-200 p-2">
      <div class="flex items-center justify-between gap-2">
        <p class="text-xs font-semibold text-slate-900">${escapeHtml(agent.display_name || agent.role || "Agent")}</p>
        <span class="rounded border border-slate-300 bg-slate-100 px-1.5 py-0.5 text-[10px] text-slate-700">S${escapeHtml(agent.stage)}</span>
      </div>
      <p class="mt-1 text-[11px] text-slate-700">${escapeHtml(agent.persona || "")}</p>
    </div>
  `).join("") : "<p class='text-xs text-slate-700'>No agents available.</p>";

  el.cloneBaseAgent.innerHTML = all.map((agent) => `<option value="${escapeHtml(agent.id)}">S${agent.stage} | ${escapeHtml(agent.display_name || agent.role || agent.id)}</option>`).join("");
}

function renderTeamsDropdown() {
  const teams = state.teams || [];
  if (!teams.length) {
    el.workTeamSelect.innerHTML = `<option value="">No teams available</option>`;
    return;
  }
  el.workTeamSelect.innerHTML = teams.map((team) => `<option value="${escapeHtml(team.id)}">${escapeHtml(team.name)}${team.is_custom ? " (custom)" : ""}</option>`).join("");
  const selected = state.teamSelection.teamId || teams[0].id;
  el.workTeamSelect.value = selected;
}

function toModeButtonState(mode) {
  const map = { [MODES.HOME]: el.navHome, [MODES.WORK]: el.navWork, [MODES.TEAM]: el.navTeam, [MODES.HISTORY]: el.navHistory };
  Object.values(map).forEach((btn) => btn?.classList.remove("mode-btn-active"));
  map[mode]?.classList.add("mode-btn-active");
}

function setMode(mode) {
  state.mode = mode;
  el.homeScreen.classList.toggle("hidden", mode !== MODES.HOME);
  el.workScreen.classList.toggle("hidden", mode !== MODES.WORK);
  el.teamScreen.classList.toggle("hidden", mode !== MODES.TEAM);
  el.historyScreen.classList.toggle("hidden", mode !== MODES.HISTORY);
  toModeButtonState(mode);
  if (mode === MODES.HISTORY) refreshTasks().catch(() => {});
}

async function loadAgentsAndTeams() {
  const [agentData, teamData] = await Promise.all([api("/api/agents", null), api("/api/teams", null)]);
  state.agents = {
    premade: agentData.premade || [],
    custom: agentData.custom || [],
    all: agentData.all || [],
    by_stage: agentData.by_stage || {},
  };
  state.teams = teamData.teams || [];
  renderAgentCatalog();
  renderTeamsDropdown();

  if (!state.teamSelection.teamId && state.teams.length) {
    const first = state.teams[0];
    applyTeamSelection(first, derivePersonasFromStageMap(first.stage_agent_ids || {}));
  } else {
    renderWorkTeamSelection();
  }

  if (!state.teamBuilder.stageAgentIds || !Object.keys(state.teamBuilder.stageAgentIds).length) {
    state.teamBuilder.stageAgentIds = state.teamSelection.stageAgentIds && Object.keys(state.teamSelection.stageAgentIds).length
      ? { ...state.teamSelection.stageAgentIds }
      : defaultBuilderMap();
  }
  renderTeamBuilderSelectors();
}

async function applySelectedTeamFromDropdown() {
  const teamId = String(el.workTeamSelect.value || "").trim();
  if (!teamId) return;
  const data = await api(`/api/teams/${encodeURIComponent(teamId)}`, null);
  applyTeamSelection(data.team || {}, data.agent_personas || {}, "");
}

async function suggestTeamFromObjectives() {
  const challenge = String(el.objectives.value || "").trim();
  if (!challenge) {
    alert("Provide the business challenge first so Synthetix can suggest a team.");
    return;
  }
  const data = await api("/api/teams/suggest", { challenge });
  applyTeamSelection(data.team || {}, data.agent_personas || {}, data.suggestion?.reason || "");
  if (data.team?.id) el.workTeamSelect.value = data.team.id;
}

async function saveTeamFromBuilder() {
  const name = String(el.teamName.value || "").trim();
  if (!name) {
    alert("Team name is required.");
    return;
  }
  const data = await api("/api/teams", {
    name,
    description: String(el.teamDescription.value || "").trim(),
    stage_agent_ids: { ...state.teamBuilder.stageAgentIds },
  });
  el.teamSaveMessage.textContent = `Saved team: ${data.team?.name || "(unnamed)"}`;
  applyTeamSelection(data.team || {}, data.agent_personas || {}, "Saved from team builder");
  await loadAgentsAndTeams();
}

async function cloneAgentFromBuilder() {
  const baseAgentId = String(el.cloneBaseAgent.value || "").trim();
  if (!baseAgentId) {
    alert("Choose a base agent to clone.");
    return;
  }
  const data = await api("/api/agents/clone", {
    base_agent_id: baseAgentId,
    display_name: String(el.cloneAgentName.value || "").trim(),
    persona: String(el.cloneAgentPersona.value || "").trim(),
  });
  el.cloneAgentMessage.textContent = `Cloned ${data.agent?.display_name || "agent"}`;
  el.cloneAgentName.value = "";
  el.cloneAgentPersona.value = "";
  await loadAgentsAndTeams();
}

function useBuilderTeamInWork() {
  const stageAgentIds = { ...state.teamBuilder.stageAgentIds };
  applyTeamSelection(
    { id: "", name: String(el.teamName.value || "Ad-hoc Team").trim() || "Ad-hoc Team", description: String(el.teamDescription.value || "").trim(), stage_agent_ids: stageAgentIds },
    derivePersonasFromStageMap(stageAgentIds),
    "Using ad-hoc team from Team Builder"
  );
  setMode(MODES.WORK);
}

async function refreshTasks() {
  const data = await api("/api/tasks", null);
  state.tasks = data.tasks || [];
  renderTasks();
}

function renderTasks() {
  const tasks = state.tasks || [];
  if (!tasks.length) {
    el.tasksList.innerHTML = "<div class='rounded-lg border border-slate-300 bg-slate-50 p-3 text-xs text-slate-700'>No previous tasks yet.</div>";
    return;
  }
  el.tasksList.innerHTML = tasks.map((task) => {
    const created = String(task.created_at || "").replace("T", " ").slice(0, 19);
    return `
      <div class="rounded-lg border border-slate-300 bg-white p-3">
        <div class="flex flex-wrap items-center justify-between gap-2">
          <div>
            <p class="text-sm font-semibold text-slate-900">${escapeHtml(task.team_name || "Unspecified Team")} | ${escapeHtml((task.status || "").toUpperCase())}</p>
            <p class="mono text-[11px] text-slate-600">${escapeHtml(task.run_id || "")}</p>
          </div>
          <div class="flex gap-2">
            <button data-open-run="${escapeHtml(task.run_id)}" class="btn-light rounded-md px-2 py-1 text-xs font-semibold">Open</button>
            <button data-clone-task="${escapeHtml(task.run_id)}" class="btn-dark rounded-md px-2 py-1 text-xs font-semibold">Clone & Reuse</button>
          </div>
        </div>
        <p class="mt-2 text-xs text-slate-700">${escapeHtml(task.objective_preview || "")}</p>
        <p class="mt-1 text-[11px] text-slate-500">Created: ${escapeHtml(created || "n/a")} | Use case: ${escapeHtml(task.use_case || "")}</p>
      </div>
    `;
  }).join("");

  el.tasksList.querySelectorAll("[data-open-run]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const runId = btn.getAttribute("data-open-run") || "";
      if (!runId) return;
      state.currentRunId = runId;
      await syncRun(runId);
      setMode(MODES.WORK);
      setWizardStep(2);
    });
  });

  el.tasksList.querySelectorAll("[data-clone-task]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const runId = btn.getAttribute("data-clone-task") || "";
      if (!runId) return;
      await cloneTaskToWorkbench(runId);
    });
  });
}

async function cloneTaskToWorkbench(runId) {
  const data = await api(`/api/tasks/${encodeURIComponent(runId)}/clone`, null);
  const t = data.template || {};
  const cfg = t.config_hints || {};

  if (cfg.provider) el.provider.value = cfg.provider;
  setDefaultModelByProvider();
  if (cfg.model) el.model.value = cfg.model;
  el.temperature.value = Number(cfg.temperature ?? el.temperature.value);
  el.parallelAgents.value = Number(cfg.parallel_agents ?? el.parallelAgents.value);
  el.maxRetries.value = Number(cfg.max_retries ?? el.maxRetries.value);
  el.liveDeploy.checked = !!cfg.live_deploy;
  if (cfg.cluster_name) el.clusterName.value = cfg.cluster_name;
  if (cfg.namespace) el.namespace.value = cfg.namespace;

  el.objectives.value = t.objectives || "";
  el.taskType.value = t.use_case || "business_objectives";
  el.legacyCode.value = t.legacy_code || "";
  el.modernizationLanguage.value = t.modernization_language || el.modernizationLanguage.value;
  el.dbSource.value = t.database_source || "";
  el.dbTarget.value = t.database_target || "";
  el.dbSchema.value = t.database_schema || "";
  el.deploymentTarget.value = t.deployment_target || "local";
  el.humanApproval.checked = !!t.human_approval;
  el.strictSecurityMode.checked = !!t.strict_security_mode;

  const cloud = t.cloud_config || {};
  el.cloudPlatform.value = cloud.platform || "";
  el.cloudRegion.value = cloud.region || "";
  el.cloudServiceName.value = cloud.service_name || "";
  el.cloudProjectId.value = cloud.project_id || "";
  el.cloudResourceGroup.value = cloud.resource_group || "";
  el.cloudSubscriptionId.value = cloud.subscription_id || "";
  el.cloudPower.value = cloud.power || "";
  el.cloudScale.value = cloud.scale || "";
  el.cloudCredentials.value = cloud.credentials || "";
  el.cloudExtra.value = Object.entries(cloud.extra || {}).map(([k, v]) => `${k}=${v}`).join("\n");

  toggleUseCasePanel();
  toggleCloudConfig();

  if (t.team_id) {
    try {
      const teamData = await api(`/api/teams/${encodeURIComponent(t.team_id)}`, null);
      applyTeamSelection(teamData.team || {}, teamData.agent_personas || {}, "Cloned from previous task");
      el.workTeamSelect.value = t.team_id;
    } catch (_err) {
      const stageMap = t.stage_agent_ids || {};
      applyTeamSelection({ id: "", name: "Cloned Ad-hoc Team", description: "Derived from previous task", stage_agent_ids: stageMap }, derivePersonasFromStageMap(stageMap), "Cloned from previous task");
    }
  }

  setMode(MODES.WORK);
  setWizardStep(1);
}

function runStatusTone(status) {
  if (status === "running") return "border-sky-400 bg-sky-100 text-sky-900";
  if (status === "waiting_approval") return "border-amber-400 bg-amber-100 text-amber-900";
  if (status === "completed") return "border-emerald-400 bg-emerald-100 text-emerald-900";
  if (status === "failed") return "border-rose-400 bg-rose-100 text-rose-900";
  return "border-slate-300 bg-slate-100 text-slate-700";
}

function renderTaskSummary() {
  if (!el.taskSummaryCard) return;

  const run = state.currentRun;
  const pipeline = run?.pipeline_state || {};
  const useCase = String(pipeline.use_case || currentUseCase());
  const objective = String(pipeline.business_objectives || el.objectives.value || "").trim();
  const teamName = run?.team_name || pipeline.team_name || state.teamSelection.teamName || "Ad-hoc Team";
  const deploymentTarget = String(pipeline.deployment_target || el.deploymentTarget.value || "local").toLowerCase();
  const humanApproval = Boolean(run?.human_approval ?? pipeline.human_approval ?? el.humanApproval.checked);
  const strictSecurity = Boolean(run?.strict_security_mode ?? pipeline.strict_security_mode ?? el.strictSecurityMode.checked);
  const parallelAgents = Number(pipeline.parallel_agents || el.parallelAgents.value || 5);
  const maxRetries = Number(pipeline.max_retries || el.maxRetries.value || 2);
  const provider = String(pipeline.provider || el.provider.value || "");
  const model = String(pipeline.model || el.model.value || "");

  const detailItems = [
    { label: "Team", value: teamName },
    { label: "Deployment", value: deploymentTarget === "cloud" ? "Cloud" : "Local Docker" },
    { label: "Human Approval", value: humanApproval ? "Enabled" : "Disabled" },
    { label: "Strict Security", value: strictSecurity ? "Enabled" : "Disabled" },
    { label: "Parallel Sub-agents", value: String(parallelAgents) },
    { label: "Max Retries", value: String(maxRetries) },
  ];

  if (provider || model) {
    detailItems.push({ label: "LLM", value: [provider, model].filter(Boolean).join(" / ") });
  }

  if (useCase === "code_modernization") {
    const lang = String(pipeline.modernization_language || el.modernizationLanguage.value || "Python");
    const legacyCode = String(pipeline.legacy_code || el.legacyCode.value || "");
    detailItems.push({ label: "Target Language", value: lang });
    detailItems.push({ label: "Legacy Code", value: `${legacyCode ? legacyCode.split("\n").length : 0} lines` });
  } else if (useCase === "database_conversion") {
    const source = String(pipeline.database_source || el.dbSource.value || "Not set");
    const target = String(pipeline.database_target || el.dbTarget.value || "Not set");
    const schema = String(pipeline.database_schema || el.dbSchema.value || "");
    detailItems.push({ label: "DB Source", value: source });
    detailItems.push({ label: "DB Target", value: target });
    detailItems.push({ label: "Schema Input", value: `${schema ? schema.split("\n").length : 0} lines` });
  }

  el.taskSummaryUseCase.textContent = useCaseLabel(useCase).toUpperCase();
  el.taskSummaryObjective.textContent = objective || "No objective provided yet.";
  el.taskSummaryDetails.innerHTML = detailItems
    .map((item) => `<div class="rounded-lg border border-slate-300 bg-white px-2 py-1.5"><span class="font-semibold text-slate-900">${escapeHtml(item.label)}:</span> ${escapeHtml(item.value)}</div>`)
    .join("");
}

function renderStatusChips() {
  const run = state.currentRun;
  const status = run?.status || "idle";
  const runId = run?.run_id || "-";
  const stage = run?.current_stage || 0;
  const retries = run?.retry_count || 0;
  const teamName = run?.team_name || run?.pipeline_state?.team_name || state.teamSelection.teamName || "-";

  el.statusChips.innerHTML = `
    <div class="rounded-lg border px-3 py-2 ${runStatusTone(status)}">
      <div class="flex items-center justify-between gap-2"><span>Run</span><span>${status.toUpperCase()}</span></div>
      <p class="mt-1 mono text-[10px]">status</p>
    </div>
    <div class="rounded-lg border border-slate-300 bg-white px-3 py-2 text-slate-700">
      <div class="flex items-center justify-between gap-2"><span>Stage</span><span>${stage}/${AGENTS.length}</span></div>
      <p class="mt-1 mono text-[10px]">progress</p>
    </div>
    <div class="rounded-lg border border-slate-300 bg-white px-3 py-2 text-slate-700">
      <div class="flex items-center justify-between gap-2"><span>Retries</span><span>${retries}</span></div>
      <p class="mt-1 mono text-[10px]">dev↔test</p>
    </div>
    <div class="rounded-lg border border-slate-300 bg-white px-3 py-2 text-slate-700">
      <div class="flex items-center justify-between gap-2"><span>Synthetix</span><span class="text-[10px]">${escapeHtml(teamName.slice(0, 18))}</span></div>
      <p class="mt-1 mono text-[10px]">${escapeHtml(runId.slice(0, 12))}</p>
    </div>
  `;
}

function latestResultByStage(run, stage) {
  const results = run?.pipeline_state?.agent_results || [];
  const filtered = results.filter((r) => Number(r.stage) === Number(stage));
  return filtered.length ? filtered[filtered.length - 1] : null;
}

function statusLabel(status) {
  if (status === "success") return "SUCCESS";
  if (status === "warning") return "WARNING";
  if (status === "running") return "RUNNING";
  if (status === "waiting_approval") return "WAITING APPROVAL";
  if (status === "error") return "ERROR";
  return "PENDING";
}

function statusTone(status) {
  if (status === "success") return "bg-emerald-100 text-emerald-900 border-emerald-300";
  if (status === "warning") return "bg-amber-100 text-amber-900 border-amber-300";
  if (status === "running") return "bg-sky-100 text-sky-900 border-sky-300";
  if (status === "waiting_approval") return "bg-amber-100 text-amber-900 border-amber-300";
  if (status === "error") return "bg-rose-100 text-rose-900 border-rose-300";
  return "bg-slate-100 text-slate-700 border-slate-300";
}

function determineCurrentStage(run) {
  const stageStatus = run?.stage_status || {};
  for (const agent of AGENTS) {
    const s = stageStatus[agent.stage];
    if (s === "running" || s === "waiting_approval") return agent.stage;
  }
  const pending = AGENTS.find((a) => !stageStatus[a.stage] || stageStatus[a.stage] === "pending");
  return pending ? pending.stage : Number(run?.current_stage || 1);
}

function renderProgress() {
  const stageStatus = state.currentRun?.stage_status || {};
  const statuses = Object.values(stageStatus);
  const completed = statuses.filter((s) => s === "success" || s === "warning").length;
  const pct = Math.round((completed / AGENTS.length) * 100);
  el.progressFill.style.width = `${pct}%`;
  el.progressMeta.textContent = `${completed} / ${AGENTS.length} stages complete`;
  el.pipelineStatusText.textContent = (state.currentRun?.status || "idle").toUpperCase();
}

function renderRetryPlan() {
  const run = state.currentRun;
  const plan = run?.pipeline_state?.retry_plan || null;
  const history = Array.isArray(run?.pipeline_state?.retry_history) ? run.pipeline_state.retry_history : [];
  if (!plan || typeof plan !== "object") {
    el.retryPlanSection.classList.add("hidden");
    return;
  }
  el.retryPlanSection.classList.remove("hidden");
  const status = String(plan.status || "pending").toUpperCase();
  el.retryPlanStatus.textContent = status;
  el.retryPlanStatus.className = "rounded-md border bg-white px-2 py-0.5 text-[11px] font-semibold " + (status === "APPLIED" ? "border-emerald-400 text-emerald-900" : "border-sky-400 text-sky-900");
  const planned = Array.isArray(plan.planned_self_heal_actions) ? plan.planned_self_heal_actions : [];
  const critical = Array.isArray(plan.critical_failures) ? plan.critical_failures : [];
  const historyHtml = history.map((h) => `<li>Attempt ${escapeHtml(h.attempt || 0)}: ${escapeHtml(String(h.status || "pending").toUpperCase())}</li>`).join("");
  el.retryPlanContent.innerHTML = `
    <div><strong>Summary:</strong> ${escapeHtml(plan.pre_retry_diagnosis?.diagnosis_summary || "")}</div>
    <div class="mt-2"><strong>Critical Failures:</strong> ${critical.length}</div>
    <ul class="list-disc pl-5">${critical.slice(0, 6).map((f) => `<li>${escapeHtml(f.name || "failure")} - ${escapeHtml(f.root_cause || "")}</li>`).join("") || "<li>None</li>"}</ul>
    <div class="mt-2"><strong>Planned Self-Heal Actions:</strong></div>
    <ul class="list-disc pl-5">${planned.slice(0, 8).map((x) => `<li>${escapeHtml(x)}</li>`).join("") || "<li>None</li>"}</ul>
    <div class="mt-2"><strong>Retry Timeline:</strong></div>
    <ul class="list-disc pl-5">${historyHtml || "<li>No retry history yet</li>"}</ul>
  `;
}

function renderAnalystReadable(output) {
  const fr = output.functional_requirements || [];
  const nfr = output.non_functional_requirements || [];
  return `
    <h5 class="text-sm font-semibold text-ink-950">Technical Requirements Document</h5>
    <div><strong>Project:</strong> ${escapeHtml(output.project_name || "Untitled")}</div>
    <div><strong>Summary:</strong> ${escapeHtml(output.executive_summary || "")}</div>
    <div class="mt-2"><strong>Functional Requirements (${fr.length})</strong></div>
    <ul class="list-disc pl-5">${fr.slice(0, 10).map((r) => `<li><strong>${escapeHtml(r.id || "FR")}</strong> ${escapeHtml(r.title || "")}</li>`).join("")}</ul>
    <div class="mt-2"><strong>Non-Functional Requirements (${nfr.length})</strong></div>
    <ul class="list-disc pl-5">${nfr.slice(0, 10).map((r) => `<li><strong>${escapeHtml(r.id || "NFR")}</strong> ${escapeHtml(r.title || "")}</li>`).join("")}</ul>
  `;
}

function renderArchitectReadable(output) {
  const legacy = output.legacy_system || {};
  const currentDiagram = legacy.current_system_diagram_mermaid
    || legacy.legacy_diagram_mermaid
    || output.current_system_diagram_mermaid
    || output.legacy_system_diagram_mermaid
    || "";
  const targetDiagram = output.target_system_diagram_mermaid
    || output.target_architecture_diagram_mermaid
    || output.target_diagram_mermaid
    || output.architecture_diagram_mermaid
    || "";
  return `
    <div><strong>Pattern:</strong> ${escapeHtml(output.pattern || "")}</div>
    <div><strong>Overview:</strong> ${escapeHtml(output.overview || "")}</div>
    ${mermaidBlock("Current System Diagram", currentDiagram)}
    ${mermaidBlock("Target Architecture Diagram", targetDiagram)}
  `;
}

function renderDeveloperReadable(output) {
  return `
    <div><strong>Total LOC:</strong> ${Number(output.total_loc || 0).toLocaleString()}</div>
    <div><strong>Components:</strong> ${Number(output.total_components || 0)}</div>
    <div><strong>Files:</strong> ${Number(output.total_files || 0)}</div>
  `;
}

function renderDatabaseReadable(output) {
  const scripts = output.generated_scripts || [];
  return `
    <div><strong>Source:</strong> ${escapeHtml(output.source_engine || "")}</div>
    <div><strong>Target:</strong> ${escapeHtml(output.target_engine || "")}</div>
    <div><strong>Migration Summary:</strong> ${escapeHtml(output.migration_summary || "")}</div>
    <div class="mt-2"><strong>Generated Scripts (${scripts.length})</strong></div>
    <ul class="list-disc pl-5">${scripts.slice(0, 8).map((s) => `<li>${escapeHtml(s.name || "script")} (${escapeHtml(s.type || "sql")})</li>`).join("")}</ul>
  `;
}

function renderSecurityReadable(output) {
  const threats = output.threat_model || [];
  const controls = output.required_controls || [];
  const release = output.release_recommendation || {};
  return `
    <div><strong>Security Summary:</strong> ${escapeHtml(output.security_summary || "")}</div>
    <div><strong>Release Recommendation:</strong> ${escapeHtml((release.status || "conditional").toUpperCase())}</div>
    <div class="mt-2"><strong>Threats (${threats.length})</strong></div>
    <ul class="list-disc pl-5">${threats.slice(0, 8).map((t) => `<li>${escapeHtml(t.asset || "asset")}: ${escapeHtml(t.threat || "")}</li>`).join("")}</ul>
    <div class="mt-2"><strong>Controls (${controls.length})</strong></div>
    <ul class="list-disc pl-5">${controls.slice(0, 8).map((c) => `<li>${escapeHtml(c.control || "control")}</li>`).join("")}</ul>
  `;
}

function renderTesterReadable(output) {
  const overall = output.overall_results || {};
  const failed = output.failed_checks || [];
  return `
    <div><strong>Total Checks:</strong> ${overall.total_tests || 0}</div>
    <div><strong>Passed:</strong> ${overall.passed || 0}</div>
    <div><strong>Failed:</strong> ${overall.failed || 0}</div>
    <div><strong>Warnings:</strong> ${overall.warnings || 0}</div>
    <div><strong>Quality Gate:</strong> ${escapeHtml((overall.quality_gate || "").toUpperCase())}</div>
    <div class="mt-2"><strong>Top Failures</strong></div>
    <ul class="list-disc pl-5">${failed.slice(0, 8).map((f) => `<li>${escapeHtml(f.name || "check")}: ${escapeHtml(f.root_cause || "")}</li>`).join("") || "<li>None</li>"}</ul>
  `;
}

function renderValidatorReadable(output) {
  const verdict = output.overall_verdict || {};
  return `
    <h5 class="text-sm font-semibold text-ink-950">Functional Validation Report</h5>
    <div><strong>Summary:</strong> ${escapeHtml(output.validation_summary || "")}</div>
    <div><strong>Verdict:</strong> ${escapeHtml((verdict.status || "unknown").toUpperCase())}</div>
    <div><strong>Functional Coverage:</strong> ${escapeHtml(verdict.functional_coverage_percent || 0)}%</div>
    <div><strong>NFR Compliance:</strong> ${escapeHtml(verdict.nfr_compliance_percent || 0)}%</div>
  `;
}

function renderDeployerReadable(output) {
  const result = output.deployment_result || {};
  return `
    <div><strong>Deployment Target:</strong> ${escapeHtml(output.deployment_target || "")}</div>
    <div><strong>Status:</strong> ${escapeHtml((result.status || "").toUpperCase())}</div>
    <div><strong>URL:</strong> ${escapeHtml(result.url || "")}</div>
  `;
}

function renderReadableOutput(stage, output) {
  if (!output || typeof output !== "object") return "<p>No structured output available.</p>";
  if (stage === 1) return renderAnalystReadable(output);
  if (stage === 2) return renderArchitectReadable(output);
  if (stage === 3) return renderDeveloperReadable(output);
  if (stage === 4) return renderDatabaseReadable(output);
  if (stage === 5) return renderSecurityReadable(output);
  if (stage === 6) return renderTesterReadable(output);
  if (stage === 7) return renderValidatorReadable(output);
  if (stage === 8) return renderDeployerReadable(output);
  return `<pre class="mono text-[11px]">${escapeHtml(JSON.stringify(output, null, 2).slice(0, 2200))}</pre>`;
}

function personaForStage(run, stage) {
  const fromRun = run?.pipeline_state?.agent_personas?.[String(stage)] || {};
  if (fromRun.display_name) return fromRun;
  return state.teamSelection.agentPersonas?.[String(stage)] || {};
}

function renderCurrentAgentPanel() {
  const run = state.currentRun;
  const stage = determineCurrentStage(run);
  const agent = AGENTS.find((a) => a.stage === stage) || AGENTS[0];
  const stageStatus = run?.stage_status || {};
  const status = stageStatus[agent.stage] || "pending";
  const result = latestResultByStage(run, agent.stage);
  const persona = personaForStage(run, agent.stage);

  el.currentAgentPanel.innerHTML = `
    <div class="flex flex-wrap items-start justify-between gap-3 ${status === "running" ? "running-glow" : ""}">
      <div>
        <p class="text-xs uppercase tracking-[0.16em] text-slate-600">Current Agent</p>
        <h3 class="mt-1 text-xl font-semibold text-ink-950">Stage ${agent.stage}: ${agent.icon} ${agent.name}</h3>
        <p class="mt-1 text-sm text-slate-700">${agent.desc}</p>
        <p class="mt-1 text-xs text-slate-700"><strong>Persona:</strong> ${escapeHtml(persona.display_name || "Default")}</p>
        <p class="mono mt-2 text-xs text-slate-700">${escapeHtml(result?.summary || "No output yet.")}</p>
      </div>
      <div class="text-right">
        <span class="inline-flex rounded-md border px-2 py-1 text-xs font-semibold ${statusTone(status)}">${statusLabel(status)}</span>
        <p class="mono mt-2 text-[11px] text-slate-600">${(result?.tokens_used || 0).toLocaleString()} tokens</p>
        <p class="mono text-[11px] text-slate-600">${((result?.latency_ms || 0) / 1000).toFixed(1)}s</p>
        ${result?.output ? `<button data-open-stage="${agent.stage}" class="btn-dark mt-2 rounded-md px-3 py-1.5 text-xs font-semibold">View Output</button>` : ""}
      </div>
    </div>
  `;

  const openBtn = el.currentAgentPanel.querySelector("[data-open-stage]");
  if (openBtn) openBtn.addEventListener("click", () => openStageModal(Number(openBtn.getAttribute("data-open-stage"))));
}

function renderAgentTabs() {
  const run = state.currentRun;
  const stageStatus = run?.stage_status || {};
  el.agentTabs.innerHTML = AGENTS.map((agent) => {
    const status = stageStatus[agent.stage] || "pending";
    const selected = state.selectedStage === agent.stage;
    return `
      <button data-select-stage="${agent.stage}" class="rounded-lg border px-3 py-2 text-xs font-semibold ${selected ? "border-ink-900 bg-sky-100 text-slate-900" : `${statusTone(status)}`}">
        ${agent.icon} ${agent.name}
      </button>
    `;
  }).join("");
  el.agentTabs.querySelectorAll("[data-select-stage]").forEach((btn) => {
    btn.addEventListener("click", () => {
      state.selectedStage = Number(btn.getAttribute("data-select-stage"));
      renderAgentTabs();
      renderAgentTabPanel();
    });
  });
}

function renderAgentTabPanel() {
  const run = state.currentRun;
  const stageStatus = run?.stage_status || {};
  const stage = state.selectedStage || 1;
  const agent = AGENTS.find((a) => a.stage === stage) || AGENTS[0];
  const result = latestResultByStage(run, stage);
  const status = stageStatus[stage] || "pending";
  const logs = (result?.logs || []).slice(-20).join("\n");
  const persona = personaForStage(run, stage);

  el.agentTabPanel.innerHTML = `
    <div class="flex flex-wrap items-start justify-between gap-3">
      <div>
        <h4 class="text-sm font-semibold text-ink-950">${agent.icon} Stage ${agent.stage}: ${agent.name}</h4>
        <p class="mt-1 text-xs text-slate-600">${agent.desc}</p>
        <p class="mt-1 text-xs text-slate-700"><strong>Persona:</strong> ${escapeHtml(persona.display_name || "Default")}</p>
      </div>
      <div class="text-right">
        <span class="inline-flex rounded-md border px-2 py-1 text-xs font-semibold ${statusTone(status)}">${statusLabel(status)}</span>
        ${result?.output ? `<button data-open-stage="${stage}" class="btn-dark ml-2 rounded-md px-3 py-1.5 text-xs font-semibold">View Full</button>` : ""}
      </div>
    </div>
    <div class="mt-2 text-xs text-slate-700">${escapeHtml(result?.summary || "No output yet.")}</div>
    <div class="mt-2 rounded-lg border border-slate-300 bg-slate-50 p-2 text-xs text-slate-800">${renderReadableOutput(stage, result?.output || {})}</div>
    <pre class="mono mt-2 h-[120px] overflow-auto rounded-lg border border-slate-300 bg-ink-950 p-3 text-[11px] text-slate-200">${escapeHtml(logs)}</pre>
  `;
  const openBtn = el.agentTabPanel.querySelector("[data-open-stage]");
  if (openBtn) openBtn.addEventListener("click", () => openStageModal(Number(openBtn.getAttribute("data-open-stage"))));
}

function openStageModal(stage) {
  const agent = AGENTS.find((a) => a.stage === stage);
  const result = latestResultByStage(state.currentRun, stage);
  if (!result || !agent) return;

  el.modalTitle.textContent = `Stage ${stage}: ${agent.name}`;
  el.modalSummary.textContent = result.summary || "";
  el.modalReadable.innerHTML = renderReadableOutput(stage, result.output || {});
  el.modalLogs.textContent = (result.logs || []).join("\n");
  if (stage === 7) {
    el.modalOutput.textContent = "Validation report is intentionally rendered as a human-readable summary in the Readable Output pane.";
  } else {
    el.modalOutput.textContent = JSON.stringify(result.output || {}, null, 2);
  }
  el.outputModal.showModal();
  setTimeout(() => renderMermaidBlocks(el.outputModal), 0);
}

function renderLogs() {
  const logs = state.currentRun?.progress_logs || [];
  el.liveLogs.textContent = logs.slice(-800).join("\n");
  el.liveLogs.scrollTop = el.liveLogs.scrollHeight;
}

function renderFlowDiagram() {
  const status = state.currentRun?.status;
  el.flowDiagramSection.classList.toggle("hidden", !(status === "completed" || status === "failed"));
}

function renderApprovalPanel() {
  const pending = state.currentRun?.pending_approval;
  if (!pending) {
    el.approvalPanel.classList.add("hidden");
    return;
  }
  el.approvalPanel.classList.remove("hidden");
  el.approvalTitle.textContent = `Approval Required: ${pending.type || "stage"}`;
  const required = pending.required_fields ? `Required fields: ${(pending.required_fields || []).join(", ")}` : "";
  el.approvalMessage.textContent = [pending.message || "", required].filter(Boolean).join(" | ");

  el.approvalDeveloperOptions.classList.add("hidden");
  el.approvalCloudOptions.classList.add("hidden");

  if (pending.type === "developer_plan") {
    el.approvalDeveloperOptions.classList.remove("hidden");
    const plan = state.currentRun?.pipeline_state?.developer_plan || {};
    el.approveMicroservicesCount.value = plan.default_microservices_count || 2;
    el.approveSplitStrategy.value = plan.default_split_strategy || "domain-driven";
    el.approveTargetLanguage.value = plan.default_target_language || el.modernizationLanguage.value || "python";
    el.approveTargetPlatform.value = plan.default_target_platform || "docker-local";
  }
  if (pending.type === "cloud_details") {
    el.approvalCloudOptions.classList.remove("hidden");
    const cfg = state.currentRun?.pipeline_state?.cloud_config || {};
    el.approveCloudPlatform.value = cfg.platform || el.cloudPlatform.value || "";
    el.approveCloudRegion.value = cfg.region || el.cloudRegion.value || "";
    el.approveCloudServiceName.value = cfg.service_name || el.cloudServiceName.value || "";
    el.approveCloudProjectId.value = cfg.project_id || el.cloudProjectId.value || "";
    el.approveCloudResourceGroup.value = cfg.resource_group || el.cloudResourceGroup.value || "";
    el.approveCloudSubscriptionId.value = cfg.subscription_id || el.cloudSubscriptionId.value || "";
    el.approveCloudPower.value = cfg.power || el.cloudPower.value || "";
    el.approveCloudScale.value = cfg.scale || el.cloudScale.value || "";
    el.approveCloudCredentials.value = cfg.credentials || el.cloudCredentials.value || "";
    const extra = cfg.extra || {};
    el.approveCloudExtra.value = Object.keys(extra).map((k) => `${k}=${extra[k]}`).join("\n");
  }
}

function renderRun() {
  if (!state.selectedStage) state.selectedStage = determineCurrentStage(state.currentRun);
  renderTaskSummary();
  renderStatusChips();
  renderProgress();
  renderRetryPlan();
  renderApprovalPanel();
  renderCurrentAgentPanel();
  renderAgentTabs();
  renderAgentTabPanel();
  renderLogs();
  renderFlowDiagram();
  setTimeout(() => renderMermaidBlocks(document), 0);
}

function stopStreaming() {
  if (state.eventSource) {
    state.eventSource.close();
    state.eventSource = null;
  }
}

function upsertLog(line) {
  if (!state.currentRun) return;
  if (!Array.isArray(state.currentRun.progress_logs)) state.currentRun.progress_logs = [];
  const logs = state.currentRun.progress_logs;
  if (!logs.length || logs[logs.length - 1] !== line) logs.push(line);
}

async function fetchRunSnapshot(runId) {
  const data = await api(`/api/runs/${runId}`, null);
  state.currentRun = data.run;
  state.selectedStage = determineCurrentStage(state.currentRun);
  const p = state.currentRun?.pipeline_state || {};
  if (p.team_id || p.stage_agent_ids) {
    applyTeamSelection(
      { id: p.team_id || "", name: p.team_name || "Ad-hoc Team", description: p.team?.description || "", stage_agent_ids: p.stage_agent_ids || {} },
      p.agent_personas || derivePersonasFromStageMap(p.stage_agent_ids || {}),
      ""
    );
  }
  renderRun();
  return data.run;
}

function startStreaming(runId) {
  stopStreaming();
  state.eventSource = new EventSource(`/api/runs/${runId}/stream`);

  state.eventSource.addEventListener("snapshot", (evt) => {
    try {
      const payload = JSON.parse(evt.data);
      if (payload.run) {
        state.currentRun = payload.run;
        state.selectedStage = determineCurrentStage(state.currentRun);
        renderRun();
      }
    } catch (_err) {}
  });

  state.eventSource.addEventListener("update", (evt) => {
    try {
      const payload = JSON.parse(evt.data);
      if (payload.run) {
        state.currentRun = payload.run;
        state.selectedStage = determineCurrentStage(state.currentRun);
        renderRun();
      }
    } catch (_err) {}
  });

  state.eventSource.addEventListener("log", (evt) => {
    try {
      const payload = JSON.parse(evt.data);
      if (payload.line) {
        upsertLog(payload.line);
        renderLogs();
      }
    } catch (_err) {}
  });

  state.eventSource.addEventListener("done", async () => {
    stopStreaming();
    await fetchRunSnapshot(runId);
    await refreshRunHistory();
    await refreshArtifactsList();
    await refreshTasks().catch(() => {});
  });

  state.eventSource.onerror = async () => {
    stopStreaming();
    try {
      const run = await fetchRunSnapshot(runId);
      if (run.status === "running") setTimeout(() => startStreaming(runId), 1200);
    } catch (err) {
      el.pipelineStatusText.textContent = `STREAM ERROR: ${err.message}`;
    }
  };
}

async function syncRun(runId) {
  if (!runId) return;
  try {
    const run = await fetchRunSnapshot(runId);
    if (run.status === "running") startStreaming(runId);
    else stopStreaming();
    await refreshArtifactsList();
  } catch (err) {
    el.pipelineStatusText.textContent = `ERROR: ${err.message}`;
  }
}

async function refreshRunHistory() {
  const data = await api("/api/runs", null);
  const runs = data.runs || [];
  if (!runs.length) {
    el.runHistory.innerHTML = `<option value="">No runs yet</option>`;
    return;
  }
  el.runHistory.innerHTML = runs.map((run) => {
    const created = (run.created_at || "").replace("T", " ").slice(0, 19);
    const obj = (run.business_objectives || "").replace(/\s+/g, " ").slice(0, 42);
    return `<option value="${run.run_id}">${created} | ${run.status} | ${obj}${obj.length >= 42 ? "..." : ""}</option>`;
  }).join("");
}

async function loadRunFromHistory() {
  const runId = el.runHistory.value;
  if (!runId) return;
  state.currentRunId = runId;
  stopStreaming();
  await syncRun(runId);

  const p = state.currentRun?.pipeline_state || {};
  if (p.business_objectives) el.objectives.value = p.business_objectives;
  if (p.legacy_code) el.legacyCode.value = p.legacy_code;
  if (p.modernization_language) el.modernizationLanguage.value = p.modernization_language;
  if (p.database_source) el.dbSource.value = p.database_source;
  if (p.database_target) el.dbTarget.value = p.database_target;
  if (p.database_schema) el.dbSchema.value = p.database_schema;
  if (p.use_case) el.taskType.value = p.use_case;
  if (p.deployment_target) el.deploymentTarget.value = p.deployment_target;

  const cfg = p.cloud_config || {};
  el.cloudPlatform.value = cfg.platform || "";
  el.cloudRegion.value = cfg.region || "";
  el.cloudServiceName.value = cfg.service_name || "";
  el.cloudProjectId.value = cfg.project_id || "";
  el.cloudResourceGroup.value = cfg.resource_group || "";
  el.cloudSubscriptionId.value = cfg.subscription_id || "";
  el.cloudPower.value = cfg.power || "";
  el.cloudScale.value = cfg.scale || "";
  el.cloudCredentials.value = cfg.credentials || "";
  el.cloudExtra.value = Object.entries(cfg.extra || {}).map(([k, v]) => `${k}=${v}`).join("\n");

  el.humanApproval.checked = !!(state.currentRun?.human_approval ?? p.human_approval);
  el.strictSecurityMode.checked = !!(state.currentRun?.strict_security_mode ?? p.strict_security_mode);

  toggleUseCasePanel();
  toggleCloudConfig();
  setMode(MODES.WORK);
  setWizardStep(2);
}

async function submitApproval(decision) {
  const runId = state.currentRunId;
  if (!runId) return;
  const pending = state.currentRun?.pending_approval || {};
  const payload = { decision };

  if (pending.type === "developer_plan") {
    payload.developer_choices = {
      microservices_count: Number(el.approveMicroservicesCount.value || 2),
      split_strategy: el.approveSplitStrategy.value || "domain-driven",
      target_language: (el.approveTargetLanguage.value || "").trim(),
      target_platform: (el.approveTargetPlatform.value || "").trim(),
    };
  }

  if (pending.type === "cloud_details") {
    payload.cloud_config = {
      platform: (el.approveCloudPlatform.value || "").trim().toLowerCase(),
      region: (el.approveCloudRegion.value || "").trim(),
      service_name: (el.approveCloudServiceName.value || "").trim(),
      project_id: (el.approveCloudProjectId.value || "").trim(),
      resource_group: (el.approveCloudResourceGroup.value || "").trim(),
      subscription_id: (el.approveCloudSubscriptionId.value || "").trim(),
      power: (el.approveCloudPower.value || "").trim(),
      scale: (el.approveCloudScale.value || "").trim(),
      credentials: (el.approveCloudCredentials.value || "").trim(),
      extra: parseKeyValueLines(el.approveCloudExtra.value || ""),
    };
  }

  try {
    await api(`/api/runs/${runId}/approve`, payload);
    await syncRun(runId);
  } catch (err) {
    alert(`Approval action failed: ${err.message}`);
  }
}

function selectedStageAgentIdsForRun() {
  if (state.teamSelection.stageAgentIds && Object.keys(state.teamSelection.stageAgentIds).length) {
    return { ...state.teamSelection.stageAgentIds };
  }
  return { ...defaultBuilderMap() };
}

async function startRun() {
  const objectives = (el.objectives.value || "").trim();
  if (!objectives) {
    alert("Business challenge is required.");
    return;
  }
  const apiKey = (el.apiKey.value || "").trim();
  if (!apiKey) {
    alert("API key is required.");
    return;
  }

  const useCase = currentUseCase();
  if (useCase === "code_modernization" && !(el.legacyCode.value || "").trim()) {
    alert("Legacy code is required for code modernization use case.");
    return;
  }
  if (useCase === "database_conversion" && !(el.dbSchema.value || "").trim()) {
    alert("Legacy schema/SQL is required for database conversion use case.");
    return;
  }

  stopStreaming();

  const cloudConfig = {
    platform: (el.cloudPlatform.value || "").trim().toLowerCase(),
    region: (el.cloudRegion.value || "").trim(),
    service_name: (el.cloudServiceName.value || "").trim(),
    project_id: (el.cloudProjectId.value || "").trim(),
    resource_group: (el.cloudResourceGroup.value || "").trim(),
    subscription_id: (el.cloudSubscriptionId.value || "").trim(),
    power: (el.cloudPower.value || "").trim(),
    scale: (el.cloudScale.value || "").trim(),
    credentials: (el.cloudCredentials.value || "").trim(),
    extra: parseKeyValueLines(el.cloudExtra.value || ""),
  };

  const payload = {
    use_case: useCase,
    objectives,
    legacy_code: (el.legacyCode.value || "").trim(),
    modernization_language: (el.modernizationLanguage.value || "").trim(),
    database_source: (el.dbSource.value || "").trim(),
    database_target: (el.dbTarget.value || "").trim(),
    database_schema: (el.dbSchema.value || "").trim(),
    human_approval: !!el.humanApproval.checked,
    strict_security_mode: !!el.strictSecurityMode.checked,
    deployment_target: el.deploymentTarget.value || "local",
    cloud_config: cloudConfig,
    team_id: state.teamSelection.teamId || "",
    stage_agent_ids: selectedStageAgentIdsForRun(),
    provider: el.provider.value,
    model: el.model.value,
    api_key: apiKey,
    temperature: Number(el.temperature.value || 0.3),
    parallel_agents: Number(el.parallelAgents.value || 5),
    max_retries: Number(el.maxRetries.value || 2),
    live_deploy: !!el.liveDeploy.checked,
    cluster_name: el.clusterName.value || "agent-pipeline",
    namespace: el.namespace.value || "agent-app",
    deploy_output_dir: el.deployOutputDir.value || "./deploy_output",
  };

  try {
    const data = await api("/api/runs", payload);
    state.currentRunId = data.run_id;
    state.selectedStage = 1;
    state.currentRun = {
      run_id: data.run_id,
      status: "running",
      current_stage: 0,
      stage_status: {},
      progress_logs: [],
      pipeline_state: null,
      error_message: null,
      retry_count: 0,
      team_id: state.teamSelection.teamId,
      team_name: state.teamSelection.teamName,
    };
    renderRun();
    await refreshRunHistory();
    await syncRun(data.run_id);
    setMode(MODES.WORK);
    setWizardStep(2);
  } catch (err) {
    alert(`Failed to start run: ${err.message}`);
  }
}

async function refreshArtifactsList() {
  if (!state.currentRunId) {
    el.artifactSelect.innerHTML = `<option value="">No run selected</option>`;
    el.artifactPreview.textContent = "";
    return;
  }
  try {
    const data = await api(`/api/runs/${state.currentRunId}/artifacts`, null);
    state.artifacts = data.artifacts || [];
    if (!state.artifacts.length) {
      el.artifactSelect.innerHTML = `<option value="">No artifacts found for this run</option>`;
      el.artifactPreview.textContent = "";
      return;
    }
    el.artifactSelect.innerHTML = state.artifacts
      .map((a) => `<option value="${escapeHtml(a.artifact_id)}">${escapeHtml(a.root)} :: ${escapeHtml(a.relative_path)} (${a.size_bytes} bytes)</option>`)
      .join("");
  } catch (err) {
    el.artifactSelect.innerHTML = `<option value="">Artifact load error: ${escapeHtml(err.message)}</option>`;
  }
}

async function openSelectedArtifact() {
  const artifactId = el.artifactSelect.value;
  if (!state.currentRunId || !artifactId) {
    el.artifactPreview.textContent = "Select an artifact first.";
    return;
  }
  try {
    const data = await api(`/api/runs/${state.currentRunId}/artifacts/content?artifact_id=${encodeURIComponent(artifactId)}`, null);
    if (data.is_binary) {
      el.artifactPreview.textContent = `Binary file: ${data.path}`;
      return;
    }
    const trunc = data.truncated ? "\n\n[truncated preview]" : "";
    el.artifactPreview.textContent = `${data.path}\n${"-".repeat(80)}\n${data.content}${trunc}`;
    el.artifactPreview.scrollTop = 0;
  } catch (err) {
    el.artifactPreview.textContent = `Failed to load artifact: ${err.message}`;
  }
}

function readTextFile(file, onSuccess) {
  if (!file) return;
  const reader = new FileReader();
  reader.onload = () => onSuccess(String(reader.result || ""));
  reader.onerror = () => alert("Failed to read selected file.");
  reader.readAsText(file);
}

function bindEvents() {
  el.navHome.addEventListener("click", () => setMode(MODES.HOME));
  el.navWork.addEventListener("click", () => {
    setMode(MODES.WORK);
    setWizardStep(1);
  });
  el.navTeam.addEventListener("click", () => setMode(MODES.TEAM));
  el.navHistory.addEventListener("click", () => setMode(MODES.HISTORY));

  el.homeWorkBtn.addEventListener("click", () => { setMode(MODES.WORK); setWizardStep(1); });
  el.homeTeamBtn.addEventListener("click", () => setMode(MODES.TEAM));
  el.homeHistoryBtn.addEventListener("click", () => setMode(MODES.HISTORY));

  el.provider.addEventListener("change", () => {
    setDefaultModelByProvider();
    renderTaskSummary();
  });
  el.model.addEventListener("input", renderTaskSummary);
  el.temperature.addEventListener("input", renderTaskSummary);
  el.parallelAgents.addEventListener("input", renderTaskSummary);
  el.maxRetries.addEventListener("input", renderTaskSummary);
  el.humanApproval.addEventListener("change", renderTaskSummary);
  el.strictSecurityMode.addEventListener("change", renderTaskSummary);
  el.liveDeploy.addEventListener("change", renderTaskSummary);
  el.deploymentTarget.addEventListener("change", () => {
    toggleCloudConfig();
    renderTaskSummary();
  });
  el.taskType.addEventListener("change", () => {
    toggleUseCasePanel();
    if (currentUseCase() !== "business_objectives") {
      el.objectives.dataset.autogen = "1";
      setAutogeneratedObjective();
    }
    renderTaskSummary();
  });
  el.modernizationLanguage.addEventListener("change", () => {
    if (isCodeModernizationMode()) {
      el.objectives.dataset.autogen = "1";
      setAutogeneratedObjective();
    }
    renderTaskSummary();
  });
  el.dbSource.addEventListener("change", () => {
    if (isDatabaseConversionMode()) {
      el.objectives.dataset.autogen = "1";
      setAutogeneratedObjective();
    }
    renderTaskSummary();
  });
  el.dbTarget.addEventListener("change", () => {
    if (isDatabaseConversionMode()) {
      el.objectives.dataset.autogen = "1";
      setAutogeneratedObjective();
    }
    renderTaskSummary();
  });
  el.objectives.addEventListener("input", () => {
    el.objectives.dataset.autogen = "0";
    renderTaskSummary();
  });
  el.legacyCode.addEventListener("input", renderTaskSummary);
  el.dbSchema.addEventListener("input", renderTaskSummary);

  el.wizardContinue.addEventListener("click", async () => {
    const objectives = String(el.objectives.value || "").trim();
    if (!objectives) {
      alert("Please provide the business challenge first.");
      return;
    }
    if (isCodeModernizationMode() && !(el.legacyCode.value || "").trim()) {
      alert("Please provide legacy code for code modernization.");
      return;
    }
    if (isDatabaseConversionMode() && !(el.dbSchema.value || "").trim()) {
      alert("Please provide legacy schema/SQL for database conversion.");
      return;
    }
    await suggestTeamFromObjectives().catch(() => {});
    setWizardStep(2);
  });

  el.wizardBack.addEventListener("click", () => setWizardStep(1));

  el.workRefreshTeams.addEventListener("click", () => loadAgentsAndTeams().catch((err) => alert(err.message)));
  el.workSuggestTeam.addEventListener("click", () => suggestTeamFromObjectives().catch((err) => alert(err.message)));
  el.workApplyTeam.addEventListener("click", () => applySelectedTeamFromDropdown().catch((err) => alert(err.message)));
  el.workOpenTeamBuilder.addEventListener("click", () => setMode(MODES.TEAM));
  el.workOpenHistory.addEventListener("click", () => setMode(MODES.HISTORY));

  el.teamSaveBtn.addEventListener("click", () => saveTeamFromBuilder().catch((err) => alert(err.message)));
  el.teamUseInWorkBtn.addEventListener("click", useBuilderTeamInWork);
  el.teamRefreshBtn.addEventListener("click", () => loadAgentsAndTeams().catch((err) => alert(err.message)));
  el.cloneAgentBtn.addEventListener("click", () => cloneAgentFromBuilder().catch((err) => alert(err.message)));

  el.tasksRefresh.addEventListener("click", () => refreshTasks().catch((err) => alert(err.message)));

  el.uploadObjectives.addEventListener("click", () => el.objectivesFile.click());
  el.objectivesFile.addEventListener("change", () => {
    const file = el.objectivesFile.files?.[0];
    readTextFile(file, (text) => {
      el.objectives.value = text;
      el.objectives.dataset.autogen = "0";
      renderTaskSummary();
    });
  });

  el.uploadLegacy.addEventListener("click", () => el.legacyFile.click());
  el.legacyFile.addEventListener("change", () => {
    const file = el.legacyFile.files?.[0];
    readTextFile(file, (text) => {
      el.legacyCode.value = text;
      if (!isCodeModernizationMode()) el.taskType.value = "code_modernization";
      toggleUseCasePanel();
      el.objectives.dataset.autogen = "1";
      setAutogeneratedObjective();
      renderTaskSummary();
    });
  });

  el.uploadDb.addEventListener("click", () => el.dbFile.click());
  el.dbFile.addEventListener("change", () => {
    const file = el.dbFile.files?.[0];
    readTextFile(file, (text) => {
      el.dbSchema.value = text;
      if (!isDatabaseConversionMode()) el.taskType.value = "database_conversion";
      toggleUseCasePanel();
      el.objectives.dataset.autogen = "1";
      setAutogeneratedObjective();
      renderTaskSummary();
    });
  });

  el.runPipeline.addEventListener("click", startRun);
  el.approveStage.addEventListener("click", () => submitApproval("approve"));
  el.rejectStage.addEventListener("click", () => submitApproval("reject"));
  el.loadRun.addEventListener("click", () => loadRunFromHistory().catch((err) => alert(err.message)));
  el.refreshRunHistory.addEventListener("click", () => refreshRunHistory().catch((err) => alert(err.message)));
  el.refreshArtifacts.addEventListener("click", () => refreshArtifactsList().catch((err) => alert(err.message)));
  el.viewArtifact.addEventListener("click", () => openSelectedArtifact().catch((err) => alert(err.message)));
  el.closeModal.addEventListener("click", () => el.outputModal.close());
}

async function init() {
  bindEvents();
  setDefaultModelByProvider();
  toggleUseCasePanel();
  toggleCloudConfig();
  setWizardStep(1);

  await loadAgentsAndTeams();
  await refreshRunHistory();
  await refreshArtifactsList();
  await refreshTasks().catch(() => {});

  renderRun();
  setMode(MODES.HOME);
}

init().catch((err) => {
  el.pipelineStatusText.textContent = `INIT ERROR: ${err.message}`;
});
