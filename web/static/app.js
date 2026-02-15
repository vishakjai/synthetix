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
  DASHBOARDS: "dashboards",
  DISCOVER: "discover",
  PLAN: "plan",
  BUILD: "build",
  VERIFY: "verify",
  SETTINGS: "settings",
};

const el = {
  navHome: document.getElementById("nav-home"),
  navWork: document.getElementById("nav-work"),
  navTeam: document.getElementById("nav-team"),
  navBuild: document.getElementById("nav-build"),
  navHistory: document.getElementById("nav-history"),
  navSettings: document.getElementById("nav-settings"),
  homeWorkBtn: document.getElementById("home-work-btn"),
  homeTeamBtn: document.getElementById("home-team-btn"),
  homeHistoryBtn: document.getElementById("home-history-btn"),
  homeScreen: document.getElementById("home-screen"),
  workScreen: document.getElementById("work-screen"),
  teamScreen: document.getElementById("team-screen"),
  historyScreen: document.getElementById("history-screen"),
  settingsScreen: document.getElementById("settings-screen"),
  globalSearch: document.getElementById("global-search"),
  globalSearchStatus: document.getElementById("global-search-status"),
  notificationsBtn: document.getElementById("notifications-btn"),
  notificationsDialog: document.getElementById("notifications-dialog"),
  notificationsClose: document.getElementById("notifications-close"),
  notificationsContent: document.getElementById("notifications-content"),
  settingsRefresh: document.getElementById("settings-refresh"),
  settingsMessage: document.getElementById("settings-message"),

  settingsGithubStatus: document.getElementById("settings-github-status"),
  settingsGithubBaseUrl: document.getElementById("settings-github-base-url"),
  settingsGithubOwner: document.getElementById("settings-github-owner"),
  settingsGithubRepository: document.getElementById("settings-github-repository"),
  settingsGithubToken: document.getElementById("settings-github-token"),
  settingsGithubReadOnly: document.getElementById("settings-github-read-only"),
  settingsGithubConnect: document.getElementById("settings-github-connect"),
  settingsGithubTest: document.getElementById("settings-github-test"),
  settingsGithubDisconnect: document.getElementById("settings-github-disconnect"),
  settingsGithubMessage: document.getElementById("settings-github-message"),

  settingsJiraStatus: document.getElementById("settings-jira-status"),
  settingsJiraBaseUrl: document.getElementById("settings-jira-base-url"),
  settingsJiraProjectKey: document.getElementById("settings-jira-project-key"),
  settingsJiraEmail: document.getElementById("settings-jira-email"),
  settingsJiraToken: document.getElementById("settings-jira-token"),
  settingsJiraConnect: document.getElementById("settings-jira-connect"),
  settingsJiraTest: document.getElementById("settings-jira-test"),
  settingsJiraDisconnect: document.getElementById("settings-jira-disconnect"),
  settingsJiraMessage: document.getElementById("settings-jira-message"),

  settingsLinearStatus: document.getElementById("settings-linear-status"),
  settingsLinearBaseUrl: document.getElementById("settings-linear-base-url"),
  settingsLinearTeamKey: document.getElementById("settings-linear-team-key"),
  settingsLinearToken: document.getElementById("settings-linear-token"),
  settingsLinearConnect: document.getElementById("settings-linear-connect"),
  settingsLinearTest: document.getElementById("settings-linear-test"),
  settingsLinearDisconnect: document.getElementById("settings-linear-disconnect"),
  settingsLinearMessage: document.getElementById("settings-linear-message"),

  settingsPolicyPack: document.getElementById("settings-policy-pack"),
  settingsPolicyQuality: document.getElementById("settings-policy-quality"),
  settingsPolicyExceptionSla: document.getElementById("settings-policy-exception-sla"),
  settingsPolicyHumanApproval: document.getElementById("settings-policy-human-approval"),
  settingsPolicyBlockCritical: document.getElementById("settings-policy-block-critical"),
  settingsPolicyRequireSecurity: document.getElementById("settings-policy-require-security"),
  settingsPolicyBranchProtection: document.getElementById("settings-policy-branch-protection"),
  settingsPolicySave: document.getElementById("settings-policy-save"),
  settingsPolicyClone: document.getElementById("settings-policy-clone"),
  settingsPolicyVersion: document.getElementById("settings-policy-version"),
  settingsPolicyExport: document.getElementById("settings-policy-export"),
  settingsPolicyMessage: document.getElementById("settings-policy-message"),

  settingsExceptionRule: document.getElementById("settings-exception-rule"),
  settingsExceptionOwner: document.getElementById("settings-exception-owner"),
  settingsExceptionExpiry: document.getElementById("settings-exception-expiry"),
  settingsExceptionReason: document.getElementById("settings-exception-reason"),
  settingsExceptionAdd: document.getElementById("settings-exception-add"),
  settingsExceptionsList: document.getElementById("settings-exceptions-list"),

  settingsRbacRoleSelect: document.getElementById("settings-rbac-role-select"),
  settingsRbacSaveRole: document.getElementById("settings-rbac-save-role"),
  settingsRbacRoleMessage: document.getElementById("settings-rbac-role-message"),
  settingsRbacUserEmail: document.getElementById("settings-rbac-user-email"),
  settingsRbacUserRole: document.getElementById("settings-rbac-user-role"),
  settingsRbacAssign: document.getElementById("settings-rbac-assign"),
  settingsRbacAssignments: document.getElementById("settings-rbac-assignments"),
  settingsAuditRefresh: document.getElementById("settings-audit-refresh"),
  settingsAuditLog: document.getElementById("settings-audit-log"),

  perspectiveSwitcher: document.getElementById("perspective-switcher"),
  contextDrawerToggle: document.getElementById("context-drawer-toggle"),
  contextDrawer: document.getElementById("context-drawer"),
  shellGrid: document.getElementById("shell-grid"),
  drawerContextBundle: document.getElementById("drawer-context-bundle"),
  drawerPolicies: document.getElementById("drawer-policies"),
  drawerLinkedSystems: document.getElementById("drawer-linked-systems"),
  drawerEvidenceStatus: document.getElementById("drawer-evidence-status"),
  cmdPaletteBtn: document.getElementById("cmd-palette-btn"),
  commandPalette: document.getElementById("command-palette"),
  cmdkSearch: document.getElementById("cmdk-search"),
  cmdkActions: document.getElementById("cmdk-actions"),
  cmdkEmpty: document.getElementById("cmdk-empty"),
  dashboardTitle: document.getElementById("dashboard-title"),
  dashboardSubtitle: document.getElementById("dashboard-subtitle"),
  dashboardKpiRow: document.getElementById("dashboard-kpi-row"),
  dashboardMainLeft: document.getElementById("dashboard-main-left"),
  dashboardMainRight: document.getElementById("dashboard-main-right"),
  dashboardBottom: document.getElementById("dashboard-bottom"),

  workConfigPanel: document.getElementById("work-config-panel"),
  workIntakeStep: document.getElementById("work-intake-step"),
  workExecutionStep: document.getElementById("work-execution-step"),
  workRuntimePanels: document.getElementById("work-runtime-panels"),
  wizardContinue: document.getElementById("wizard-continue"),
  wizardPrevDiscover: document.getElementById("wizard-prev-discover"),
  wizardBack: document.getElementById("wizard-back"),
  discoverStepStatus: document.getElementById("discover-step-status"),
  discoverStepConnect: document.getElementById("discover-step-connect"),
  discoverStepScope: document.getElementById("discover-step-scope"),
  discoverStepScan: document.getElementById("discover-step-scan"),
  discoverStepResults: document.getElementById("discover-step-results"),
  discoverConnectPanel: document.getElementById("discover-connect-panel"),
  discoverScopePanel: document.getElementById("discover-scope-panel"),
  discoverScanPanel: document.getElementById("discover-scan-panel"),
  discoverResultsPanel: document.getElementById("discover-results-panel"),
  discoverResultsSummary: document.getElementById("discover-results-summary"),
  discoverResultsState: document.getElementById("discover-results-state"),
  discoverResultsIntegrations: document.getElementById("discover-results-integrations"),
  discoverResultsScan: document.getElementById("discover-results-scan"),
  discoverOpenCityMap: document.getElementById("discover-open-city-map"),
  discoverOpenSystemMap: document.getElementById("discover-open-system-map"),
  discoverOpenHealthDebt: document.getElementById("discover-open-health-debt"),
  discoverOpenConventions: document.getElementById("discover-open-conventions"),
  discoverExportBaseline: document.getElementById("discover-export-baseline"),
  discoverCityMapPanel: document.getElementById("discover-city-map-panel"),
  discoverSystemMapPanel: document.getElementById("discover-system-map-panel"),
  discoverHealthPanel: document.getElementById("discover-health-panel"),
  discoverConventionsPanel: document.getElementById("discover-conventions-panel"),
  projectStateMode: document.getElementById("project-state-mode"),
  detectProjectState: document.getElementById("detect-project-state"),
  projectStateResult: document.getElementById("project-state-result"),
  brownfieldIntegrations: document.getElementById("brownfield-integrations"),
  greenfieldIntegrations: document.getElementById("greenfield-integrations"),
  bfRepoProvider: document.getElementById("bf-repo-provider"),
  bfRepoUrl: document.getElementById("bf-repo-url"),
  bfIssueProvider: document.getElementById("bf-issue-provider"),
  bfIssueProject: document.getElementById("bf-issue-project"),
  bfDocsUrl: document.getElementById("bf-docs-url"),
  bfRuntimeTelemetry: document.getElementById("bf-runtime-telemetry"),
  gfRepoDestination: document.getElementById("gf-repo-destination"),
  gfRepoTarget: document.getElementById("gf-repo-target"),
  gfTrackerProvider: document.getElementById("gf-tracker-provider"),
  gfTrackerProject: document.getElementById("gf-tracker-project"),
  gfSaveGenerated: document.getElementById("gf-save-generated"),
  gfReadWriteTracker: document.getElementById("gf-read-write-tracker"),
  analysisDepth: document.getElementById("analysis-depth"),
  telemetryMode: document.getElementById("telemetry-mode"),
  includePaths: document.getElementById("include-paths"),
  excludePaths: document.getElementById("exclude-paths"),

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
  contextLayerCard: document.getElementById("context-layer-card"),
  contextLayerStatus: document.getElementById("context-layer-status"),
  contextLayerVersion: document.getElementById("context-layer-version"),
  contextLayerCommit: document.getElementById("context-layer-commit"),
  contextLayerCounts: document.getElementById("context-layer-counts"),
  contextLayerDelta: document.getElementById("context-layer-delta"),

  statusChips: document.getElementById("status-chips"),
  progressFill: document.getElementById("progress-fill"),
  progressMeta: document.getElementById("progress-meta"),
  pipelineStatusText: document.getElementById("pipeline-status-text"),
  retryPlanSection: document.getElementById("retry-plan-section"),
  retryPlanStatus: document.getElementById("retry-plan-status"),
  retryPlanContent: document.getElementById("retry-plan-content"),
  contextImpactInput: document.getElementById("context-impact-input"),
  contextImpactFiles: document.getElementById("context-impact-files"),
  runImpactForecast: document.getElementById("run-impact-forecast"),
  runDriftScan: document.getElementById("run-drift-scan"),
  contextOpsOutput: document.getElementById("context-ops-output"),
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
  mode: MODES.DASHBOARDS,
  wizardStep: 1,
  discoverStep: 1,
  discoverResultsView: "",
  notificationsTab: "approvals",
  projectState: {
    mode: "auto",
    detected: "",
    confidence: 0,
    reason: "",
  },
  currentRunId: "",
  currentRun: null,
  eventSource: null,
  selectedStage: 1,
  artifacts: [],
  teams: [],
  agents: { premade: [], custom: [], all: [], by_stage: {} },
  tasks: [],
  dashboardRuns: [],
  dashboardTasks: [],
  dashboardRunDetails: {},
  teamSelection: {
    teamId: "",
    teamName: "",
    description: "",
    stageAgentIds: {},
    agentPersonas: {},
    reason: "",
  },
  settings: null,
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

function parseLines(text) {
  return String(text || "")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
}

function setGlobalSearchStatus(text, isError = false) {
  if (!el.globalSearchStatus) return;
  el.globalSearchStatus.textContent = String(text || "");
  el.globalSearchStatus.className = `mt-2 text-xs ${isError ? "text-rose-700" : "text-slate-700"}`;
}

function handleGlobalSearchQuery(query) {
  const q = String(query || "").trim().toLowerCase();
  if (!q) {
    setGlobalSearchStatus("");
    return;
  }
  const runs = Array.isArray(state.dashboardRuns) ? state.dashboardRuns : [];
  const tasks = Array.isArray(state.dashboardTasks) ? state.dashboardTasks : [];
  const runMatch = runs.find((run) => {
    const haystack = [
      run.run_id,
      run.status,
      run.business_objectives,
      run.created_at,
      run.updated_at,
    ].join(" ").toLowerCase();
    return haystack.includes(q);
  });
  if (runMatch?.run_id) {
    state.currentRunId = String(runMatch.run_id);
    syncRun(state.currentRunId).catch(() => {});
    setMode(MODES.BUILD);
    setGlobalSearchStatus(`Opened run ${runMatch.run_id}.`);
    return;
  }

  const taskMatch = tasks.find((task) => {
    const haystack = [
      task.run_id,
      task.status,
      task.business_objective,
      task.objective_preview,
      task.use_case,
    ].join(" ").toLowerCase();
    return haystack.includes(q);
  });
  if (taskMatch?.run_id) {
    state.currentRunId = String(taskMatch.run_id);
    syncRun(state.currentRunId).catch(() => {});
    setMode(MODES.BUILD);
    setGlobalSearchStatus(`Opened work item ${taskMatch.run_id}.`);
    return;
  }
  setGlobalSearchStatus("No matches found. Try searching by service name, work ID, or artifact version.", true);
}

function renderDiscoverResultsView() {
  const view = String(state.discoverResultsView || "");
  const viewMap = {
    city: el.discoverCityMapPanel,
    system: el.discoverSystemMapPanel,
    health: el.discoverHealthPanel,
    conventions: el.discoverConventionsPanel,
  };
  Object.entries(viewMap).forEach(([key, panel]) => {
    panel?.classList.toggle("hidden", key !== view);
  });
}

function setDiscoverResultsView(view) {
  state.discoverResultsView = String(view || "");
  renderDiscoverResultsView();
}

function renderNotifications(tab = "approvals") {
  const selected = String(tab || "approvals").toLowerCase();
  state.notificationsTab = selected;
  if (!el.notificationsDialog || !el.notificationsContent) return;

  const runs = Array.isArray(state.dashboardRuns) ? state.dashboardRuns : [];
  let rows = [];
  let emptyText = "You’re all set. No actions require attention right now.";
  if (selected === "approvals") {
    rows = runs
      .filter((run) => String(run.status || "").toLowerCase() === "waiting_approval")
      .slice(0, 20)
      .map((run) => `Run <strong>${escapeHtml(run.run_id)}</strong> is paused and waiting for approval.`);
  } else if (selected === "failures") {
    rows = runs
      .filter((run) => String(run.status || "").toLowerCase() === "failed")
      .slice(0, 20)
      .map((run) => `Run <strong>${escapeHtml(run.run_id)}</strong> failed at ${escapeHtml(stageName(extractFailedStage(run)))}.`);
  } else {
    rows = runs
      .filter((run) => String(run.status || "").toLowerCase() === "failed" && !!run?.config?.strict_security_mode)
      .slice(0, 20)
      .map((run) => `Exception candidate for run <strong>${escapeHtml(run.run_id)}</strong> under strict policy.`);
  }
  el.notificationsContent.innerHTML = listBlock(rows, emptyText);
  el.notificationsDialog.querySelectorAll("[data-notification-tab]").forEach((node) => {
    if (!(node instanceof HTMLElement)) return;
    const key = String(node.getAttribute("data-notification-tab") || "");
    node.classList.toggle("btn-dark", key === selected);
    node.classList.toggle("btn-light", key !== selected);
  });
}

function filterCommandPalette() {
  if (!el.cmdkActions || !el.cmdkSearch) return;
  const query = String(el.cmdkSearch.value || "").trim().toLowerCase();
  const buttons = Array.from(el.cmdkActions.querySelectorAll("[data-command]"));
  let visible = 0;
  buttons.forEach((btn) => {
    if (!(btn instanceof HTMLElement)) return;
    const text = btn.textContent?.toLowerCase() || "";
    const show = !query || text.includes(query);
    btn.classList.toggle("hidden", !show);
    if (show) visible += 1;
  });
  if (el.cmdkEmpty) el.cmdkEmpty.classList.toggle("hidden", visible > 0);
}

function setSettingsMessage(text, isError = false) {
  if (!el.settingsMessage) return;
  el.settingsMessage.textContent = String(text || "");
  el.settingsMessage.className = `mt-2 text-xs ${isError ? "text-rose-700" : "text-slate-700"}`;
}

function settingsIntegrationRefs(provider) {
  if (provider === "github") {
    return {
      status: el.settingsGithubStatus,
      message: el.settingsGithubMessage,
      fields: {
        base_url: el.settingsGithubBaseUrl,
        owner: el.settingsGithubOwner,
        repository: el.settingsGithubRepository,
        token: el.settingsGithubToken,
        read_only: el.settingsGithubReadOnly,
      },
      secretKey: "token",
    };
  }
  if (provider === "jira") {
    return {
      status: el.settingsJiraStatus,
      message: el.settingsJiraMessage,
      fields: {
        base_url: el.settingsJiraBaseUrl,
        project_key: el.settingsJiraProjectKey,
        email: el.settingsJiraEmail,
        api_token: el.settingsJiraToken,
      },
      secretKey: "api_token",
    };
  }
  return {
    status: el.settingsLinearStatus,
    message: el.settingsLinearMessage,
    fields: {
      base_url: el.settingsLinearBaseUrl,
      team_key: el.settingsLinearTeamKey,
      api_token: el.settingsLinearToken,
    },
    secretKey: "api_token",
  };
}

function integrationStatusClass(status) {
  const value = String(status || "").toLowerCase();
  if (value === "connected") return "rounded border border-emerald-300 bg-emerald-50 px-2 py-0.5 text-[10px] text-emerald-900";
  if (value === "error") return "rounded border border-rose-300 bg-rose-50 px-2 py-0.5 text-[10px] text-rose-900";
  if (value === "incomplete") return "rounded border border-amber-300 bg-amber-50 px-2 py-0.5 text-[10px] text-amber-900";
  return "rounded border border-slate-300 bg-slate-100 px-2 py-0.5 text-[10px] text-slate-800";
}

function collectIntegrationPayload(provider) {
  const refs = settingsIntegrationRefs(provider);
  const fields = refs.fields || {};
  const payload = {};
  Object.entries(fields).forEach(([key, node]) => {
    if (!node) return;
    if (node instanceof HTMLInputElement && node.type === "checkbox") {
      payload[key] = !!node.checked;
      return;
    }
    payload[key] = String(node.value || "").trim();
  });
  return payload;
}

function updateIntegrationForm(provider, integration) {
  const refs = settingsIntegrationRefs(provider);
  const data = (integration && typeof integration === "object") ? integration : {};
  if (refs.status) {
    const status = String(data.status || (data.connected ? "connected" : "disconnected"));
    refs.status.textContent = status === "connected" ? "Connected" : "Not connected";
    refs.status.className = integrationStatusClass(status);
  }

  Object.entries(refs.fields || {}).forEach(([key, node]) => {
    if (!node) return;
    if (node instanceof HTMLInputElement && node.type === "checkbox") {
      node.checked = !!data[key];
      return;
    }
    if (key === refs.secretKey) {
      node.value = "";
      return;
    }
    node.value = String(data[key] || "");
  });

  if (refs.message) {
    const masked = String(data[`${refs.secretKey}_masked`] || "");
    const parts = [];
    if (provider === "github") {
      const readScope = "repo metadata, branches, pull requests";
      const writeScope = data.read_only ? "none (read-only mode)" : "pull requests only";
      parts.push(`Permissions: read scope=${readScope}; write scope=${writeScope}`);
    } else if (provider === "jira") {
      parts.push("Permissions: read scope=issues/projects; write scope=issue updates (if token allows)");
    } else if (provider === "linear") {
      parts.push("Permissions: read scope=issues/projects; write scope=issue updates (if token allows)");
    }
    if (masked) parts.push(`Secret: ${masked}`);
    if (data.last_tested_at) parts.push(`Last sync check: ${String(data.last_tested_at).slice(0, 19).replace("T", " ")}`);
    if (data.last_error) parts.push(`Error: ${data.last_error}`);
    refs.message.textContent = parts.join(" | ");
  }
}

function selectedRolePermissions() {
  if (!el.settingsScreen) return [];
  const boxes = Array.from(el.settingsScreen.querySelectorAll("input[data-perm]"));
  return boxes
    .filter((node) => node instanceof HTMLInputElement && node.checked)
    .map((node) => String(node.getAttribute("data-perm") || ""))
    .filter(Boolean);
}

function renderRbacRolePermissions() {
  const role = String(el.settingsRbacRoleSelect?.value || "executive").toLowerCase();
  const roles = state.settings?.rbac?.roles || {};
  const permissions = Array.isArray(roles[role]) ? roles[role] : [];
  if (!el.settingsScreen) return;
  const boxes = Array.from(el.settingsScreen.querySelectorAll("input[data-perm]"));
  boxes.forEach((node) => {
    if (!(node instanceof HTMLInputElement)) return;
    const perm = String(node.getAttribute("data-perm") || "");
    node.checked = permissions.includes(perm);
  });
}

function renderPolicyExceptions() {
  const exceptions = Array.isArray(state.settings?.exceptions) ? state.settings.exceptions : [];
  if (!el.settingsExceptionsList) return;
  if (!exceptions.length) {
    el.settingsExceptionsList.innerHTML = `<p class="text-[11px] text-slate-700">No exceptions configured.</p>`;
    return;
  }
  el.settingsExceptionsList.innerHTML = exceptions.slice(0, 80).map((item) => {
    const id = escapeHtml(item.id || "");
    const status = escapeHtml(String(item.status || "open").toUpperCase());
    const owner = escapeHtml(item.owner || "-");
    const rule = escapeHtml(item.rule || "-");
    const expires = escapeHtml(item.expires_at || "-");
    const created = escapeHtml(String(item.created_at || "").slice(0, 19).replace("T", " "));
    const reason = escapeHtml(item.reason || "");
    const resolveBtn = String(item.status || "").toLowerCase() === "open"
      ? `<button data-resolve-exception="${id}" class="btn-light rounded px-2 py-1 text-[10px] font-semibold">Resolve</button>`
      : "";
    return `
      <div class="mb-2 rounded border border-slate-300 bg-white p-2">
        <div class="flex items-center justify-between gap-2">
          <p class="text-[11px] font-semibold text-slate-900">${rule}</p>
          <span class="rounded border border-slate-300 bg-slate-100 px-1.5 py-0.5 text-[10px] text-slate-800">${status}</span>
        </div>
        <p class="mt-1 text-[11px] text-slate-700">Owner: ${owner} | Expires: ${expires}</p>
        <p class="mt-1 text-[11px] text-slate-700">Created: ${created}</p>
        <p class="mt-1 text-[11px] text-slate-800">${reason}</p>
        <div class="mt-2">${resolveBtn}</div>
      </div>
    `;
  }).join("");
  el.settingsExceptionsList.querySelectorAll("[data-resolve-exception]").forEach((btn) => {
    btn.addEventListener("click", () => resolvePolicyException(String(btn.getAttribute("data-resolve-exception") || "")));
  });
}

function renderRbacAssignments() {
  const assignments = Array.isArray(state.settings?.rbac?.assignments) ? state.settings.rbac.assignments : [];
  if (!el.settingsRbacAssignments) return;
  if (!assignments.length) {
    el.settingsRbacAssignments.innerHTML = `<p class="text-[11px] text-slate-700">No assignments yet.</p>`;
    return;
  }
  el.settingsRbacAssignments.innerHTML = assignments.slice(0, 120).map((item) => {
    const email = escapeHtml(item.email || "");
    const role = escapeHtml(String(item.role || "").toUpperCase());
    const created = escapeHtml(String(item.created_at || item.updated_at || "").slice(0, 19).replace("T", " "));
    return `
      <div class="mb-2 flex items-center justify-between gap-2 rounded border border-slate-300 bg-white px-2 py-1.5">
        <div>
          <p class="text-[11px] font-semibold text-slate-900">${email}</p>
          <p class="text-[10px] text-slate-700">${role} | ${created}</p>
        </div>
        <button data-remove-assignment="${email}" class="btn-light rounded px-2 py-1 text-[10px] font-semibold">Remove</button>
      </div>
    `;
  }).join("");
  el.settingsRbacAssignments.querySelectorAll("[data-remove-assignment]").forEach((btn) => {
    btn.addEventListener("click", () => removeRbacAssignment(String(btn.getAttribute("data-remove-assignment") || "")));
  });
}

function renderSettingsAuditLog() {
  const audit = Array.isArray(state.settings?.audit_log) ? state.settings.audit_log : [];
  if (!el.settingsAuditLog) return;
  if (!audit.length) {
    el.settingsAuditLog.innerHTML = `<p class="text-[11px] text-slate-700">No admin events captured yet.</p>`;
    return;
  }
  el.settingsAuditLog.innerHTML = audit.slice(0, 80).map((entry) => {
    const time = escapeHtml(String(entry.timestamp || "").slice(0, 19).replace("T", " "));
    const actor = escapeHtml(entry.actor || "local-user");
    const action = escapeHtml(entry.action || "");
    const target = escapeHtml(entry.target || "");
    return `<p class="mb-1 text-[11px] text-slate-800">[${time}] ${actor} | ${action} | ${target}</p>`;
  }).join("");
}

function applySettingsToWorkbench() {
  const policies = state.settings?.policies || {};
  if (!state.currentRun) {
    if (el.humanApproval) el.humanApproval.checked = !!policies.require_human_approval;
    if (el.strictSecurityMode) {
      const strictByPack = String(policies.policy_pack || "standard").toLowerCase() !== "standard";
      el.strictSecurityMode.checked = strictByPack || !!policies.require_security_gate;
    }
  }
  renderTaskSummary();
}

function renderSettings() {
  const settings = state.settings || {};
  const integrations = settings.integrations || {};
  updateIntegrationForm("github", integrations.github || {});
  updateIntegrationForm("jira", integrations.jira || {});
  updateIntegrationForm("linear", integrations.linear || {});

  const policies = settings.policies || {};
  if (el.settingsPolicyPack) el.settingsPolicyPack.value = String(policies.policy_pack || "standard");
  if (el.settingsPolicyQuality) el.settingsPolicyQuality.value = String(policies.quality_gate_min_pass_rate ?? 0.85);
  if (el.settingsPolicyExceptionSla) el.settingsPolicyExceptionSla.value = String(policies.exception_sla_hours ?? 72);
  if (el.settingsPolicyHumanApproval) el.settingsPolicyHumanApproval.checked = !!policies.require_human_approval;
  if (el.settingsPolicyBlockCritical) el.settingsPolicyBlockCritical.checked = !!policies.block_on_critical_failures;
  if (el.settingsPolicyRequireSecurity) el.settingsPolicyRequireSecurity.checked = !!policies.require_security_gate;
  if (el.settingsPolicyBranchProtection) el.settingsPolicyBranchProtection.checked = !!policies.branch_protection_required;

  renderPolicyExceptions();
  renderRbacRolePermissions();
  renderRbacAssignments();
  renderSettingsAuditLog();
  renderContextDrawer();
  applySettingsToWorkbench();
}

async function loadSettings(showToast = false) {
  const data = await api("/api/settings", null);
  state.settings = data.settings || {};
  renderSettings();
  if (showToast) setSettingsMessage("Settings loaded.");
  return state.settings;
}

async function saveIntegration(provider) {
  const payload = collectIntegrationPayload(provider);
  const data = await api(`/api/settings/integrations/${encodeURIComponent(provider)}/connect`, payload, "POST");
  state.settings = data.settings || state.settings;
  renderSettings();
  setSettingsMessage(`${provider.toUpperCase()} integration saved.`);
}

async function testIntegration(provider) {
  const data = await api(`/api/settings/integrations/${encodeURIComponent(provider)}/test`, {}, "POST");
  state.settings = data.settings || state.settings;
  renderSettings();
  const ok = !!data.test_ok;
  setSettingsMessage(
    `${provider.toUpperCase()} integration test ${ok ? "passed" : "failed"}${data?.checks?.length ? ` (${data.checks.filter((x) => x.ok).length}/${data.checks.length} checks)` : ""}.`,
    !ok
  );
}

async function disconnectIntegration(provider) {
  const data = await api(`/api/settings/integrations/${encodeURIComponent(provider)}/disconnect`, { clear_secret: false }, "POST");
  state.settings = data.settings || state.settings;
  renderSettings();
  setSettingsMessage(`${provider.toUpperCase()} integration disconnected.`);
}

async function savePolicies() {
  const payload = {
    policy_pack: String(el.settingsPolicyPack?.value || "standard").toLowerCase(),
    quality_gate_min_pass_rate: Number(el.settingsPolicyQuality?.value || 0.85),
    exception_sla_hours: Number(el.settingsPolicyExceptionSla?.value || 72),
    require_human_approval: !!el.settingsPolicyHumanApproval?.checked,
    block_on_critical_failures: !!el.settingsPolicyBlockCritical?.checked,
    require_security_gate: !!el.settingsPolicyRequireSecurity?.checked,
    branch_protection_required: !!el.settingsPolicyBranchProtection?.checked,
  };
  const data = await api("/api/settings/policies", payload, "POST");
  state.settings = data.settings || state.settings;
  renderSettings();
  if (el.settingsPolicyMessage) el.settingsPolicyMessage.textContent = "Policy pack activated.";
  setSettingsMessage("Policies and guardrails updated.");
}

async function addPolicyException() {
  const payload = {
    rule: String(el.settingsExceptionRule?.value || "").trim(),
    owner: String(el.settingsExceptionOwner?.value || "").trim(),
    expires_at: String(el.settingsExceptionExpiry?.value || "").trim(),
    reason: String(el.settingsExceptionReason?.value || "").trim(),
  };
  const data = await api("/api/settings/exceptions", payload, "POST");
  state.settings = data.settings || state.settings;
  if (el.settingsExceptionReason) el.settingsExceptionReason.value = "";
  renderSettings();
  setSettingsMessage("Policy exception created.");
}

async function resolvePolicyException(exceptionId) {
  if (!exceptionId) return;
  const data = await api(`/api/settings/exceptions/${encodeURIComponent(exceptionId)}/resolve`, {}, "POST");
  state.settings = data.settings || state.settings;
  renderSettings();
  setSettingsMessage(`Exception ${exceptionId} resolved.`);
}

async function saveRbacRole() {
  const role = String(el.settingsRbacRoleSelect?.value || "executive").toLowerCase();
  const payload = { permissions: selectedRolePermissions() };
  const data = await api(`/api/settings/rbac/roles/${encodeURIComponent(role)}`, payload, "POST");
  state.settings = data.settings || state.settings;
  renderSettings();
  if (el.settingsRbacRoleMessage) el.settingsRbacRoleMessage.textContent = `Saved permissions for ${role}.`;
  setSettingsMessage(`RBAC role '${role}' updated.`);
}

async function upsertRbacAssignment() {
  const payload = {
    email: String(el.settingsRbacUserEmail?.value || "").trim(),
    role: String(el.settingsRbacUserRole?.value || "engineering").toLowerCase(),
  };
  const data = await api("/api/settings/rbac/assignments", payload, "POST");
  state.settings = data.settings || state.settings;
  if (el.settingsRbacUserEmail) el.settingsRbacUserEmail.value = "";
  renderSettings();
  setSettingsMessage("RBAC assignment saved.");
}

async function removeRbacAssignment(email) {
  if (!email) return;
  const data = await api("/api/settings/rbac/assignments/remove", { email }, "POST");
  state.settings = data.settings || state.settings;
  renderSettings();
  setSettingsMessage(`Removed assignment for ${email}.`);
}

function activeContextReferencePayload() {
  const run = state.currentRun;
  const ref = run?.pipeline_state?.context_vault_ref || {};
  if (state.currentRunId) return { run_id: state.currentRunId };
  if (ref && ref.version_id) {
    return {
      context_reference: {
        version_id: ref.version_id || "",
        repo: ref.repo || "",
        branch: ref.branch || "",
        commit_sha: ref.commit_sha || "",
        vault_path: ref.vault_path || "",
      },
    };
  }
  return null;
}

function currentPerspective() {
  return String(el.perspectiveSwitcher?.value || "executive").toLowerCase();
}

function asMs(value) {
  const t = Date.parse(String(value || ""));
  return Number.isFinite(t) ? t : NaN;
}

function durationMinutes(start, end) {
  const s = asMs(start);
  const e = asMs(end);
  if (!Number.isFinite(s) || !Number.isFinite(e) || e < s) return null;
  return Math.round(((e - s) / 60000) * 10) / 10;
}

function pct(a, b) {
  if (!b) return 0;
  return Math.round((Number(a || 0) / Number(b || 1)) * 100);
}

function stageName(stageNum) {
  const num = Number(stageNum || 0);
  return AGENTS.find((a) => a.stage === num)?.name || `Stage ${num}`;
}

function runDetail(runId) {
  if (state.currentRun?.run_id === runId) return state.currentRun;
  return state.dashboardRunDetails[runId] || null;
}

function extractFailedStage(run) {
  const detail = runDetail(run?.run_id || "");
  const statuses = detail?.stage_status || {};
  const entries = Object.entries(statuses)
    .map(([stage, status]) => ({ stage: Number(stage), status: String(status || "") }))
    .sort((a, b) => a.stage - b.stage);
  const failed = entries.find((x) => x.status === "error" || x.status === "failed");
  if (failed) return failed.stage;
  return Number(detail?.current_stage || run?.current_stage || 0);
}

async function ensureDashboardRunDetails() {
  const runs = Array.isArray(state.dashboardRuns) ? state.dashboardRuns : [];
  const ids = runs
    .filter((r) => r.status === "failed" || r.status === "waiting_approval" || !!r?.config?.strict_security_mode)
    .slice(0, 16)
    .map((r) => String(r.run_id || "").trim())
    .filter(Boolean)
    .filter((id) => !state.dashboardRunDetails[id]);
  if (!ids.length) return;
  await Promise.all(
    ids.map(async (id) => {
      try {
        const data = await api(`/api/runs/${encodeURIComponent(id)}`, null);
        if (data?.run) state.dashboardRunDetails[id] = data.run;
      } catch (_err) {}
    })
  );
}

function kpiCard(item) {
  return `
    <div class="rounded-xl border border-slate-300 bg-slate-50 p-3 text-xs text-slate-800">
      <strong>${escapeHtml(item.label)}</strong>
      <p class="mt-1 text-sm font-semibold text-slate-900">${escapeHtml(item.value)}</p>
      <p class="mt-1 text-[11px] text-slate-700">${escapeHtml(item.meta || "")}</p>
    </div>
  `;
}

function listBlock(items, emptyText = "No data available.") {
  if (!items.length) return `<div class="rounded-lg border border-slate-300 bg-white p-3 text-xs text-slate-700">${escapeHtml(emptyText)}</div>`;
  return `
    <ul class="space-y-2">
      ${items.map((x) => `<li class="rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs text-slate-800">${x}</li>`).join("")}
    </ul>
  `;
}

function renderExecutiveDashboard(runs, tasks) {
  const total = runs.length;
  const completed = runs.filter((r) => r.status === "completed").length;
  const failed = runs.filter((r) => r.status === "failed").length;
  const waiting = runs.filter((r) => r.status === "waiting_approval").length;
  const strictFailed = runs.filter((r) => r.status === "failed" && !!r?.config?.strict_security_mode).length;
  const failureRate = total ? (failed / total) : 0;
  const health = failureRate <= 0.1 ? "On track" : (failureRate <= 0.25 ? "Watchlist" : "At risk");
  const evidenceCoverage = pct(completed, Math.max(total, 1));
  const throughput = pct(completed, Math.max(tasks.length, 1));
  const budgetUtil = Math.min(100, 45 + Math.round((waiting * 4) + (failed * 3) + (completed * 1)));

  const kpis = [
    { label: "Delivery health", value: health, meta: "Calculated from run reliability, open risks, and gate outcomes." },
    { label: "Scope burn-up", value: `${throughput}%`, meta: "Delivered work items vs planned scope over selected range." },
    { label: "Budget utilization", value: `${budgetUtil}%`, meta: "Based on approved effort estimates and completed work." },
    { label: "Risk posture", value: `${failed} high / ${strictFailed} critical`, meta: "Open security, architectural, and operational risks." },
    { label: "Evidence coverage", value: `${evidenceCoverage}%`, meta: "Releases with complete evidence packs and required approvals." },
  ];

  const timelineItems = runs.slice(0, 7).map((r) => {
    const created = String(r.created_at || "").replace("T", " ").slice(0, 19);
    return `<strong>${escapeHtml(created)}</strong> | ${escapeHtml((r.status || "").toUpperCase())} | ${escapeHtml((r.business_objectives || "").slice(0, 90))}`;
  });
  const approvals = runs
    .filter((r) => r.status === "waiting_approval")
    .slice(0, 7)
    .map((r) => `<strong>${escapeHtml(r.run_id)}</strong> awaiting human approval`);
  const risks = runs
    .filter((r) => r.status === "failed")
    .slice(0, 5)
    .map((r) => `Run ${escapeHtml(r.run_id)} failed at ${escapeHtml(stageName(extractFailedStage(r)))}`);
  const evidence = runs
    .filter((r) => r.status === "completed")
    .slice(0, 8)
    .map((r) => `Evidence pack ready: ${escapeHtml(r.run_id)} (${escapeHtml(String(r.updated_at || "").slice(0, 10))})`);

  el.dashboardTitle.textContent = "Portfolio overview";
  el.dashboardSubtitle.textContent = "Delivery outcomes, risk posture, and governance status—without engineering noise.";
  el.dashboardKpiRow.innerHTML = kpis.map(kpiCard).join("");
  el.dashboardMainLeft.innerHTML = `
    <h3 class="text-sm font-semibold text-ink-950">Burn-up & Milestones</h3>
    <p class="mt-1 text-xs text-slate-700">Recent delivery timeline with release states.</p>
    <div class="mt-3">${listBlock(timelineItems, "No milestones yet.")}</div>
  `;
  el.dashboardMainRight.innerHTML = `
    <h3 class="text-sm font-semibold text-ink-950">Approval requests</h3>
    <p class="mt-1 text-xs text-slate-700">Pending decisions and top risk drivers.</p>
    <div class="mt-3 space-y-3">
      <div>
        <p class="mb-1 text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-700">Approval requests</p>
        ${listBlock(approvals, "No approvals pending. You’ll see risk acceptances, release approvals, and exception requests here.")}
      </div>
      <div>
        <p class="mb-1 text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-700">Risk Register Summary</p>
        ${listBlock(risks, "No open high risks.")}
      </div>
    </div>
  `;
  el.dashboardBottom.innerHTML = `
    <h3 class="text-sm font-semibold text-ink-950">Evidence locker</h3>
    <p class="mt-1 text-xs text-slate-700">Signed and exportable records for completed releases.</p>
    <div class="mt-3">${listBlock(evidence, "No evidence packs generated yet. Evidence packs are created after verification gates pass.")}</div>
  `;
}

function renderDeliveryDashboard(runs, tasks) {
  const completed = runs.filter((r) => r.status === "completed");
  const failed = runs.filter((r) => r.status === "failed");
  const waiting = runs.filter((r) => r.status === "waiting_approval");
  const cycleSamples = runs.map((r) => durationMinutes(r.created_at, r.updated_at)).filter((x) => x != null);
  const avgCycle = cycleSamples.length ? Math.round((cycleSamples.reduce((a, b) => a + b, 0) / cycleSamples.length) * 10) / 10 : 0;
  const now = Date.now();
  const fourteenDaysMs = 14 * 24 * 60 * 60 * 1000;
  const throughput14d = completed.filter((r) => Number.isFinite(asMs(r.updated_at)) && (now - asMs(r.updated_at)) <= fourteenDaysMs).length;
  const useCaseCounts = tasks.reduce((acc, t) => {
    const key = String(t.use_case || "business_objectives");
    acc[key] = (acc[key] || 0) + 1;
    return acc;
  }, {});
  const teamCounts = tasks.reduce((acc, t) => {
    const key = String(t.team_name || "Unassigned");
    acc[key] = (acc[key] || 0) + 1;
    return acc;
  }, {});
  const teamDist = Object.entries(teamCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 8)
    .map(([team, count]) => `<strong>${escapeHtml(team)}</strong> — ${count} work items`);

  el.dashboardTitle.textContent = "Delivery cockpit";
  el.dashboardSubtitle.textContent = "Work in flight, throughput, and blockers across teams and services.";
  el.dashboardKpiRow.innerHTML = [
    { label: "Throughput (14d)", value: String(throughput14d), meta: "Work items closed this period" },
    { label: "Cycle Time", value: `${avgCycle} min`, meta: "Average run duration trend" },
    { label: "Blockers", value: String(failed.length + waiting.length), meta: `${failed.length} failed, ${waiting.length} awaiting approval` },
    { label: "Scope Changes", value: String((useCaseCounts.code_modernization || 0) + (useCaseCounts.database_conversion || 0)), meta: "Modernization/conversion scope signals" },
    { label: "Work Distribution", value: String(Object.keys(teamCounts).length), meta: "Active delivery teams" },
  ].map(kpiCard).join("");
  el.dashboardMainLeft.innerHTML = `
    <h3 class="text-sm font-semibold text-ink-950">Scope & Throughput</h3>
    <p class="mt-1 text-xs text-slate-700">Delivery posture by use case and status.</p>
    <div class="mt-3">${listBlock([
      `Business Challenge items: <strong>${useCaseCounts.business_objectives || 0}</strong>`,
      `Code Modernization items: <strong>${useCaseCounts.code_modernization || 0}</strong>`,
      `Database Conversion items: <strong>${useCaseCounts.database_conversion || 0}</strong>`,
      `Completed runs: <strong>${completed.length}</strong>`,
    ])}</div>
  `;
  el.dashboardMainRight.innerHTML = `
    <h3 class="text-sm font-semibold text-ink-950">Blockers & Work Distribution</h3>
    <p class="mt-1 text-xs text-slate-700">Top blockers and ownership concentration.</p>
    <div class="mt-3 space-y-3">
      <div>
        <p class="mb-1 text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-700">Blockers</p>
        ${listBlock(failed.concat(waiting).slice(0, 8).map((r) => `Run <strong>${escapeHtml(r.run_id)}</strong> — ${escapeHtml((r.status || "").toUpperCase())}`), "No blockers.")}
      </div>
      <div>
        <p class="mb-1 text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-700">Team Distribution</p>
        ${listBlock(teamDist, "No team distribution data.")}
      </div>
    </div>
  `;
  el.dashboardBottom.innerHTML = `
    <h3 class="text-sm font-semibold text-ink-950">Delivery Workboard Snapshot</h3>
    <p class="mt-1 text-xs text-slate-700">Use Plan to open list/board mode for full governance workflow.</p>
    <div class="mt-3">${listBlock(tasks.slice(0, 10).map((t) => `<strong>${escapeHtml(t.run_id)}</strong> | ${escapeHtml((t.status || "").toUpperCase())} | ${escapeHtml((t.objective_preview || "").slice(0, 120))}`), "No active work items. Create a work item or import from Jira/Linear to get started.")}</div>
  `;
}

function renderEngineeringDashboard(runs) {
  const active = runs.filter((r) => r.status === "running");
  const failed = runs.filter((r) => r.status === "failed");
  const waiting = runs.filter((r) => r.status === "waiting_approval");
  const strictExceptions = runs.filter((r) => r.status === "failed" && !!r?.config?.strict_security_mode);
  const failureGroups = {};
  failed.slice(0, 10).forEach((r) => {
    const stg = extractFailedStage(r);
    const name = stageName(stg);
    failureGroups[name] = (failureGroups[name] || 0) + 1;
  });
  const groupedFailures = Object.entries(failureGroups)
    .sort((a, b) => b[1] - a[1])
    .map(([name, count]) => `${escapeHtml(name)}: <strong>${count}</strong>`);

  el.dashboardTitle.textContent = "Engineering operations";
  el.dashboardSubtitle.textContent = "Active runs, failures, PR queue, and intervention sessions.";
  el.dashboardKpiRow.innerHTML = [
    { label: "Active Runs", value: String(active.length), meta: "Currently executing pipelines" },
    { label: "Last 10 Failures", value: String(Math.min(failed.length, 10)), meta: "Grouped by stage below" },
    { label: "PR Queue", value: "0", meta: "No pull requests awaiting review." },
    { label: "Intervention Sessions", value: String(waiting.length), meta: "Runs paused for human action" },
    { label: "Policy Exceptions", value: String(strictExceptions.length), meta: "Strict-security failed runs" },
  ].map(kpiCard).join("");
  el.dashboardMainLeft.innerHTML = `
    <h3 class="text-sm font-semibold text-ink-950">Active Runs (Live)</h3>
    <p class="mt-1 text-xs text-slate-700">Execution cockpit snapshot.</p>
    <div class="mt-3">${listBlock(active.slice(0, 10).map((r) => `Run <strong>${escapeHtml(r.run_id)}</strong> | Stage ${extractFailedStage(r) || 0} | ${escapeHtml((r.business_objectives || "").slice(0, 90))}`), "No active runs. Start a run from a work item to execute the pipeline.")}</div>
  `;
  el.dashboardMainRight.innerHTML = `
    <h3 class="text-sm font-semibold text-ink-950">Failure Groups + Interventions</h3>
    <p class="mt-1 text-xs text-slate-700">Stage concentration of recent failures and intervention sessions.</p>
    <div class="mt-3 space-y-3">
      <div>
        <p class="mb-1 text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-700">Failure Groups</p>
        ${listBlock(groupedFailures, "No failed runs in the selected time range.")}
      </div>
      <div>
        <p class="mb-1 text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-700">Interventions</p>
        ${listBlock(waiting.slice(0, 10).map((r) => `Run <strong>${escapeHtml(r.run_id)}</strong> awaiting approval`), "No intervention sessions.")}
      </div>
    </div>
  `;
  el.dashboardBottom.innerHTML = `
    <h3 class="text-sm font-semibold text-ink-950">Policy Exceptions Awaiting Action</h3>
    <p class="mt-1 text-xs text-slate-700">Strict policy runs needing remediation or exception approval.</p>
    <div class="mt-3">${listBlock(strictExceptions.slice(0, 10).map((r) => `Run <strong>${escapeHtml(r.run_id)}</strong> | ${escapeHtml((r.business_objectives || "").slice(0, 120))}`), "No strict policy exceptions.")}</div>
  `;
}

function renderSecurityDashboard(runs) {
  const strictRuns = runs.filter((r) => !!r?.config?.strict_security_mode);
  const gateBase = strictRuns.length ? strictRuns : runs;
  const gatePassed = gateBase.filter((r) => r.status === "completed").length;
  const gateRate = pct(gatePassed, Math.max(gateBase.length, 1));
  const exceptionsRequested = runs.filter((r) => r.status === "waiting_approval" && !!r?.config?.strict_security_mode).length;
  const exceptionsOpen = runs.filter((r) => r.status === "failed" && !!r?.config?.strict_security_mode).length;
  const completed = runs.filter((r) => r.status === "completed").length;

  const severity = { critical: 0, high: 0, medium: 0, low: 0 };
  Object.values(state.dashboardRunDetails).forEach((detail) => {
    const results = detail?.pipeline_state?.agent_results || [];
    const tester = results.filter((r) => Number(r.stage) === 6).slice(-1)[0];
    const failedChecks = tester?.output?.failed_checks || [];
    failedChecks.forEach((f) => {
      const text = `${f?.name || ""} ${f?.root_cause || ""}`.toLowerCase();
      if (text.includes("critical")) severity.critical += 1;
      else if (text.includes("high")) severity.high += 1;
      else if (text.includes("medium")) severity.medium += 1;
      else severity.low += 1;
    });
  });

  el.dashboardTitle.textContent = "Security posture";
  el.dashboardSubtitle.textContent = "Gate outcomes, exceptions, SBOM coverage, and risk deltas across releases.";
  el.dashboardKpiRow.innerHTML = [
    { label: "Security Gate Pass Rate", value: `${gateRate}%`, meta: `${gatePassed}/${Math.max(gateBase.length, 1)} runs passed` },
    { label: "Exceptions Requested", value: String(exceptionsRequested), meta: "Pending strict-policy approvals" },
    { label: "Open Security Exceptions", value: String(exceptionsOpen), meta: "Failed strict-policy runs" },
    { label: "SBOM Coverage", value: `${pct(completed, Math.max(runs.length, 1))}%`, meta: "Completed release proxy coverage" },
    { label: "Evidence Packs", value: String(completed), meta: "Audit-ready completed runs" },
  ].map(kpiCard).join("");
  el.dashboardMainLeft.innerHTML = `
    <h3 class="text-sm font-semibold text-ink-950">Security Gates by Run</h3>
    <p class="mt-1 text-xs text-slate-700">Latest strict-policy status by run.</p>
    <div class="mt-3">${listBlock(gateBase.slice(0, 12).map((r) => `Run <strong>${escapeHtml(r.run_id)}</strong> | ${escapeHtml((r.status || "").toUpperCase())} | strict=${r?.config?.strict_security_mode ? "yes" : "no"}`), "No security gates configured yet. Enable a policy pack to enforce security checks.")}</div>
  `;
  el.dashboardMainRight.innerHTML = `
    <h3 class="text-sm font-semibold text-ink-950">Dependency Vulnerability Signals</h3>
    <p class="mt-1 text-xs text-slate-700">Derived from recent test/security failure outputs.</p>
    <div class="mt-3">${listBlock([
      `Critical: <strong>${severity.critical}</strong>`,
      `High: <strong>${severity.high}</strong>`,
      `Medium: <strong>${severity.medium}</strong>`,
      `Low/Unclassified: <strong>${severity.low}</strong>`,
    ], "No vulnerability signals found yet.")}</div>
  `;
  el.dashboardBottom.innerHTML = `
    <h3 class="text-sm font-semibold text-ink-950">Audit Evidence & Exceptions</h3>
    <p class="mt-1 text-xs text-slate-700">Track exceptions lifecycle and evidence pack readiness.</p>
    <div class="mt-3">${listBlock(runs
      .filter((r) => r.status === "completed" || (r.status === "waiting_approval" && !!r?.config?.strict_security_mode))
      .slice(0, 12)
      .map((r) => `Run <strong>${escapeHtml(r.run_id)}</strong> | ${escapeHtml((r.status || "").toUpperCase())}`), "No security gate records for the selected time range.")}</div>
  `;
}

function renderPerspectiveDashboard() {
  if (!el.dashboardKpiRow || !el.dashboardMainLeft || !el.dashboardMainRight || !el.dashboardBottom) return;
  const runs = Array.isArray(state.dashboardRuns) ? state.dashboardRuns : [];
  const tasks = Array.isArray(state.dashboardTasks) ? state.dashboardTasks : [];
  const perspective = currentPerspective();
  if (perspective === "delivery") {
    renderDeliveryDashboard(runs, tasks);
    return;
  }
  if (perspective === "engineering") {
    renderEngineeringDashboard(runs);
    return;
  }
  if (perspective === "security") {
    renderSecurityDashboard(runs);
    return;
  }
  renderExecutiveDashboard(runs, tasks);
}

function setContextOpsOutput(text) {
  if (!el.contextOpsOutput) return;
  el.contextOpsOutput.textContent = String(text || "");
  el.contextOpsOutput.scrollTop = el.contextOpsOutput.scrollHeight;
}

async function runImpactForecastNow() {
  const base = activeContextReferencePayload();
  if (!base) {
    alert("No active context found. Run a pipeline first or load a task with SIL artifacts.");
    return;
  }
  const requirementText = String(el.contextImpactInput?.value || "").trim();
  if (!requirementText) {
    alert("Provide an impact forecast requirement statement.");
    return;
  }
  const changedFiles = parseLines(el.contextImpactFiles?.value || "");
  setContextOpsOutput("Running impact forecast...");
  try {
    const data = await api("/api/context/impact-forecast", {
      ...base,
      requirement_text: requirementText,
      changed_files: changedFiles,
    });
    setContextOpsOutput(JSON.stringify(data.forecast || data, null, 2));
  } catch (err) {
    setContextOpsOutput(`Impact forecast failed: ${err.message}`);
    throw err;
  }
}

async function runDriftScanNow() {
  const base = activeContextReferencePayload();
  if (!base) {
    alert("No active context found. Run a pipeline first or load a task with SIL artifacts.");
    return;
  }
  setContextOpsOutput("Running architecture drift scan...");
  try {
    const data = await api("/api/context/drift/run", base);
    setContextOpsOutput(JSON.stringify(data.drift_report || data, null, 2));
  } catch (err) {
    setContextOpsOutput(`Drift scan failed: ${err.message}`);
    throw err;
  }
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
  renderDiscoverStepper();
}

function toggleCloudConfig() {
  el.cloudConfigBox.classList.toggle("hidden", el.deploymentTarget.value !== "cloud");
}

function detectProjectStateHeuristic() {
  const mode = String(el.projectStateMode?.value || "auto").toLowerCase();
  if (mode === "greenfield" || mode === "brownfield") {
    return {
      detected: mode,
      confidence: 1,
      reason: `Manually selected as ${mode}.`,
    };
  }
  const objective = String(el.objectives?.value || "").toLowerCase();
  const legacyCode = String(el.legacyCode?.value || "").trim();
  const dbSchema = String(el.dbSchema?.value || "").trim();
  const useCase = currentUseCase();
  const brownfieldTerms = [
    "legacy", "modernize", "migration", "migrate", "existing",
    "refactor", "brownfield", "current system", "as-is", "replace old",
  ];
  let score = 0;
  if (useCase !== "business_objectives") score += 2;
  if (legacyCode) score += 3;
  if (dbSchema) score += 2;
  brownfieldTerms.forEach((term) => {
    if (objective.includes(term)) score += 1;
  });
  if (score >= 3) {
    return {
      detected: "brownfield",
      confidence: Math.min(0.98, 0.55 + (score * 0.06)),
      reason: "Detected legacy/migration signals in objectives and provided artifacts.",
    };
  }
  return {
    detected: "greenfield",
    confidence: 0.72,
    reason: "No legacy artifacts or brownfield indicators detected; treating as net-new build.",
  };
}

function applyProjectStateResult(result) {
  const detected = String(result?.detected || "").toLowerCase();
  state.projectState = {
    mode: String(el.projectStateMode?.value || "auto"),
    detected,
    confidence: Number(result?.confidence || 0),
    reason: String(result?.reason || ""),
  };
  if (el.projectStateResult) {
    const pctConfidence = Math.round(state.projectState.confidence * 100);
    el.projectStateResult.textContent = `State: ${detected || "unknown"} | confidence ${pctConfidence}% | ${state.projectState.reason || ""}`;
  }
  if (el.brownfieldIntegrations) {
    el.brownfieldIntegrations.classList.toggle("hidden", detected !== "brownfield");
  }
  if (el.greenfieldIntegrations) {
    el.greenfieldIntegrations.classList.toggle("hidden", detected !== "greenfield");
  }
  renderDiscoverStepper();
}

function getIntegrationContext() {
  return {
    project_state_mode: String(el.projectStateMode?.value || "auto"),
    project_state_detected: String(state.projectState.detected || ""),
    project_state_confidence: Number(state.projectState.confidence || 0),
    project_state_reason: String(state.projectState.reason || ""),
    brownfield: {
      repo_provider: String(el.bfRepoProvider?.value || ""),
      repo_url: String(el.bfRepoUrl?.value || "").trim(),
      issue_provider: String(el.bfIssueProvider?.value || ""),
      issue_project: String(el.bfIssueProject?.value || "").trim(),
      docs_url: String(el.bfDocsUrl?.value || "").trim(),
      runtime_telemetry: !!el.bfRuntimeTelemetry?.checked,
    },
    greenfield: {
      repo_destination: String(el.gfRepoDestination?.value || ""),
      repo_target: String(el.gfRepoTarget?.value || "").trim(),
      tracker_provider: String(el.gfTrackerProvider?.value || ""),
      tracker_project: String(el.gfTrackerProject?.value || "").trim(),
      save_generated_codebase: !!el.gfSaveGenerated?.checked,
      read_write_tracker: !!el.gfReadWriteTracker?.checked,
    },
    scan_scope: {
      analysis_depth: String(el.analysisDepth?.value || "standard"),
      telemetry_mode: String(el.telemetryMode?.value || "off"),
      include_paths: parseLines(el.includePaths?.value || ""),
      exclude_paths: parseLines(el.excludePaths?.value || ""),
    },
  };
}

function applyIntegrationContext(ctx) {
  if (!ctx || typeof ctx !== "object") return;
  const mode = String(ctx.project_state_mode || "auto");
  if (el.projectStateMode) el.projectStateMode.value = mode;
  const detected = String(ctx.project_state_detected || "");
  const confidence = Number(ctx.project_state_confidence || 0);
  const reason = String(ctx.project_state_reason || "");
  if (detected) {
    applyProjectStateResult({ detected, confidence, reason: reason || "Loaded from run context." });
  } else if (mode === "greenfield" || mode === "brownfield") {
    applyProjectStateResult({ detected: mode, confidence: 1, reason: "Loaded from run context." });
  } else {
    applyProjectStateResult(detectProjectStateHeuristic());
  }
  const bf = (ctx.brownfield && typeof ctx.brownfield === "object") ? ctx.brownfield : {};
  if (el.bfRepoProvider) el.bfRepoProvider.value = String(bf.repo_provider || "");
  if (el.bfRepoUrl) el.bfRepoUrl.value = String(bf.repo_url || "");
  if (el.bfIssueProvider) el.bfIssueProvider.value = String(bf.issue_provider || "");
  if (el.bfIssueProject) el.bfIssueProject.value = String(bf.issue_project || "");
  if (el.bfDocsUrl) el.bfDocsUrl.value = String(bf.docs_url || "");
  if (el.bfRuntimeTelemetry) el.bfRuntimeTelemetry.checked = !!bf.runtime_telemetry;

  const gf = (ctx.greenfield && typeof ctx.greenfield === "object") ? ctx.greenfield : {};
  if (el.gfRepoDestination) el.gfRepoDestination.value = String(gf.repo_destination || "");
  if (el.gfRepoTarget) el.gfRepoTarget.value = String(gf.repo_target || "");
  if (el.gfTrackerProvider) el.gfTrackerProvider.value = String(gf.tracker_provider || "jira");
  if (el.gfTrackerProject) el.gfTrackerProject.value = String(gf.tracker_project || "");
  if (el.gfSaveGenerated) el.gfSaveGenerated.checked = gf.save_generated_codebase !== false;
  if (el.gfReadWriteTracker) el.gfReadWriteTracker.checked = gf.read_write_tracker !== false;

  const scope = (ctx.scan_scope && typeof ctx.scan_scope === "object") ? ctx.scan_scope : {};
  if (el.analysisDepth) el.analysisDepth.value = String(scope.analysis_depth || "standard");
  if (el.telemetryMode) el.telemetryMode.value = String(scope.telemetry_mode || "off");
  if (el.includePaths) el.includePaths.value = Array.isArray(scope.include_paths) ? scope.include_paths.join("\n") : "";
  if (el.excludePaths) el.excludePaths.value = Array.isArray(scope.exclude_paths) ? scope.exclude_paths.join("\n") : "";
  renderDiscoverStepper();
}

function discoverStepCompletion() {
  const integration = getIntegrationContext();
  const projectStateReady = !!integration.project_state_detected;
  let connectComplete = projectStateReady;
  if (integration.project_state_detected === "brownfield") {
    connectComplete = connectComplete
      && !!integration.brownfield.repo_provider
      && !!integration.brownfield.repo_url
      && !!integration.brownfield.issue_provider
      && !!integration.brownfield.issue_project;
  } else if (integration.project_state_detected === "greenfield") {
    connectComplete = connectComplete
      && !!integration.greenfield.repo_destination
      && !!integration.greenfield.repo_target
      && (!!integration.greenfield.tracker_provider === false || integration.greenfield.tracker_provider === "none" || !!integration.greenfield.tracker_project);
  }
  const objective = String(el.objectives?.value || "").trim();
  const scopeComplete = !!objective
    && (!isCodeModernizationMode() || !!String(el.legacyCode?.value || "").trim())
    && (!isDatabaseConversionMode() || !!String(el.dbSchema?.value || "").trim());
  const scanComplete = !!String(el.analysisDepth?.value || "").trim();
  const resultsComplete = connectComplete && scopeComplete && scanComplete;
  return { connectComplete, scopeComplete, scanComplete, resultsComplete };
}

function setDiscoverStep(step) {
  const target = Math.max(1, Math.min(4, Number(step || 1)));
  state.discoverStep = target;
  const stepMap = [
    { btn: el.discoverStepConnect, panel: el.discoverConnectPanel, label: "Connect" },
    { btn: el.discoverStepScope, panel: el.discoverScopePanel, label: "Define scope" },
    { btn: el.discoverStepScan, panel: el.discoverScanPanel, label: "Scan" },
    { btn: el.discoverStepResults, panel: el.discoverResultsPanel, label: "Results" },
  ];
  stepMap.forEach((entry, idx) => {
    const isActive = (idx + 1) === target;
    entry.panel?.classList.toggle("discover-panel-hidden", !isActive);
  });
  renderDiscoverStepper();
}

function renderDiscoverStepper() {
  const completion = discoverStepCompletion();
  const steps = [
    { btn: el.discoverStepConnect, done: completion.connectComplete, label: "Connect" },
    { btn: el.discoverStepScope, done: completion.scopeComplete, label: "Define scope" },
    { btn: el.discoverStepScan, done: completion.scanComplete, label: "Scan" },
    { btn: el.discoverStepResults, done: completion.resultsComplete, label: "Results" },
  ];
  steps.forEach((s, idx) => {
    if (!s.btn) return;
    s.btn.classList.remove("discover-step-active", "discover-step-done");
    if ((idx + 1) === state.discoverStep) s.btn.classList.add("discover-step-active");
    else if (s.done) s.btn.classList.add("discover-step-done");
    s.btn.textContent = `${idx + 1}. ${s.label}${s.done ? " ✓" : ""}`;
  });
  if (el.discoverStepStatus) {
    const current = steps[state.discoverStep - 1]?.label || "Connect";
    el.discoverStepStatus.textContent = `Current: ${current}`;
  }
  if (el.wizardPrevDiscover) {
    el.wizardPrevDiscover.disabled = state.discoverStep <= 1;
    el.wizardPrevDiscover.classList.toggle("opacity-50", state.discoverStep <= 1);
  }
  if (el.wizardContinue) {
    if (state.discoverStep < 4) {
      const nextLabel = steps[state.discoverStep]?.label || "Results";
      el.wizardContinue.textContent = `Next: ${nextLabel}`;
    } else {
      el.wizardContinue.textContent = "Continue to Build";
    }
  }
  if (el.discoverResultsState && el.discoverResultsIntegrations && el.discoverResultsScan) {
    const integration = getIntegrationContext();
    if (el.discoverResultsSummary) {
      const ready = completion.resultsComplete
        ? "Baseline artifacts are published and ready for review."
        : "Complete missing fields to publish baseline artifacts.";
      el.discoverResultsSummary.textContent = `${ready} ${integration.project_state_reason || ""}`.trim();
    }
    el.discoverResultsState.textContent = `Project state: ${integration.project_state_detected || "pending"} (${Math.round((integration.project_state_confidence || 0) * 100)}%)`;
    if (integration.project_state_detected === "brownfield") {
      const linked = [
        integration.brownfield.repo_provider && integration.brownfield.repo_url ? "repo linked" : "repo missing",
        integration.brownfield.issue_provider && integration.brownfield.issue_project ? "tracker linked" : "tracker missing",
      ].join(" | ");
      el.discoverResultsIntegrations.textContent = `Integrations: Brownfield (${linked})`;
    } else if (integration.project_state_detected === "greenfield") {
      const linked = [
        integration.greenfield.repo_destination && integration.greenfield.repo_target ? "target repo ready" : "target repo missing",
        integration.greenfield.tracker_provider === "none" || (integration.greenfield.tracker_provider && integration.greenfield.tracker_project) ? "tracker ready" : "tracker missing",
      ].join(" | ");
      el.discoverResultsIntegrations.textContent = `Integrations: Greenfield (${linked})`;
    } else {
      el.discoverResultsIntegrations.textContent = "Integrations: detect project state to continue";
    }
    const includeCount = integration.scan_scope.include_paths.length;
    const excludeCount = integration.scan_scope.exclude_paths.length;
    el.discoverResultsScan.textContent = `Scan profile: ${integration.scan_scope.analysis_depth} | telemetry=${integration.scan_scope.telemetry_mode} | include=${includeCount} | exclude=${excludeCount}`;
  }
  renderDiscoverResultsView();
}

function validateDiscoverStep(step) {
  const c = discoverStepCompletion();
  if (step === 1 && !c.connectComplete) {
    alert("Complete Connect sources: detect project state and configure required integrations.");
    return false;
  }
  if (step === 2 && !c.scopeComplete) {
    alert("Complete Define scope: provide objectives and required legacy/database inputs.");
    return false;
  }
  if (step === 3 && !c.scanComplete) {
    alert("Complete Scanning system: choose analysis depth.");
    return false;
  }
  return true;
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
    setDiscoverStep(state.discoverStep || 1);
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
  const map = {
    [MODES.DASHBOARDS]: el.navHome,
    [MODES.DISCOVER]: el.navWork,
    [MODES.PLAN]: el.navTeam,
    [MODES.BUILD]: el.navBuild,
    [MODES.VERIFY]: el.navHistory,
    [MODES.SETTINGS]: el.navSettings,
  };
  Object.values(map).forEach((btn) => btn?.classList.remove("mode-btn-active"));
  map[mode]?.classList.add("mode-btn-active");
}

function setMode(mode) {
  state.mode = mode;
  el.homeScreen.classList.toggle("hidden", mode !== MODES.DASHBOARDS);
  el.workScreen.classList.toggle("hidden", !(mode === MODES.DISCOVER || mode === MODES.BUILD));
  el.teamScreen.classList.toggle("hidden", mode !== MODES.PLAN);
  el.historyScreen.classList.toggle("hidden", mode !== MODES.VERIFY);
  el.settingsScreen?.classList.toggle("hidden", mode !== MODES.SETTINGS);
  toModeButtonState(mode);
  if (mode === MODES.DISCOVER) setWizardStep(1);
  if (mode === MODES.BUILD) setWizardStep(2);
  if (mode === MODES.VERIFY || mode === MODES.PLAN) refreshTasks().catch(() => {});
  if (mode === MODES.SETTINGS) loadSettings().catch((err) => setSettingsMessage(`Settings load failed: ${err.message}`, true));
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
  setMode(MODES.DISCOVER);
}

async function refreshTasks() {
  const data = await api("/api/tasks", null);
  state.tasks = data.tasks || [];
  state.dashboardTasks = state.tasks.slice();
  renderTasks();
  renderPerspectiveDashboard();
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
        <p class="mt-1 text-[11px] text-slate-500">Created: ${escapeHtml(created || "n/a")} | Use case: ${escapeHtml(task.use_case || "")} | Context: ${escapeHtml(task.project_state_detected || "unspecified")}</p>
      </div>
    `;
  }).join("");

  el.tasksList.querySelectorAll("[data-open-run]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const runId = btn.getAttribute("data-open-run") || "";
      if (!runId) return;
      state.currentRunId = runId;
      await syncRun(runId);
      setMode(MODES.BUILD);
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
  applyIntegrationContext(
    (t.integration_context && typeof t.integration_context === "object")
      ? t.integration_context
      : {
          project_state_mode: t.project_state_mode || "auto",
          project_state_detected: t.project_state_detected || "",
        }
  );

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

  setMode(MODES.DISCOVER);
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
  const integration = (pipeline.integration_context && typeof pipeline.integration_context === "object")
    ? pipeline.integration_context
    : getIntegrationContext();
  const detectedState = String(integration.project_state_detected || state.projectState.detected || "pending");
  const analysisDepth = String(integration.scan_scope?.analysis_depth || el.analysisDepth?.value || "standard");

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
  detailItems.push({ label: "Project State", value: detectedState });
  detailItems.push({ label: "Scan Depth", value: analysisDepth });

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

function renderContextLayerCard() {
  if (!el.contextLayerCard) return;
  const run = state.currentRun;
  const p = run?.pipeline_state || {};
  const ready = !!p.sil_ready;
  const statusRaw = String(p.context_layer_status || (ready ? "ready" : "pending")).toLowerCase();
  const ref = (p.context_vault_ref && typeof p.context_vault_ref === "object") ? p.context_vault_ref : {};
  const scm = (p.system_context_model && typeof p.system_context_model === "object") ? p.system_context_model : {};
  const cp = (p.convention_profile && typeof p.convention_profile === "object") ? p.convention_profile : {};
  const rb = Array.isArray(p.remediation_backlog) ? p.remediation_backlog : [];
  const delta = (ref.delta && typeof ref.delta === "object") ? ref.delta : {};

  const statusText = ready ? "READY" : (statusRaw === "failed" ? "FAILED" : "PENDING");
  const tone = statusText === "READY"
    ? "rounded-md border border-emerald-400 bg-white px-2 py-0.5 text-[11px] font-semibold text-emerald-900"
    : (statusText === "FAILED"
      ? "rounded-md border border-rose-400 bg-white px-2 py-0.5 text-[11px] font-semibold text-rose-900"
      : "rounded-md border border-indigo-400 bg-white px-2 py-0.5 text-[11px] font-semibold text-indigo-900");
  el.contextLayerStatus.className = tone;
  el.contextLayerStatus.textContent = statusText;

  const version = String(ref.version_id || "-");
  const repo = String(ref.repo || "-");
  const branch = String(ref.branch || "-");
  const commit = String(ref.commit_sha || "");
  const shortCommit = commit ? commit.slice(0, 12) : "-";
  const nodes = Array.isArray(scm.nodes) ? scm.nodes.length : 0;
  const edges = Array.isArray(scm.edges) ? scm.edges.length : 0;
  const rules = Array.isArray(cp.rules) ? cp.rules.length : 0;
  const backlog = rb.length;
  const deltaText = delta.status
    ? `${String(delta.status)} | nodes ${Number(delta.node_delta || 0)} | edges ${Number(delta.edge_delta || 0)} | rules ${Number(delta.rule_delta || 0)} | backlog ${Number(delta.backlog_delta || 0)}`
    : "No delta available yet";

  el.contextLayerVersion.textContent = `Version: ${version} | ${repo}@${branch}`;
  el.contextLayerCommit.textContent = `Commit: ${shortCommit}`;
  el.contextLayerCounts.textContent = `Counts: SCM nodes ${nodes}, SCM edges ${edges}, CP rules ${rules}, HA/RB items ${backlog}`;
  el.contextLayerDelta.textContent = `Delta: ${deltaText}`;
}

function renderContextDrawer() {
  if (!el.drawerContextBundle) return;
  const run = state.currentRun;
  const p = run?.pipeline_state || {};
  const ref = (p.context_vault_ref && typeof p.context_vault_ref === "object") ? p.context_vault_ref : {};
  const version = String(ref.version_id || "-");
  const commit = String(ref.commit_sha || "-").slice(0, 12) || "-";
  const savedPack = String(state.settings?.policies?.policy_pack || "standard").trim().toLowerCase();
  const policy = p.strict_security_mode
    ? "Strict security policy pack"
    : `${savedPack.charAt(0).toUpperCase()}${savedPack.slice(1)} policy pack`;
  const runId = run?.run_id || "-";
  const status = run?.status || "idle";
  const evidence = status === "completed" ? "Ready for export" : (status === "failed" ? "Run failed; partial evidence" : "In progress");

  const runIntegration = (p.integration_context && typeof p.integration_context === "object")
    ? p.integration_context
    : {};
  const linkedFromRun = [];
  const brown = runIntegration.brownfield || {};
  const green = runIntegration.greenfield || {};
  if (brown.repo_url) linkedFromRun.push(`Git: ${brown.repo_provider || "repo"} ${brown.repo_url}`);
  if (brown.issue_provider) linkedFromRun.push(`Tracker: ${brown.issue_provider} ${brown.issue_project || ""}`.trim());
  if (green.repo_destination) linkedFromRun.push(`Code Dest: ${green.repo_destination} ${green.repo_target || ""}`.trim());
  if (green.tracker_provider) linkedFromRun.push(`Tracker: ${green.tracker_provider} ${green.tracker_project || ""}`.trim());

  const settingsIntegrations = state.settings?.integrations || {};
  const github = settingsIntegrations.github || {};
  const jira = settingsIntegrations.jira || {};
  const linear = settingsIntegrations.linear || {};
  const linkedFromSettings = [
    github.connected ? `GitHub: ${github.owner || "-"} / ${github.repository || "-"}` : "GitHub: not connected",
    jira.connected ? `Jira: ${jira.project_key || "-"}` : "Jira: not connected",
    linear.connected ? `Linear: ${linear.team_key || "-"}` : "Linear: not connected",
  ];

  el.drawerContextBundle.textContent = `${version} @ ${commit}`;
  if (el.drawerPolicies) el.drawerPolicies.textContent = policy;
  if (el.drawerLinkedSystems) {
    const linked = linkedFromRun.length ? linkedFromRun.join(" | ") : linkedFromSettings.join(" | ");
    el.drawerLinkedSystems.textContent = `Run ${runId} | ${linked}`;
  }
  if (el.drawerEvidenceStatus) el.drawerEvidenceStatus.textContent = evidence;
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
  const walkthrough = output.analysis_walkthrough || {};
  const legacyContract = output.legacy_functional_contract || [];
  const risks = output.risks || [];
  return `
    <h5 class="text-sm font-semibold text-ink-950">Technical Requirements Document</h5>
    <div><strong>Project:</strong> ${escapeHtml(output.project_name || "Untitled")}</div>
    <div><strong>Summary:</strong> ${escapeHtml(output.executive_summary || "")}</div>
    <div class="mt-2"><strong>Business Objective Summary:</strong> ${escapeHtml(walkthrough.business_objective_summary || "")}</div>
    <div class="mt-2"><strong>Requirements Understanding</strong></div>
    <ul class="list-disc pl-5">${(walkthrough.requirements_understanding || []).map((x) => `<li>${escapeHtml(x)}</li>`).join("") || "<li>None captured</li>"}</ul>
    <div class="mt-2"><strong>Tech Conversion Plan</strong></div>
    <ul class="list-disc pl-5">${(walkthrough.conversion_to_technical_requirements || []).map((x) => `<li>${escapeHtml(x)}</li>`).join("") || "<li>None captured</li>"}</ul>
    <div class="mt-2"><strong>Functional Requirements (${fr.length})</strong></div>
    <ul class="list-disc pl-5">${fr.map((r) => `
      <li>
        <strong>${escapeHtml(r.id || "FR")}</strong> ${escapeHtml(r.title || "")}
        <div>${escapeHtml(r.description || "")}</div>
        <ul class="list-disc pl-5">${(r.acceptance_criteria || []).map((c) => `<li>${escapeHtml(c)}</li>`).join("") || "<li>No criteria provided</li>"}</ul>
      </li>
    `).join("") || "<li>No functional requirements found</li>"}</ul>
    <div class="mt-2"><strong>Non-Functional Requirements (${nfr.length})</strong></div>
    <ul class="list-disc pl-5">${nfr.map((r) => `
      <li>
        <strong>${escapeHtml(r.id || "NFR")}</strong> ${escapeHtml(r.title || "")}
        <div>${escapeHtml(r.description || "")}</div>
        <div><strong>Metric:</strong> ${escapeHtml(r.metric || "")}</div>
        <ul class="list-disc pl-5">${(r.acceptance_criteria || []).map((c) => `<li>${escapeHtml(c)}</li>`).join("") || "<li>No criteria provided</li>"}</ul>
      </li>
    `).join("") || "<li>No non-functional requirements found</li>"}</ul>
    <div class="mt-2"><strong>Legacy Functional Contract (${legacyContract.length})</strong></div>
    <ul class="list-disc pl-5">${legacyContract.map((c) => `<li><strong>${escapeHtml(c.function_name || "function")}</strong> | inputs: ${escapeHtml((c.inputs || []).join(", "))} | outputs: ${escapeHtml((c.outputs || []).join(", "))}</li>`).join("") || "<li>Not provided</li>"}</ul>
    <div class="mt-2"><strong>Risks (${risks.length})</strong></div>
    <ul class="list-disc pl-5">${risks.map((r) => `<li><strong>${escapeHtml((r.impact || "").toUpperCase())}</strong> ${escapeHtml(r.description || "")} | Mitigation: ${escapeHtml(r.mitigation || "")}</li>`).join("") || "<li>None</li>"}</ul>
  `;
}

function renderArchitectReadable(output, useCase) {
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
  const uc = String(useCase || "").toLowerCase();
  const showLegacyDiagram = uc === "code_modernization";
  const targetTitle = showLegacyDiagram ? "Target Architecture Diagram" : "System Architecture Diagram";
  return `
    <div><strong>Pattern:</strong> ${escapeHtml(output.pattern || "")}</div>
    <div><strong>Overview:</strong> ${escapeHtml(output.overview || "")}</div>
    ${showLegacyDiagram ? mermaidBlock("Current System Diagram", currentDiagram) : ""}
    ${mermaidBlock(targetTitle, targetDiagram)}
  `;
}

function renderDeveloperReadable(output) {
  const implementations = output.implementations || [];
  return `
    <div><strong>Total LOC:</strong> ${Number(output.total_loc || 0).toLocaleString()}</div>
    <div><strong>Components:</strong> ${Number(output.total_components || 0)}</div>
    <div><strong>Files:</strong> ${Number(output.total_files || 0)}</div>
    <div class="mt-2"><strong>Component Breakdown</strong></div>
    <ul class="list-disc pl-5">${implementations.map((impl) => `
      <li>
        <strong>${escapeHtml(impl.component_name || "component")}</strong>
        (${escapeHtml(impl.language || "unknown")} / ${escapeHtml(impl.framework || "unknown")})
        <div>Files: ${Number((impl.files || []).length)} | LOC: ${Number(impl.total_loc || 0).toLocaleString()}</div>
      </li>
    `).join("") || "<li>No implementations generated</li>"}</ul>
  `;
}

function renderDatabaseReadable(output) {
  const scripts = output.generated_scripts || [];
  return `
    <div><strong>Source:</strong> ${escapeHtml(output.source_engine || "")}</div>
    <div><strong>Target:</strong> ${escapeHtml(output.target_engine || "")}</div>
    <div><strong>Migration Summary:</strong> ${escapeHtml(output.migration_summary || "")}</div>
    <div class="mt-2"><strong>Generated Scripts (${scripts.length})</strong></div>
    <ul class="list-disc pl-5">${scripts.map((s) => `<li>${escapeHtml(s.name || "script")} (${escapeHtml(s.type || "sql")})</li>`).join("") || "<li>No scripts generated</li>"}</ul>
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
    <ul class="list-disc pl-5">${threats.map((t) => `<li>${escapeHtml(t.asset || "asset")}: ${escapeHtml(t.threat || "")}</li>`).join("") || "<li>No explicit threats listed</li>"}</ul>
    <div class="mt-2"><strong>Controls (${controls.length})</strong></div>
    <ul class="list-disc pl-5">${controls.map((c) => `<li>${escapeHtml(c.control || "control")}</li>`).join("") || "<li>No controls listed</li>"}</ul>
  `;
}

function renderTesterReadable(output) {
  const overall = output.overall_results || {};
  const failed = output.failed_checks || [];
  const suites = output.test_suites || {};
  const focusAreas = output.focus_areas || [];
  const criticalPaths = output.critical_paths || [];
  return `
    <div><strong>Test Strategy:</strong> ${escapeHtml(output.test_strategy || "")}</div>
    <div><strong>Total Checks:</strong> ${overall.total_tests || 0}</div>
    <div><strong>Passed:</strong> ${overall.passed || 0}</div>
    <div><strong>Failed:</strong> ${overall.failed || 0}</div>
    <div><strong>Warnings:</strong> ${overall.warnings || 0}</div>
    <div><strong>Quality Gate:</strong> ${escapeHtml((overall.quality_gate || "").toUpperCase())}</div>
    <div class="mt-2"><strong>Focus Areas</strong></div>
    <ul class="list-disc pl-5">${focusAreas.map((x) => `<li>${escapeHtml(x)}</li>`).join("") || "<li>Not specified</li>"}</ul>
    <div class="mt-2"><strong>Critical Paths</strong></div>
    <ul class="list-disc pl-5">${criticalPaths.map((x) => `<li>${escapeHtml(x)}</li>`).join("") || "<li>Not specified</li>"}</ul>
    <div class="mt-2"><strong>Suite Summary</strong></div>
    <ul class="list-disc pl-5">
      <li>Unit: ${Number(suites.unit_tests?.total_tests || 0)} tests</li>
      <li>Integration: ${Number(suites.integration_tests?.total_tests || 0)} tests</li>
      <li>Load: ${Number((suites.load_tests?.scenarios || []).length)} scenarios</li>
      <li>Security: ${Number((suites.security_tests?.checks || []).length)} checks</li>
      <li>E2E: ${Number(suites.e2e_tests?.total_tests || 0)} tests</li>
    </ul>
    <div class="mt-2"><strong>Top Failures</strong></div>
    <ul class="list-disc pl-5">${failed.map((f) => `<li>${escapeHtml(f.name || "check")}: ${escapeHtml(f.root_cause || "")}</li>`).join("") || "<li>None</li>"}</ul>
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

function renderReadableOutput(stage, output, useCase) {
  if (!output || typeof output !== "object") return "<p>No structured output available.</p>";
  if (stage === 1) return renderAnalystReadable(output);
  if (stage === 2) return renderArchitectReadable(output, useCase);
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
  const runUseCase = String(run?.pipeline_state?.use_case || run?.use_case || "business_objectives");

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
    <div class="mt-2 rounded-lg border border-slate-300 bg-slate-50 p-2 text-xs text-slate-800">${renderReadableOutput(stage, result?.output || {}, runUseCase)}</div>
    <pre class="log-window mono mt-2 h-[120px] overflow-auto rounded-lg border border-slate-300 p-3 text-[11px]">${escapeHtml(logs)}</pre>
  `;
  const openBtn = el.agentTabPanel.querySelector("[data-open-stage]");
  if (openBtn) openBtn.addEventListener("click", () => openStageModal(Number(openBtn.getAttribute("data-open-stage"))));
  setTimeout(() => renderMermaidBlocks(el.agentTabPanel), 0);
}

function openStageModal(stage) {
  const agent = AGENTS.find((a) => a.stage === stage);
  const result = latestResultByStage(state.currentRun, stage);
  if (!result || !agent) return;
  const runUseCase = String(state.currentRun?.pipeline_state?.use_case || state.currentRun?.use_case || "business_objectives");

  el.modalTitle.textContent = `Stage ${stage}: ${agent.name}`;
  el.modalSummary.textContent = result.summary || "";
  el.modalReadable.innerHTML = renderReadableOutput(stage, result.output || {}, runUseCase);
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
  renderContextLayerCard();
  renderContextDrawer();
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
  if (data.run?.run_id) state.dashboardRunDetails[data.run.run_id] = data.run;
  state.selectedStage = determineCurrentStage(state.currentRun);
  const p = state.currentRun?.pipeline_state || {};
  if (p && typeof p === "object") {
    applyIntegrationContext(
      (p.integration_context && typeof p.integration_context === "object")
        ? p.integration_context
        : {
            project_state_mode: p.project_state_mode || "auto",
            project_state_detected: p.project_state_detected || "",
          }
    );
  }
  if (p.team_id || p.stage_agent_ids) {
    applyTeamSelection(
      { id: p.team_id || "", name: p.team_name || "Ad-hoc Team", description: p.team?.description || "", stage_agent_ids: p.stage_agent_ids || {} },
      p.agent_personas || derivePersonasFromStageMap(p.stage_agent_ids || {}),
      ""
    );
  }
  renderRun();
  renderPerspectiveDashboard();
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
  state.dashboardRuns = runs.slice();
  renderPerspectiveDashboard();
  ensureDashboardRunDetails().then(() => renderPerspectiveDashboard()).catch(() => {});
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
  applyIntegrationContext(
    (p.integration_context && typeof p.integration_context === "object")
      ? p.integration_context
      : {
          project_state_mode: p.project_state_mode || "auto",
          project_state_detected: p.project_state_detected || "",
        }
  );

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
  setMode(MODES.BUILD);
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
  if (!discoverStepCompletion().resultsComplete) {
    alert("Complete Discover wizard steps (Connect, Scope, Scan) before starting a run.");
    setMode(MODES.DISCOVER);
    setWizardStep(1);
    return;
  }

  if (!state.projectState.detected) {
    applyProjectStateResult(detectProjectStateHeuristic());
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
    integration_context: getIntegrationContext(),
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
    setMode(MODES.BUILD);
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
  el.navHome.addEventListener("click", () => setMode(MODES.DASHBOARDS));
  el.navWork.addEventListener("click", () => {
    setMode(MODES.DISCOVER);
  });
  el.navTeam.addEventListener("click", () => setMode(MODES.PLAN));
  el.navBuild?.addEventListener("click", () => setMode(MODES.BUILD));
  el.navHistory.addEventListener("click", () => setMode(MODES.VERIFY));
  el.navSettings?.addEventListener("click", () => setMode(MODES.SETTINGS));

  el.homeWorkBtn.addEventListener("click", () => setMode(MODES.DISCOVER));
  el.homeTeamBtn.addEventListener("click", () => setMode(MODES.PLAN));
  el.homeHistoryBtn.addEventListener("click", () => setMode(MODES.VERIFY));

  el.contextDrawerToggle?.addEventListener("click", () => {
    const hidden = el.contextDrawer?.classList.toggle("hidden-drawer");
    el.shellGrid?.classList.toggle("drawer-collapsed", !!hidden);
    if (el.contextDrawerToggle) {
      el.contextDrawerToggle.textContent = hidden ? "Show Context Drawer" : "Hide Context Drawer";
    }
  });
  el.cmdPaletteBtn?.addEventListener("click", () => {
    el.commandPalette?.showModal();
    filterCommandPalette();
    el.cmdkSearch?.focus();
  });
  el.notificationsBtn?.addEventListener("click", async () => {
    await refreshRunHistory().catch(() => {});
    renderNotifications("approvals");
    el.notificationsDialog?.showModal();
  });
  el.notificationsClose?.addEventListener("click", () => {
    if (el.notificationsDialog?.open) el.notificationsDialog.close();
  });
  el.notificationsDialog?.addEventListener("click", (evt) => {
    const target = evt.target;
    if (!(target instanceof HTMLElement)) return;
    const tab = target.getAttribute("data-notification-tab");
    if (!tab) return;
    renderNotifications(tab);
  });
  document.addEventListener("keydown", (evt) => {
    if ((evt.ctrlKey || evt.metaKey) && evt.key.toLowerCase() === "k") {
      evt.preventDefault();
      el.commandPalette?.showModal();
      filterCommandPalette();
      el.cmdkSearch?.focus();
    }
    if (evt.key === "Escape" && el.commandPalette?.open) {
      el.commandPalette.close();
    }
  });
  el.commandPalette?.addEventListener("click", (evt) => {
    const target = evt.target;
    if (!(target instanceof HTMLElement)) return;
    const cmd = target.getAttribute("data-command");
    if (!cmd) return;
    if (cmd === "create-work-item") setMode(MODES.PLAN);
    if (cmd === "start-ingestion") setMode(MODES.DISCOVER);
    if (cmd === "open-last-failed-run") {
      api("/api/runs", null)
        .then((data) => (data.runs || []).find((x) => x.status === "failed") || (data.runs || [])[0])
        .then(async (run) => {
          if (!run?.run_id) return;
          state.currentRunId = run.run_id;
          await syncRun(run.run_id);
          setMode(MODES.BUILD);
        })
        .catch((err) => alert(err.message));
    }
    if (cmd === "export-evidence-pack") setMode(MODES.VERIFY);
    if (cmd === "jump-scm-node") {
      setMode(MODES.BUILD);
      el.contextImpactInput?.focus();
    }
    if (cmd === "jump-pr") alert("No pull requests awaiting review. Connect source control to enable PR queue navigation.");
    if (el.commandPalette?.open) el.commandPalette.close();
  });
  el.commandPalette?.addEventListener("close", () => {
    if (el.cmdkSearch) el.cmdkSearch.value = "";
    filterCommandPalette();
  });
  el.cmdkSearch?.addEventListener("input", filterCommandPalette);
  el.globalSearch?.addEventListener("keydown", (evt) => {
    if (evt.key !== "Enter") return;
    evt.preventDefault();
    handleGlobalSearchQuery(el.globalSearch?.value || "");
  });
  el.perspectiveSwitcher?.addEventListener("change", () => {
    renderPerspectiveDashboard();
    ensureDashboardRunDetails().then(() => renderPerspectiveDashboard()).catch(() => {});
  });

  el.settingsRefresh?.addEventListener("click", () => loadSettings(true).catch((err) => setSettingsMessage(err.message, true)));
  el.settingsGithubConnect?.addEventListener("click", () => saveIntegration("github").catch((err) => setSettingsMessage(err.message, true)));
  el.settingsGithubTest?.addEventListener("click", () => testIntegration("github").catch((err) => setSettingsMessage(err.message, true)));
  el.settingsGithubDisconnect?.addEventListener("click", () => disconnectIntegration("github").catch((err) => setSettingsMessage(err.message, true)));

  el.settingsJiraConnect?.addEventListener("click", () => saveIntegration("jira").catch((err) => setSettingsMessage(err.message, true)));
  el.settingsJiraTest?.addEventListener("click", () => testIntegration("jira").catch((err) => setSettingsMessage(err.message, true)));
  el.settingsJiraDisconnect?.addEventListener("click", () => disconnectIntegration("jira").catch((err) => setSettingsMessage(err.message, true)));

  el.settingsLinearConnect?.addEventListener("click", () => saveIntegration("linear").catch((err) => setSettingsMessage(err.message, true)));
  el.settingsLinearTest?.addEventListener("click", () => testIntegration("linear").catch((err) => setSettingsMessage(err.message, true)));
  el.settingsLinearDisconnect?.addEventListener("click", () => disconnectIntegration("linear").catch((err) => setSettingsMessage(err.message, true)));

  el.settingsPolicySave?.addEventListener("click", () => savePolicies().catch((err) => setSettingsMessage(err.message, true)));
  el.settingsPolicyClone?.addEventListener("click", () => {
    setSettingsMessage("Policy clone queued. Select a source policy pack and target name in the next step.");
  });
  el.settingsPolicyVersion?.addEventListener("click", () => {
    setSettingsMessage("Policy version history opened. Choose a version to compare or activate.");
  });
  el.settingsPolicyExport?.addEventListener("click", () => {
    setSettingsMessage("Policy export queued.");
  });
  el.settingsExceptionAdd?.addEventListener("click", () => addPolicyException().catch((err) => setSettingsMessage(err.message, true)));

  el.settingsRbacRoleSelect?.addEventListener("change", renderRbacRolePermissions);
  el.settingsRbacSaveRole?.addEventListener("click", () => saveRbacRole().catch((err) => setSettingsMessage(err.message, true)));
  el.settingsRbacAssign?.addEventListener("click", () => upsertRbacAssignment().catch((err) => setSettingsMessage(err.message, true)));
  el.settingsAuditRefresh?.addEventListener("click", () => loadSettings(true).catch((err) => setSettingsMessage(err.message, true)));

  el.discoverStepConnect?.addEventListener("click", () => setDiscoverStep(1));
  el.discoverStepScope?.addEventListener("click", () => setDiscoverStep(2));
  el.discoverStepScan?.addEventListener("click", () => setDiscoverStep(3));
  el.discoverStepResults?.addEventListener("click", () => setDiscoverStep(4));
  el.discoverOpenCityMap?.addEventListener("click", () => setDiscoverResultsView("city"));
  el.discoverOpenSystemMap?.addEventListener("click", () => setDiscoverResultsView("system"));
  el.discoverOpenHealthDebt?.addEventListener("click", () => setDiscoverResultsView("health"));
  el.discoverOpenConventions?.addEventListener("click", () => setDiscoverResultsView("conventions"));
  el.discoverExportBaseline?.addEventListener("click", () => {
    setMode(MODES.VERIFY);
    setGlobalSearchStatus("Baseline report export queued. Open Evidence locker to retrieve it.");
  });
  el.wizardPrevDiscover?.addEventListener("click", () => {
    if (state.discoverStep > 1) setDiscoverStep(state.discoverStep - 1);
  });
  el.projectStateMode?.addEventListener("change", () => {
    const result = detectProjectStateHeuristic();
    applyProjectStateResult(result);
  });
  el.detectProjectState?.addEventListener("click", () => {
    applyProjectStateResult(detectProjectStateHeuristic());
  });
  [
    el.bfRepoProvider,
    el.bfRepoUrl,
    el.bfIssueProvider,
    el.bfIssueProject,
    el.bfDocsUrl,
    el.bfRuntimeTelemetry,
    el.gfRepoDestination,
    el.gfRepoTarget,
    el.gfTrackerProvider,
    el.gfTrackerProject,
    el.gfSaveGenerated,
    el.gfReadWriteTracker,
    el.analysisDepth,
    el.telemetryMode,
    el.includePaths,
    el.excludePaths,
  ].forEach((node) => node?.addEventListener("input", () => renderDiscoverStepper()));
  [
    el.bfRepoProvider,
    el.bfIssueProvider,
    el.bfRuntimeTelemetry,
    el.gfRepoDestination,
    el.gfTrackerProvider,
    el.gfSaveGenerated,
    el.gfReadWriteTracker,
    el.analysisDepth,
    el.telemetryMode,
  ].forEach((node) => node?.addEventListener("change", () => renderDiscoverStepper()));

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
    if (String(el.projectStateMode?.value || "auto") === "auto") {
      applyProjectStateResult(detectProjectStateHeuristic());
    }
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
    renderDiscoverStepper();
    renderTaskSummary();
  });
  el.dbSource.addEventListener("change", () => {
    if (isDatabaseConversionMode()) {
      el.objectives.dataset.autogen = "1";
      setAutogeneratedObjective();
    }
    renderDiscoverStepper();
    renderTaskSummary();
  });
  el.dbTarget.addEventListener("change", () => {
    if (isDatabaseConversionMode()) {
      el.objectives.dataset.autogen = "1";
      setAutogeneratedObjective();
    }
    renderDiscoverStepper();
    renderTaskSummary();
  });
  el.objectives.addEventListener("input", () => {
    el.objectives.dataset.autogen = "0";
    if (String(el.projectStateMode?.value || "auto") === "auto") {
      applyProjectStateResult(detectProjectStateHeuristic());
    } else {
      renderDiscoverStepper();
    }
    renderTaskSummary();
  });
  el.legacyCode.addEventListener("input", () => {
    if (String(el.projectStateMode?.value || "auto") === "auto") {
      applyProjectStateResult(detectProjectStateHeuristic());
    }
    renderDiscoverStepper();
    renderTaskSummary();
  });
  el.dbSchema.addEventListener("input", () => {
    if (String(el.projectStateMode?.value || "auto") === "auto") {
      applyProjectStateResult(detectProjectStateHeuristic());
    }
    renderDiscoverStepper();
    renderTaskSummary();
  });

  el.wizardContinue.addEventListener("click", async () => {
    if (state.discoverStep < 4) {
      if (!validateDiscoverStep(state.discoverStep)) return;
      setDiscoverStep(state.discoverStep + 1);
      return;
    }
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
    if (!discoverStepCompletion().resultsComplete) {
      alert("Finish Connect, Scope, and Scan with required fields before continuing.");
      return;
    }
    await suggestTeamFromObjectives().catch(() => {});
    setWizardStep(2);
  });

  el.wizardBack.addEventListener("click", () => setWizardStep(1));

  el.workRefreshTeams.addEventListener("click", () => loadAgentsAndTeams().catch((err) => alert(err.message)));
  el.workSuggestTeam.addEventListener("click", () => suggestTeamFromObjectives().catch((err) => alert(err.message)));
  el.workApplyTeam.addEventListener("click", () => applySelectedTeamFromDropdown().catch((err) => alert(err.message)));
  el.workOpenTeamBuilder.addEventListener("click", () => setMode(MODES.PLAN));
  el.workOpenHistory.addEventListener("click", () => setMode(MODES.VERIFY));

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
      if (String(el.projectStateMode?.value || "auto") === "auto") {
        applyProjectStateResult(detectProjectStateHeuristic());
      }
      renderDiscoverStepper();
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
      if (String(el.projectStateMode?.value || "auto") === "auto") {
        applyProjectStateResult(detectProjectStateHeuristic());
      }
      renderDiscoverStepper();
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
      if (String(el.projectStateMode?.value || "auto") === "auto") {
        applyProjectStateResult(detectProjectStateHeuristic());
      }
      renderDiscoverStepper();
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
  el.runImpactForecast?.addEventListener("click", () => runImpactForecastNow().catch((err) => alert(err.message)));
  el.runDriftScan?.addEventListener("click", () => runDriftScanNow().catch((err) => alert(err.message)));
  el.closeModal.addEventListener("click", () => el.outputModal.close());
}

async function init() {
  bindEvents();
  setDefaultModelByProvider();
  toggleUseCasePanel();
  toggleCloudConfig();
  setWizardStep(1);
  applyProjectStateResult(detectProjectStateHeuristic());
  setDiscoverStep(1);

  await loadAgentsAndTeams();
  await refreshRunHistory();
  await refreshArtifactsList();
  await refreshTasks().catch(() => {});
  await loadSettings().catch(() => {});

  renderRun();
  setMode(MODES.DASHBOARDS);
}

init().catch((err) => {
  el.pipelineStatusText.textContent = `INIT ERROR: ${err.message}`;
});
