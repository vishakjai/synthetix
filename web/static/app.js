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
  brandHomeBtn: document.getElementById("brand-home-btn"),
  brandLogoSidebar: document.getElementById("brand-logo-sidebar"),
  brandLogoHero: document.getElementById("brand-logo-hero"),
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

  settingsLlmAnthropicStatus: document.getElementById("settings-llm-anthropic-status"),
  settingsLlmAnthropicModel: document.getElementById("settings-llm-anthropic-model"),
  settingsLlmAnthropicBaseUrl: document.getElementById("settings-llm-anthropic-base-url"),
  settingsLlmAnthropicKey: document.getElementById("settings-llm-anthropic-key"),
  settingsLlmAnthropicConnect: document.getElementById("settings-llm-anthropic-connect"),
  settingsLlmAnthropicTest: document.getElementById("settings-llm-anthropic-test"),
  settingsLlmAnthropicDisconnect: document.getElementById("settings-llm-anthropic-disconnect"),
  settingsLlmAnthropicMessage: document.getElementById("settings-llm-anthropic-message"),

  settingsLlmOpenaiStatus: document.getElementById("settings-llm-openai-status"),
  settingsLlmOpenaiModel: document.getElementById("settings-llm-openai-model"),
  settingsLlmOpenaiBaseUrl: document.getElementById("settings-llm-openai-base-url"),
  settingsLlmOpenaiKey: document.getElementById("settings-llm-openai-key"),
  settingsLlmOpenaiConnect: document.getElementById("settings-llm-openai-connect"),
  settingsLlmOpenaiTest: document.getElementById("settings-llm-openai-test"),
  settingsLlmOpenaiDisconnect: document.getElementById("settings-llm-openai-disconnect"),
  settingsLlmOpenaiMessage: document.getElementById("settings-llm-openai-message"),

  settingsGithubStatus: document.getElementById("settings-github-status"),
  settingsGithubBaseUrl: document.getElementById("settings-github-base-url"),
  settingsGithubOwner: document.getElementById("settings-github-owner"),
  settingsGithubRepository: document.getElementById("settings-github-repository"),
  settingsGithubToken: document.getElementById("settings-github-token"),
  settingsGithubReadOnly: document.getElementById("settings-github-read-only"),
  settingsGithubRunExportEnabled: document.getElementById("settings-github-run-export-enabled"),
  settingsGithubExportBaseUrl: document.getElementById("settings-github-export-base-url"),
  settingsGithubExportOwner: document.getElementById("settings-github-export-owner"),
  settingsGithubExportRepository: document.getElementById("settings-github-export-repository"),
  settingsGithubExportBranch: document.getElementById("settings-github-export-branch"),
  settingsGithubExportPrefix: document.getElementById("settings-github-export-prefix"),
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
  discoverCityMapContent: document.getElementById("discover-city-map-content"),
  discoverSystemMapContent: document.getElementById("discover-system-map-content"),
  discoverHealthContent: document.getElementById("discover-health-content"),
  discoverConventionsContent: document.getElementById("discover-conventions-content"),
  cityMapSvg: document.getElementById("city-map-svg"),
  cityMapInspector: document.getElementById("city-map-inspector"),
  cityMapReset: document.getElementById("city-map-reset"),
  systemMapSvg: document.getElementById("system-map-svg"),
  systemMapInspector: document.getElementById("system-map-inspector"),
  systemMapSearch: document.getElementById("system-map-search"),
  systemMapClear: document.getElementById("system-map-clear"),
  projectStateMode: document.getElementById("project-state-mode"),
  detectProjectState: document.getElementById("detect-project-state"),
  discoverUseSample: document.getElementById("discover-use-sample"),
  projectStateResult: document.getElementById("project-state-result"),
  brownfieldIntegrations: document.getElementById("brownfield-integrations"),
  greenfieldIntegrations: document.getElementById("greenfield-integrations"),
  bfRepoProvider: document.getElementById("bf-repo-provider"),
  bfRepoUrl: document.getElementById("bf-repo-url"),
  bfIssueProvider: document.getElementById("bf-issue-provider"),
  bfIssueProject: document.getElementById("bf-issue-project"),
  bfDocsUrl: document.getElementById("bf-docs-url"),
  bfRuntimeTelemetry: document.getElementById("bf-runtime-telemetry"),
  bfLoadGithubTree: document.getElementById("bf-load-github-tree"),
  bfGithubTreeStatus: document.getElementById("bf-github-tree-status"),
  bfGithubTreePreview: document.getElementById("bf-github-tree-preview"),
  bfLoadLinearIssues: document.getElementById("bf-load-linear-issues"),
  bfLinearIssuesStatus: document.getElementById("bf-linear-issues-status"),
  bfLinearIssuesPreview: document.getElementById("bf-linear-issues-preview"),
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
  domainPackSelect: document.getElementById("domain-pack-select"),
  domainJurisdiction: document.getElementById("domain-jurisdiction"),
  domainDataClassification: document.getElementById("domain-data-classification"),
  customDomainPackPanel: document.getElementById("custom-domain-pack-panel"),
  domainPackJson: document.getElementById("domain-pack-json"),
  domainPackStatus: document.getElementById("domain-pack-status"),
  domainPackFile: document.getElementById("domain-pack-file"),
  uploadDomainPack: document.getElementById("upload-domain-pack"),
  discoverRunAnalystBrief: document.getElementById("discover-run-analyst-brief"),
  discoverAnalystBriefStatus: document.getElementById("discover-analyst-brief-status"),
  discoverAnalystBriefPreview: document.getElementById("discover-analyst-brief-preview"),

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
  cloneRequirementsPackProfile: document.getElementById("clone-requirements-pack-profile"),
  cloneRequirementsPackTemplate: document.getElementById("clone-requirements-pack-template"),
  cloneAgentBtn: document.getElementById("clone-agent-btn"),
  cloneAgentMessage: document.getElementById("clone-agent-message"),
  teamAgentCatalog: document.getElementById("team-agent-catalog"),

  tasksRefresh: document.getElementById("tasks-refresh"),
  tasksList: document.getElementById("tasks-list"),
  workItemsRefresh: document.getElementById("work-items-refresh"),
  workItemTitle: document.getElementById("work-item-title"),
  workItemType: document.getElementById("work-item-type"),
  workItemGovernance: document.getElementById("work-item-governance"),
  workItemLinkedIssue: document.getElementById("work-item-linked-issue"),
  workItemDescription: document.getElementById("work-item-description"),
  workItemCreate: document.getElementById("work-item-create"),
  workItemRecommendation: document.getElementById("work-item-recommendation"),
  workItemsList: document.getElementById("work-items-list"),
  verifyHeaderSubtitle: document.getElementById("verify-header-subtitle"),
  verifyRunSelect: document.getElementById("verify-run-select"),
  verifyRefresh: document.getElementById("verify-refresh"),
  verifyExportPdf: document.getElementById("verify-export-pdf"),
  verifyExportJson: document.getElementById("verify-export-json"),
  verifyReleaseReadiness: document.getElementById("verify-release-readiness"),
  verifyApprovalsPending: document.getElementById("verify-approvals-pending"),
  verifyLastUpdated: document.getElementById("verify-last-updated"),
  verifyBaselineDiff: document.getElementById("verify-baseline-diff"),
  verifyTabButtons: document.getElementById("verify-tab-buttons"),
  verifyTabContent: document.getElementById("verify-tab-content"),

  provider: document.getElementById("provider"),
  model: document.getElementById("model"),
  temperature: document.getElementById("temperature"),
  parallelAgents: document.getElementById("parallel-agents"),
  maxRetries: document.getElementById("max-retries"),
  taskType: document.getElementById("task-type"),
  modernizationPanel: document.getElementById("modernization-panel"),
  modernizationLanguage: document.getElementById("modernization-language"),
  modernizationSourceMode: document.getElementById("modernization-source-mode"),
  modernizationSourceHelp: document.getElementById("modernization-source-help"),
  modernizationManualInputs: document.getElementById("modernization-manual-inputs"),
  databasePanel: document.getElementById("database-panel"),
  dbSource: document.getElementById("db-source"),
  dbTarget: document.getElementById("db-target"),
  dbSchema: document.getElementById("db-schema"),
  dbFile: document.getElementById("db-file"),
  dbUploadStatus: document.getElementById("db-upload-status"),
  uploadDb: document.getElementById("upload-db"),
  humanApproval: document.getElementById("human-approval"),
  strictSecurityMode: document.getElementById("strict-security-mode"),
  liveDeploy: document.getElementById("live-deploy"),
  deploymentTarget: document.getElementById("deployment-target"),
  enableCloudPromotion: document.getElementById("enable-cloud-promotion"),
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
  runPause: document.getElementById("run-pause"),
  runResume: document.getElementById("run-resume"),
  runRerunStage: document.getElementById("run-rerun-stage"),
  runIntervene: document.getElementById("run-intervene"),
  runAbort: document.getElementById("run-abort"),
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
  impactDiffPanel: document.getElementById("impact-diff-panel"),
  impactDiffContent: document.getElementById("impact-diff-content"),
  currentAgentPanel: document.getElementById("current-agent-panel"),
  agentTabs: document.getElementById("agent-tabs"),
  agentTabPanel: document.getElementById("agent-tab-panel"),
  collaborationPanel: document.getElementById("collaboration-panel"),
  collabStageLabel: document.getElementById("collab-stage-label"),
  collabTabButtons: document.getElementById("collab-tab-buttons"),
  collabTabContent: document.getElementById("collab-tab-content"),
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
    sampleDatasetEnabled: false,
  },
  discoverGithubTree: {
    loading: false,
    error: "",
    repo: null,
    tree: null,
  },
  discoverLinearIssues: {
    loading: false,
    error: "",
    team: null,
    issues: [],
    source: "",
  },
  discoverAutoFetch: {
    githubKey: "",
    linearKey: "",
  },
  discoverAnalystBrief: {
    loading: false,
    error: "",
    data: null,
    requestKey: "",
    threadId: "",
  },
  domainPackCatalog: [],
  currentRunId: "",
  currentRun: null,
  eventSource: null,
  selectedStage: 1,
  impactDiffTab: "topology",
  cityOverlay: "none",
  systemSearch: "",
  graphView: {
    city: { x: 0, y: 0, scale: 1 },
    system: { x: 0, y: 0, scale: 1 },
  },
  graphSelected: {
    city: "",
    system: "",
  },
  artifacts: [],
  teams: [],
  agents: { premade: [], custom: [], all: [], by_stage: {} },
  tasks: [],
  workItems: [],
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
  collaboration: {
    selectedTab: "chat",
    cache: {},
    loadingKey: "",
    errorByKey: {},
    drafts: {},
  },
  verify: {
    selectedTab: "summary",
    selectedRunId: "",
    loadingRunId: "",
  },
  analyst: {
    selectedTab: "spec",
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

function setDbUploadStatus(message, isError = false) {
  if (!el.dbUploadStatus) return;
  el.dbUploadStatus.textContent = String(message || "").trim() || "Upload SQL/DDL text or Microsoft Access files (.mdb/.accdb).";
  el.dbUploadStatus.className = `mt-1 text-[11px] ${isError ? "text-rose-700" : "text-slate-700"}`;
}

async function parseAccessDatabaseFile(file) {
  const form = new FormData();
  form.append("file", file);
  form.append("target_engine", String(el.dbTarget?.value || "PostgreSQL"));
  const res = await fetch("/api/discover/access/inspect", {
    method: "POST",
    body: form,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || data.ok === false) {
    throw new Error(String(data.error || `HTTP ${res.status}`));
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

function parseCommaValues(text) {
  return String(text || "")
    .split(/[\n,]/g)
    .map((part) => part.trim())
    .filter(Boolean);
}

function slugifyValue(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function normalizeCustomDomainPack(rawPack) {
  if (!rawPack || typeof rawPack !== "object" || Array.isArray(rawPack)) return null;
  const pack = { ...rawPack };
  const id = String(pack.id || "").trim() || `custom-${slugifyValue(pack.name || "domain-pack") || "domain-pack"}-v1`;
  const name = String(pack.name || "").trim() || "Custom Domain Pack";
  const version = String(pack.version || "").trim() || "1.0.0";
  const ontology = (pack.ontology && typeof pack.ontology === "object" && !Array.isArray(pack.ontology))
    ? pack.ontology
    : {};
  const capabilities = Array.isArray(ontology.capabilities)
    ? ontology.capabilities.filter((item) => item && typeof item === "object")
    : [];
  return {
    id,
    name,
    version,
    ontology: {
      framework: String(ontology.framework || "Capability Taxonomy"),
      capabilities,
    },
    standards: Array.isArray(pack.standards) ? pack.standards.filter((item) => item && typeof item === "object") : [],
    regulations: Array.isArray(pack.regulations) ? pack.regulations.filter((item) => item && typeof item === "object") : [],
    gold_patterns: Array.isArray(pack.gold_patterns) ? pack.gold_patterns.filter((item) => item && typeof item === "object") : [],
    rules: (pack.rules && typeof pack.rules === "object" && !Array.isArray(pack.rules))
      ? pack.rules
      : {
          non_negotiables: [],
          completeness_checklist: [],
        },
    evaluation_harness: (pack.evaluation_harness && typeof pack.evaluation_harness === "object" && !Array.isArray(pack.evaluation_harness))
      ? pack.evaluation_harness
      : {
          minimum_functional_requirements: 6,
          minimum_non_functional_requirements: 4,
          minimum_bdd_scenarios: 4,
          required_quality_gates: ["gherkin_syntax", "requirements_completeness"],
        },
  };
}

function currentDomainPackConfig() {
  const selected = String(el.domainPackSelect?.value || "auto").trim() || "auto";
  const jurisdictionRaw = String(el.domainJurisdiction?.value || "AUTO").trim().toUpperCase();
  const jurisdiction = jurisdictionRaw === "AUTO" ? "" : jurisdictionRaw;
  const classes = parseCommaValues(el.domainDataClassification?.value || "")
    .map((entry) => entry.toUpperCase())
    .filter(Boolean);
  const dedupClasses = [...new Set(classes)];
  const payload = {
    selected,
    jurisdiction,
    data_classification: dedupClasses,
    domain_pack_id: "",
    custom_domain_pack: null,
    error: "",
  };
  if (selected === "auto") return payload;
  if (selected === "custom") {
    const rawJson = String(el.domainPackJson?.value || "").trim();
    if (!rawJson) {
      payload.error = "Custom Domain Pack JSON is required when custom mode is selected.";
      return payload;
    }
    try {
      const parsed = JSON.parse(rawJson);
      const normalized = normalizeCustomDomainPack(parsed);
      if (!normalized) {
        payload.error = "Custom Domain Pack JSON must be a valid object.";
        return payload;
      }
      payload.domain_pack_id = String(normalized.id || "").trim();
      payload.custom_domain_pack = normalized;
      return payload;
    } catch (err) {
      payload.error = `Invalid JSON: ${err.message || err}`;
      return payload;
    }
  }
  payload.domain_pack_id = selected;
  return payload;
}

function renderDomainPackControls() {
  const selected = String(el.domainPackSelect?.value || "auto").trim() || "auto";
  if (el.customDomainPackPanel) {
    el.customDomainPackPanel.classList.toggle("hidden", selected !== "custom");
  }
  const config = currentDomainPackConfig();
  if (!el.domainPackStatus) return;
  if (config.error) {
    el.domainPackStatus.textContent = config.error;
    el.domainPackStatus.className = "mt-2 text-[11px] text-rose-700";
    return;
  }
  if (selected === "auto") {
    el.domainPackStatus.textContent = "Using automatic domain-pack selection from objective context.";
    el.domainPackStatus.className = "mt-2 text-[11px] text-slate-700";
    return;
  }
  if (selected === "custom") {
    const packName = String(config.custom_domain_pack?.name || "Custom Domain Pack");
    const packId = String(config.custom_domain_pack?.id || "");
    el.domainPackStatus.textContent = `Custom Domain Pack ready: ${packName}${packId ? ` (${packId})` : ""}.`;
    el.domainPackStatus.className = "mt-2 text-[11px] text-emerald-700";
    return;
  }
  const catalog = Array.isArray(state.domainPackCatalog) ? state.domainPackCatalog : [];
  const found = catalog.find((item) => String(item.id || "") === selected);
  const name = found?.name || selected;
  el.domainPackStatus.textContent = `Using built-in Domain Pack: ${name}${found?.version ? ` (v${found.version})` : ""}.`;
  el.domainPackStatus.className = "mt-2 text-[11px] text-slate-700";
}

function renderDomainPackCatalog() {
  if (!el.domainPackSelect) return;
  const selected = String(el.domainPackSelect.value || "auto").trim() || "auto";
  const fallback = [
    { id: "banking-core-v1", name: "Banking Core Domain Pack", version: "1.0.0" },
    { id: "software-general-v1", name: "General Software Domain Pack", version: "1.0.0" },
  ];
  const catalog = Array.isArray(state.domainPackCatalog) && state.domainPackCatalog.length
    ? state.domainPackCatalog
    : fallback;
  const options = [
    `<option value="auto">Auto-detect from objective</option>`,
    ...catalog.map((pack) => {
      const id = String(pack.id || "").trim();
      if (!id) return "";
      const name = String(pack.name || id);
      const version = String(pack.version || "").trim();
      const label = version ? `${name} (v${version})` : name;
      return `<option value="${escapeHtml(id)}">${escapeHtml(label)}</option>`;
    }),
    `<option value="custom">Custom Domain Pack (JSON)</option>`,
  ].filter(Boolean);
  el.domainPackSelect.innerHTML = options.join("");
  if ([...el.domainPackSelect.options].some((opt) => String(opt.value) === selected)) {
    el.domainPackSelect.value = selected;
  } else {
    el.domainPackSelect.value = "auto";
  }
  renderDomainPackControls();
}

async function loadDomainPackCatalog() {
  try {
    const data = await api("/api/domain-packs", null);
    state.domainPackCatalog = Array.isArray(data?.domain_packs)
      ? data.domain_packs.filter((item) => item && typeof item === "object" && String(item.id || "").trim())
      : [];
  } catch (_err) {
    state.domainPackCatalog = [];
  }
  renderDomainPackCatalog();
}

function downloadText(filename, content, mimeType = "text/plain;charset=utf-8") {
  const blob = new Blob([String(content || "")], { type: mimeType });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

function getAnalystOutput(run) {
  const result = latestResultByStage(run, 1);
  if (result?.output && typeof result.output === "object") return result.output;
  const fromState = run?.pipeline_state?.analyst_output;
  return (fromState && typeof fromState === "object") ? fromState : {};
}

function resolveLegacyInventory(output) {
  if (!output || typeof output !== "object") return {};
  const reqPack = (output.requirements_pack && typeof output.requirements_pack === "object")
    ? output.requirements_pack
    : {};
  const direct = (output.legacy_code_inventory && typeof output.legacy_code_inventory === "object")
    ? output.legacy_code_inventory
    : {};
  const fromPack = (reqPack.legacy_code_inventory && typeof reqPack.legacy_code_inventory === "object")
    ? reqPack.legacy_code_inventory
    : {};
  const base = Object.keys(direct).length ? direct : fromPack;
  if (Object.keys(base).length) {
    const vb6 = (output.vb6_analysis && typeof output.vb6_analysis === "object")
      ? output.vb6_analysis
      : ((reqPack.vb6_analysis && typeof reqPack.vb6_analysis === "object") ? reqPack.vb6_analysis : {});
    if (!Array.isArray(base.vb6_projects) && Array.isArray(vb6.projects)) {
      return { ...base, vb6_projects: vb6.projects };
    }
    return base;
  }

  const vb6 = (output.vb6_analysis && typeof output.vb6_analysis === "object")
    ? output.vb6_analysis
    : ((reqPack.vb6_analysis && typeof reqPack.vb6_analysis === "object") ? reqPack.vb6_analysis : {});
  if (!Object.keys(vb6).length) return {};
  const formsRaw = Array.isArray(vb6.forms) ? vb6.forms : [];
  const forms = formsRaw.slice(0, 80).map((row) => {
    const raw = String(row || "").trim();
    let formType = "Form";
    let formName = raw;
    if (raw.includes(":")) {
      const [head, ...rest] = raw.split(":");
      formType = String(head || "Form").trim() || "Form";
      formName = String(rest.join(":") || raw).trim();
    }
    return {
      form_name: formName || "Form",
      form_type: formType,
      business_use: "Business workflow executed through event-driven UI controls.",
      controls: [],
      event_handlers: [],
    };
  });
  const activex = Array.isArray(vb6.activex_dependencies) ? vb6.activex_dependencies : [];
  const projects = Array.isArray(vb6.projects) ? vb6.projects : [];
  return {
    summary: `Detected ${projects.length} VB6 project(s), ${forms.length} forms/usercontrols, ${Array.isArray(vb6.controls) ? vb6.controls.length : 0} controls, ${activex.length} ActiveX/COM dependencies.`,
    vb6_projects: projects,
    forms,
    activex_controls: activex,
    dll_dependencies: activex.filter((x) => String(x || "").toUpperCase().endsWith(".DLL")),
    ocx_dependencies: activex.filter((x) => String(x || "").toUpperCase().endsWith(".OCX")),
    event_handlers: Array.isArray(vb6.event_handlers) ? vb6.event_handlers : [],
    project_members: Array.isArray(vb6.project_members) ? vb6.project_members : [],
    database_tables: [],
    procedures: [],
    input_signals: [],
    side_effect_patterns: [],
    business_rules_catalog: Array.isArray(vb6.business_rules_catalog) ? vb6.business_rules_catalog : [],
    vb6_analysis: vb6,
  };
}

function resolveLegacyForms(legacyInventory) {
  const fromInventory = Array.isArray(legacyInventory?.forms) ? legacyInventory.forms : [];
  const projects = Array.isArray(legacyInventory?.vb6_projects) ? legacyInventory.vb6_projects : [];
  const synthesized = [];
  projects.forEach((project, idx) => {
    const projectName = String(project?.project_name || `VB6-Project-${idx + 1}`).trim();
    const forms = Array.isArray(project?.forms) ? project.forms : [];
    forms.forEach((raw) => {
      const text = String(raw || "").trim();
      if (!text) return;
      let formType = "Form";
      let formName = text;
      if (text.includes(":")) {
        const [head, ...rest] = text.split(":");
        formType = String(head || "Form").trim() || "Form";
        formName = String(rest.join(":") || text).trim();
      }
      synthesized.push({
        form_name: `${projectName}::${formName}`,
        form_type: formType,
        business_use: "Business workflow executed through event-driven UI controls.",
        controls: [],
        event_handlers: [],
      });
    });
  });
  const merged = [...fromInventory, ...synthesized];
  const deduped = [];
  const seen = new Set();
  merged.forEach((row, idx) => {
    if (!row || typeof row !== "object") return;
    const formType = String(row.form_type || "Form").trim() || "Form";
    const formName = String(row.form_name || `Form-${idx + 1}`).trim();
    if (!formName) return;
    const key = `${formType}|${formName}`;
    if (seen.has(key)) return;
    seen.add(key);
    deduped.push({
      form_name: formName,
      form_type: formType,
      business_use: String(row.business_use || "Business workflow executed through event-driven UI controls."),
      controls: Array.isArray(row.controls) ? row.controls : [],
      event_handlers: Array.isArray(row.event_handlers) ? row.event_handlers : [],
    });
  });
  return deduped;
}

function resolveBusinessRulesCatalog(output, legacyInventory) {
  const reqPack = (output?.requirements_pack && typeof output.requirements_pack === "object")
    ? output.requirements_pack
    : {};
  const direct = Array.isArray(output?.business_rules_catalog) ? output.business_rules_catalog : [];
  const fromLegacy = Array.isArray(legacyInventory?.business_rules_catalog) ? legacyInventory.business_rules_catalog : [];
  const fromPack = Array.isArray(reqPack.business_rules_catalog) ? reqPack.business_rules_catalog : [];
  const vb6 = (legacyInventory?.vb6_analysis && typeof legacyInventory.vb6_analysis === "object")
    ? legacyInventory.vb6_analysis
    : {};
  const fromVb6 = Array.isArray(vb6.business_rules_catalog) ? vb6.business_rules_catalog : [];
  const rows = direct.length ? direct : (fromLegacy.length ? fromLegacy : (fromPack.length ? fromPack : fromVb6));
  const normalized = [];
  const seen = new Set();
  rows.forEach((row, idx) => {
    if (typeof row === "string") {
      const text = String(row || "").trim();
      if (!text) return;
      const key = `text|${text}`;
      if (seen.has(key)) return;
      seen.add(key);
      normalized.push({
        id: `BR-${String(idx + 1).padStart(3, "0")}`,
        rule_type: "derived_rule",
        statement: text,
        scope: "legacy-code",
        evidence: "",
      });
      return;
    }
    if (!row || typeof row !== "object") return;
    const statement = String(row.statement || "").trim();
    if (!statement) return;
    const id = String(row.id || `BR-${String(idx + 1).padStart(3, "0")}`).trim();
    const ruleType = String(row.rule_type || "derived_rule").trim() || "derived_rule";
    const scope = String(row.scope || "legacy-code").trim() || "legacy-code";
    const evidence = String(row.evidence || "").trim();
    const key = `${ruleType}|${statement}|${scope}`;
    if (seen.has(key)) return;
    seen.add(key);
    normalized.push({ id, rule_type: ruleType, statement, scope, evidence, confidence: row.confidence });
  });
  return normalized;
}

function groupBusinessRulesByType(rules) {
  const rows = Array.isArray(rules) ? rules : [];
  const order = [
    "business_objective",
    "workflow_orchestration",
    "calculation_logic",
    "threshold_rule",
    "decision_branching",
    "input_validation",
    "date_rule",
    "data_persistence",
    "derived_rule",
  ];
  const rank = new Map(order.map((key, idx) => [key, idx]));
  const pretty = (value) => String(value || "derived_rule")
    .replace(/[_\s]+/g, " ")
    .trim()
    .replace(/\b\w/g, (m) => m.toUpperCase());

  const groups = new Map();
  rows.forEach((row) => {
    if (!row || typeof row !== "object") return;
    const key = String(row.rule_type || "derived_rule").trim() || "derived_rule";
    if (!groups.has(key)) {
      groups.set(key, {
        key,
        label: pretty(key),
        count: 0,
        rules: [],
      });
    }
    const group = groups.get(key);
    group.count += 1;
    group.rules.push(row);
  });

  return [...groups.values()].sort((a, b) => {
    const ar = rank.has(a.key) ? rank.get(a.key) : 999;
    const br = rank.has(b.key) ? rank.get(b.key) : 999;
    if (ar !== br) return ar - br;
    return String(a.label).localeCompare(String(b.label));
  });
}

function resolveLegacySkillProfile(output) {
  if (!output || typeof output !== "object") return {};
  const reqPack = (output.requirements_pack && typeof output.requirements_pack === "object")
    ? output.requirements_pack
    : {};
  const direct = (output.legacy_skill_profile && typeof output.legacy_skill_profile === "object")
    ? output.legacy_skill_profile
    : {};
  const fromPack = (reqPack.legacy_skill_profile && typeof reqPack.legacy_skill_profile === "object")
    ? reqPack.legacy_skill_profile
    : {};
  return Object.keys(direct).length ? direct : fromPack;
}

function normalizeOpenQuestionEntry(entry, index = 0) {
  if (typeof entry === "string") {
    const text = String(entry || "").trim();
    return {
      id: `Q-${String(index + 1).padStart(3, "0")}`,
      question: text || "Clarification required",
      owner: "Unassigned",
      severity: "medium",
      context: "",
    };
  }
  if (!entry || typeof entry !== "object") {
    return {
      id: `Q-${String(index + 1).padStart(3, "0")}`,
      question: "Clarification required",
      owner: "Unassigned",
      severity: "medium",
      context: "",
    };
  }
  const id = String(entry.id || `Q-${String(index + 1).padStart(3, "0")}`).trim();
  const question = String(entry.question || entry.text || entry.summary || "").trim() || "Clarification required";
  const owner = String(entry.owner || entry.assignee || "Unassigned").trim() || "Unassigned";
  const severityRaw = String(entry.severity || entry.priority || "medium").trim().toLowerCase();
  const severity = ["blocker", "high", "medium", "low"].includes(severityRaw) ? severityRaw : "medium";
  const context = String(entry.context || entry.impact || "").trim();
  return { id, question, owner, severity, context };
}

function extractTablesFromSqlCatalog(sqlCatalog) {
  const rows = Array.isArray(sqlCatalog) ? sqlCatalog : [];
  const found = [];
  const seen = new Set();
  rows.forEach((sql) => {
    const text = String(sql || "");
    const matches = text.match(/\b(?:from|join|into|update)\s+([a-zA-Z_][\w.]*)/ig) || [];
    matches.forEach((m) => {
      const name = String(m || "").replace(/\b(?:from|join|into|update)\s+/i, "").trim();
      if (!name) return;
      const key = name.toLowerCase();
      if (seen.has(key)) return;
      seen.add(key);
      found.push(name);
    });
  });
  return found.slice(0, 24);
}

function deriveGoldenFlows(uiEventMap, legacyForms, bddFeatures) {
  const rows = Array.isArray(uiEventMap) ? uiEventMap : [];
  const flows = [];
  const seen = new Set();
  rows.slice(0, 12).forEach((row, idx) => {
    if (!row || typeof row !== "object") return;
    const handler = String(row.event_handler || "").trim();
    const form = String(row.form || "").trim();
    const control = String(row.control || "").trim();
    const event = String(row.event || "").trim();
    const key = `${form}|${control}|${event}|${handler}`.toLowerCase();
    if (!key || seen.has(key)) return;
    seen.add(key);
    const touched = Array.isArray(row.sql_touches)
      ? row.sql_touches.map((x) => String(x || "").trim()).filter(Boolean).slice(0, 6)
      : [];
    const linked = [];
    (Array.isArray(bddFeatures) ? bddFeatures : []).forEach((feature) => {
      const fid = String(feature?.id || "").trim();
      if (!fid) return;
      const gherkin = String(feature?.gherkin || "").toLowerCase();
      if (!gherkin) return;
      if ((form && gherkin.includes(form.toLowerCase())) || (handler && gherkin.includes(handler.toLowerCase()))) {
        linked.push(fid);
      }
    });
    flows.push({
      id: `GF-${String(idx + 1).padStart(3, "0")}`,
      name: handler || `${form || "Form"} ${event || "flow"}`.trim(),
      entrypoint: [form, handler || event].filter(Boolean).join("::") || "legacy-flow",
      tables_touched: touched,
      expected_outcome: "Behavior matches legacy flow with equivalent side effects.",
      bdd_scenario_ids: linked.slice(0, 4),
    });
  });
  if (!flows.length) {
    (Array.isArray(legacyForms) ? legacyForms : []).slice(0, 5).forEach((form, idx) => {
      const formName = String(form?.form_name || `Form-${idx + 1}`).trim();
      flows.push({
        id: `GF-${String(idx + 1).padStart(3, "0")}`,
        name: `${formName} critical flow`,
        entrypoint: `${formName}::primary_event`,
        tables_touched: [],
        expected_outcome: "Behavior matches legacy flow with equivalent side effects.",
        bdd_scenario_ids: [],
      });
    });
  }
  return flows.slice(0, 10);
}

function isGenericBddFeature(feature) {
  const gherkin = String(feature?.gherkin || "").trim().toLowerCase();
  if (!gherkin) return true;
  return (
    gherkin.includes("given requirement")
    || gherkin.includes("when requirement")
    || gherkin.includes("then requirement")
  );
}

function buildAnalystReportV2(output) {
  const prebuilt = (output?.analyst_report_v2 && typeof output.analyst_report_v2 === "object")
    ? output.analyst_report_v2
    : null;
  if (
    prebuilt
    && String(prebuilt.artifact_type || "").trim() === "analyst_report"
    && String(prebuilt.artifact_version || "").trim() === "2.0"
  ) {
    return prebuilt;
  }

  const safeOutput = (output && typeof output === "object") ? output : {};
  const reqPack = (safeOutput.requirements_pack && typeof safeOutput.requirements_pack === "object")
    ? safeOutput.requirements_pack
    : {};
  const walkthrough = (safeOutput.analysis_walkthrough && typeof safeOutput.analysis_walkthrough === "object")
    ? safeOutput.analysis_walkthrough
    : {};
  const legacyInventory = resolveLegacyInventory(safeOutput);
  const legacyForms = resolveLegacyForms(legacyInventory);
  const vb6Projects = Array.isArray(legacyInventory.vb6_projects) ? legacyInventory.vb6_projects : [];
  const legacyRules = resolveBusinessRulesCatalog(safeOutput, legacyInventory);
  const vb6Analysis = (legacyInventory.vb6_analysis && typeof legacyInventory.vb6_analysis === "object")
    ? legacyInventory.vb6_analysis
    : {};
  const readiness = (legacyInventory.modernization_readiness && typeof legacyInventory.modernization_readiness === "object")
    ? legacyInventory.modernization_readiness
    : ((vb6Analysis.modernization_readiness && typeof vb6Analysis.modernization_readiness === "object") ? vb6Analysis.modernization_readiness : {});
  const sourceTargetProfile = (legacyInventory.source_target_modernization_profile && typeof legacyInventory.source_target_modernization_profile === "object")
    ? legacyInventory.source_target_modernization_profile
    : ((vb6Analysis.source_target_modernization_profile && typeof vb6Analysis.source_target_modernization_profile === "object")
      ? vb6Analysis.source_target_modernization_profile
      : ((safeOutput.source_target_modernization_profile && typeof safeOutput.source_target_modernization_profile === "object")
        ? safeOutput.source_target_modernization_profile
        : ((reqPack.source_target_modernization_profile && typeof reqPack.source_target_modernization_profile === "object")
          ? reqPack.source_target_modernization_profile
          : {})));
  const sourceProfile = (sourceTargetProfile.source && typeof sourceTargetProfile.source === "object")
    ? sourceTargetProfile.source
    : {};
  const targetProfile = (sourceTargetProfile.target && typeof sourceTargetProfile.target === "object")
    ? sourceTargetProfile.target
    : {};
  const contextRef = (safeOutput.context_reference && typeof safeOutput.context_reference === "object")
    ? safeOutput.context_reference
    : ((reqPack.context_reference && typeof reqPack.context_reference === "object") ? reqPack.context_reference : {});
  const skill = resolveLegacySkillProfile(safeOutput);
  const fr = Array.isArray(safeOutput.functional_requirements) ? safeOutput.functional_requirements : [];
  const nfr = Array.isArray(safeOutput.non_functional_requirements) ? safeOutput.non_functional_requirements : [];
  const openQuestionsRaw = Array.isArray(safeOutput.open_questions)
    ? safeOutput.open_questions
    : (Array.isArray(reqPack.open_questions) ? reqPack.open_questions : []);
  const openQuestions = openQuestionsRaw.map((q, idx) => normalizeOpenQuestionEntry(q, idx));
  const risks = Array.isArray(safeOutput.risks) ? safeOutput.risks : [];
  const bddContract = (safeOutput.bdd_contract && typeof safeOutput.bdd_contract === "object")
    ? safeOutput.bdd_contract
    : ((reqPack.bdd_contract && typeof reqPack.bdd_contract === "object") ? reqPack.bdd_contract : {});
  const bddFeatures = Array.isArray(bddContract.features) ? bddContract.features : [];
  const qualityGatesRaw = Array.isArray(safeOutput.quality_gates)
    ? safeOutput.quality_gates
    : (Array.isArray(reqPack.quality_gates) ? reqPack.quality_gates : []);
  const acceptanceMap = Array.isArray(safeOutput.acceptance_test_mapping)
    ? safeOutput.acceptance_test_mapping
    : (Array.isArray(reqPack.acceptance_test_mapping) ? reqPack.acceptance_test_mapping : []);
  const uiEventMap = Array.isArray(legacyInventory.ui_event_map)
    ? legacyInventory.ui_event_map
    : (Array.isArray(vb6Analysis.ui_event_map) ? vb6Analysis.ui_event_map : []);
  const sqlCatalog = Array.isArray(legacyInventory.sql_query_catalog)
    ? legacyInventory.sql_query_catalog
    : (Array.isArray(vb6Analysis.sql_query_catalog) ? vb6Analysis.sql_query_catalog : []);
  const pitfallDetectors = Array.isArray(legacyInventory.pitfall_detectors)
    ? legacyInventory.pitfall_detectors
    : (Array.isArray(vb6Analysis.pitfall_detectors) ? vb6Analysis.pitfall_detectors : []);
  const activeX = Array.isArray(legacyInventory.activex_controls) ? legacyInventory.activex_controls : [];
  const eventHandlers = Array.isArray(legacyInventory.event_handlers) ? legacyInventory.event_handlers : [];
  const controlsCount = vb6Projects.reduce((acc, project) => {
    const controls = Array.isArray(project?.controls) ? project.controls.length : 0;
    return acc + controls;
  }, 0);
  const projectFormCount = vb6Projects.reduce((acc, project) => {
    const explicit = Number(project?.forms_count || 0);
    if (Number.isFinite(explicit) && explicit > 0) return acc + explicit;
    return acc + (Array.isArray(project?.forms) ? project.forms.length : 0);
  }, 0);
  const formsCount = Math.max(legacyForms.length, projectFormCount);
  const inferredTables = Array.isArray(legacyInventory.database_tables) ? legacyInventory.database_tables : [];
  const tablesTouched = [...new Set([
    ...inferredTables.map((x) => String(x || "").trim()).filter(Boolean),
    ...extractTablesFromSqlCatalog(sqlCatalog),
  ])].slice(0, 12);
  const readinessScoreRaw = Number(readiness.score);
  const readinessScore = Number.isFinite(readinessScoreRaw)
    ? Math.max(0, Math.min(100, Math.round(readinessScoreRaw)))
    : 60;
  const riskTier = String(readiness.risk_tier || "").trim().toLowerCase();
  const normalizedRiskTier = ["low", "medium", "high"].includes(riskTier) ? riskTier : (readinessScore < 45 ? "high" : (readinessScore < 75 ? "medium" : "low"));
  const topRiskDrivers = [];
  pitfallDetectors
    .slice()
    .sort((a, b) => Number(b?.count || 0) - Number(a?.count || 0))
    .slice(0, 3)
    .forEach((det, idx) => {
      topRiskDrivers.push({
        id: String(det?.id || `DET-${idx + 1}`),
        severity: String(det?.severity || "medium").toLowerCase(),
        description: `${String(det?.id || "Detector")} count=${Number(det?.count || 0)}${String(det?.evidence || "").trim() ? ` | ${String(det.evidence).trim()}` : ""}`,
        mitigation: "Add targeted migration playbooks and parity tests for this detector pattern.",
        evidence_refs: [String(det?.id || `DET-${idx + 1}`)],
      });
    });
  risks.slice(0, 3).forEach((risk, idx) => {
    topRiskDrivers.push({
      id: String(risk?.id || `RISK-${idx + 1}`),
      severity: String(risk?.impact || "medium").toLowerCase(),
      description: String(risk?.description || "Legacy modernization risk identified."),
      mitigation: String(risk?.mitigation || "Add explicit mitigation plan and gate checks."),
      evidence_refs: [],
    });
  });
  const blockingDecisions = [
    {
      id: "DEC-UI-001",
      question: "Target UI framework selection for migrated forms",
      options: ["WinForms", "WPF", "Web UI"],
      default_recommendation: "WinForms for lowest event-model delta from VB6 unless UX redesign is in-scope.",
      impact_if_wrong: "High rework risk in form/event parity and control migration.",
    },
    {
      id: "DEC-OCX-001",
      question: "ActiveX/OCX replacement strategy by dependency",
      options: ["Replace", "Wrap temporarily", "Isolate and defer"],
      default_recommendation: "Replace common controls; isolate high-risk dependencies behind adapters.",
      impact_if_wrong: "Runtime regressions and release delays from unresolved OCX behavior.",
    },
    {
      id: "DEC-DB-001",
      question: "Database contract strategy during migration",
      options: ["Preserve schema/queries", "Introduce migration layer", "Redesign schema"],
      default_recommendation: "Preserve contracts initially; migrate behind compatibility layer.",
      impact_if_wrong: "Business rule drift and data-side regressions.",
    },
  ];
  openQuestions
    .filter((q) => q.severity === "blocker" || q.severity === "high")
    .slice(0, 3)
    .forEach((q) => {
      blockingDecisions.push({
        id: String(q.id || "DEC-Q"),
        question: String(q.question || "Open clarification"),
        options: [],
        default_recommendation: "Resolve with product/business owner before implementation commit.",
        impact_if_wrong: "Execution ambiguity and acceptance test churn.",
      });
    });
  const nonBlockingDecisions = [
    {
      id: "DEC-OBS-001",
      question: "Logging/observability stack for migrated runtime",
      options: ["OpenTelemetry + structured logs", "Basic logs only"],
      default_recommendation: "OpenTelemetry + structured logs for parity troubleshooting.",
      impact_if_wrong: "Lower diagnosability during phased cutover.",
    },
  ];

  const backlogItems = [];
  fr.forEach((item, idx) => {
    const reqId = String(item?.id || `FR-${String(idx + 1).padStart(3, "0")}`);
    backlogItems.push({
      id: reqId,
      type: "functional",
      priority: String(item?.priority || "P1").toUpperCase(),
      title: String(item?.title || reqId),
      outcome: String(item?.description || "Deliver functional parity for this requirement."),
      acceptance_criteria: Array.isArray(item?.acceptance_criteria) ? item.acceptance_criteria.map((x) => String(x || "")).filter(Boolean) : [],
      depends_on: [],
      evidence_expected: ["traceability_matrix", "functional_test_report"],
    });
  });
  nfr.forEach((item, idx) => {
    const reqId = String(item?.id || `NFR-${String(idx + 1).padStart(3, "0")}`);
    backlogItems.push({
      id: reqId,
      type: "non_functional",
      priority: "P1",
      title: String(item?.title || reqId),
      outcome: String(item?.description || "Deliver non-functional controls."),
      acceptance_criteria: Array.isArray(item?.acceptance_criteria) ? item.acceptance_criteria.map((x) => String(x || "")).filter(Boolean) : [],
      depends_on: [],
      evidence_expected: ["nfr_validation_report", "quality_gate_report"],
    });
  });

  const goldenFlows = deriveGoldenFlows(uiEventMap, legacyForms, bddFeatures);
  const genericBddCount = bddFeatures.filter((feature) => isGenericBddFeature(feature)).length;
  const bddGate = {
    id: "bdd_flow_grounding",
    result: genericBddCount > 0 ? "warn" : "pass",
    description: genericBddCount > 0
      ? `${genericBddCount} BDD feature(s) appear generic; ground scenarios in real form/event entrypoints.`
      : "BDD scenarios are grounded in extracted legacy flows.",
    remediation: genericBddCount > 0 ? "Regenerate BDD from UI Event Map golden flows." : "",
  };
  const qualityGates = qualityGatesRaw.map((gate, idx) => {
    const status = String(gate?.status || "").trim().toLowerCase();
    return {
      id: String(gate?.id || gate?.name || `gate_${idx + 1}`),
      result: status === "pass" ? "pass" : (status === "fail" ? "fail" : "warn"),
      description: String(gate?.message || gate?.name || "Quality gate result"),
      remediation: status === "fail" ? "Address gate failure before progression." : "",
    };
  });
  qualityGates.push(bddGate);

  const traceabilityLinks = (Array.isArray(reqPack?.traceability?.links) ? reqPack.traceability.links : [])
    .map((row) => {
      const from = String(row?.from || "").trim();
      const to = String(row?.to || "").trim();
      const type = String(row?.type || "").trim();
      if (!from || !to || !type) return null;
      return { from, to, type };
    })
    .filter(Boolean);

  const testMatrix = acceptanceMap.map((entry) => ({
    requirement_id: String(entry?.requirement_id || "").trim(),
    test_types: Array.isArray(entry?.test_types) ? entry.test_types.map((x) => String(x || "").toLowerCase()).filter(Boolean) : [],
    scenario_ids: Array.isArray(entry?.bdd_scenarios) ? entry.bdd_scenarios.map((x) => String(x || "")).filter(Boolean) : [],
  })).filter((row) => row.requirement_id);

  const report = {
    artifact_type: "analyst_report",
    artifact_version: "2.0",
    metadata: {
      project: {
        name: String(safeOutput.project_name || "Untitled"),
        objective: String(walkthrough.business_objective_summary || safeOutput.executive_summary || "Objective not captured."),
        domain: String(reqPack?.project?.domain || "software"),
        audience_modes: ["client", "engineering"],
      },
      generated_at: new Date().toISOString(),
      skill_pack: {
        id: String(skill.selected_skill_id || "generic_legacy"),
        name: String(skill.selected_skill_name || "Generic Legacy Skill"),
        version: String(skill.version || "1.0.0"),
        confidence: Number(skill.confidence || 0),
        rationale: Array.isArray(skill.reasons) ? skill.reasons.map((x) => String(x || "")).filter(Boolean).join(" | ") : "",
      },
      context_reference: {
        repo: String(contextRef.repo || contextRef.repo_url || sourceProfile.repo || ""),
        branch: String(contextRef.branch || "main"),
        commit_sha: String(contextRef.commit_sha || ""),
        version_id: String(contextRef.version_id || ""),
        scm_version: String(contextRef.scm_version || "1.0"),
        cp_version: String(contextRef.cp_version || "1.0"),
        ha_version: String(contextRef.ha_version || "1.0"),
      },
    },
    decision_brief: {
      at_a_glance: {
        readiness_score: readinessScore,
        risk_tier: normalizedRiskTier,
        inventory_summary: {
          projects: vb6Projects.length,
          forms: formsCount,
          controls: controlsCount,
          dependencies: activeX.length,
          event_handlers: eventHandlers.length,
          tables_touched: tablesTouched,
        },
        headline: String(readiness.recommended_strategy?.name || "Phased modernization") + " recommended.",
      },
      recommended_strategy: {
        name: String(readiness.recommended_strategy?.name || "Phased modernization"),
        rationale: String(readiness.recommended_strategy?.rationale || "Preserve behavior first, then modernize in controlled phases."),
        phases: [
          {
            id: "PH0",
            title: "Baseline and equivalence harness",
            outcome: "Capture golden flows and baseline outputs.",
            exit_criteria: ["Golden flows agreed", "Baseline outputs captured", "Parity checks defined"],
          },
          {
            id: "PH1",
            title: "Incremental migration and dependency replacement",
            outcome: "Migrate forms/modules with OCX/COM risk controls.",
            exit_criteria: ["P0 flows migrated", "Critical dependencies addressed", "Regression suite passing"],
          },
          {
            id: "PH2",
            title: "Hardening and release evidence",
            outcome: "Finalize quality gates and publish evidence pack.",
            exit_criteria: ["Quality gates pass", "Traceability complete", "Release readiness approved"],
          },
        ],
      },
      decisions_required: {
        blocking: blockingDecisions.slice(0, 8),
        non_blocking: nonBlockingDecisions,
      },
      top_risks: topRiskDrivers.slice(0, 8),
      next_steps: [
        {
          id: "NS-001",
          title: "Confirm blocking decisions and freeze modernization scope",
          owner_role: "Tech Lead",
          done_when: ["Blocking decisions approved", "Backlog dependencies resolved"],
        },
        {
          id: "NS-002",
          title: "Implement golden flow harness for parity validation",
          owner_role: "QA Lead",
          done_when: ["Golden flow tests created", "Baseline artifacts stored"],
        },
      ],
    },
    delivery_spec: {
      scope: {
        in_scope: [
          "Preserve legacy business behavior and workflows",
          "Migrate UI and code to target stack",
          "Control dependency and data-side risks during migration",
        ],
        out_of_scope: Array.isArray(reqPack.out_of_scope) ? reqPack.out_of_scope.map((x) => String(x || "")).filter(Boolean) : [],
      },
      constraints: {
        musts: [
          "No critical workflow regression for P0 flows",
          "Traceability from requirements to tests and evidence artifacts",
        ],
        shoulds: [
          "Phased rollout with rollback points",
          "Preserve DB contracts unless explicitly approved to change",
        ],
      },
      backlog: {
        items: backlogItems.slice(0, 80),
      },
      testing_and_evidence: {
        golden_flows: goldenFlows,
        test_matrix: testMatrix,
        evidence_outputs: [
          { type: "traceability_matrix", path_hint: "artifacts/evidence/traceability-matrix.json", description: "Requirement-to-test traceability" },
          { type: "quality_gate_report", path_hint: "artifacts/evidence/quality-gates.json", description: "Gate outcomes and remediation" },
          { type: "golden_flow_diff", path_hint: "artifacts/evidence/golden-flow-diff.json", description: "Legacy vs modernized output parity" },
        ],
        quality_gates: qualityGates.slice(0, 20),
      },
      traceability: {
        links: traceabilityLinks,
      },
      open_questions: openQuestions,
    },
    appendix: {
      artifact_refs: {
        legacy_inventory_ref: "artifact://analyst/raw/legacy_inventory/v1",
        event_map_ref: "artifact://analyst/raw/event_map/v1",
        sql_catalog_ref: "artifact://analyst/raw/sql_catalog/v1",
        sql_map_ref: "artifact://analyst/raw/sql_map/v1",
        procedure_summary_ref: "artifact://analyst/raw/procedure_summary/v1",
        dependency_list_ref: "artifact://analyst/raw/dependency_inventory/v1",
        dependency_inventory_ref: "artifact://analyst/raw/dependency_inventory/v1",
        business_rules_ref: "artifact://analyst/raw/business_rule_catalog/v1",
        detector_findings_ref: "artifact://analyst/raw/detector_findings/v1",
        delivery_constitution_ref: "artifact://analyst/raw/delivery_constitution/v1",
        artifact_index_ref: "artifact://analyst/raw/artifact_index/v1",
      },
      high_volume_sections: {
        legacy_inventory: legacyInventory,
        event_map: uiEventMap,
        sql_catalog: sqlCatalog,
        dependencies: activeX,
        business_rules: legacyRules,
      },
    },
    spec_kit_decomposition: {
      artifact_type: "spec_kit_projection",
      artifact_version: "1.0",
      discovery_spec: {
        title: "Legacy discovery spec",
        objective: String(analysisWalk.business_objective_summary || output.executive_summary || "Objective not captured."),
        inventory_counts: {
          projects: vb6Projects.length,
          forms: formsCount,
          dependencies: activeX.length,
          procedures: uiEventMap.length,
          sql_map_entries: sqlCatalog.length,
        },
        key_user_stories: [
          `As a modernization engineer, I need a deterministic map of ${formsCount} UI flows to avoid behavioral drift.`,
          "As a delivery lead, I need explicit clarification markers to avoid speculative implementation.",
          "As QA, I need event-to-query evidence to build equivalence tests.",
        ],
        needs_clarification: openQuestions.map((q, idx) => normalizeOpenQuestionEntry(q, idx)),
      },
      modernization_plan: {
        title: "Modernization implementation plan",
        strategy: String(readiness.recommended_strategy?.name || "Phased modernization"),
        rationale: String(readiness.recommended_strategy?.rationale || "Preserve behavior first, then modernize in controlled phases."),
        phases: [
          { id: "PH0", title: "Baseline and equivalence harness" },
          { id: "PH1", title: "Incremental migration and dependency replacement" },
          { id: "PH2", title: "Hardening and release evidence" },
        ],
        backlog_items: backlogItems.length,
        blocking_decisions: blockingDecisions.length,
      },
      executable_contracts: {
        title: "Executable contracts",
        golden_flow_count: goldenFlows.length,
        test_matrix_rows: testMatrix.length,
        quality_gate_count: qualityGates.length,
        grounding_status: qualityGates.some((g) => String(g.id || "") === "bdd_flow_grounding" && String(g.result || "") === "warn")
          ? "needs_improvement"
          : "grounded",
        traceability_links: traceabilityLinks.length,
      },
      constitution: {
        principles: [
          "Preserve critical legacy behavior first; modernization must prove functional equivalence.",
          "Every modernization decision must map to explicit evidence (code, query, event, or rule).",
          "No breaking change to data contracts without approved migration path and rollback evidence.",
        ],
      },
    },
  };
  return report;
}

function buildAnalystTechReqMarkdown(output, options = {}) {
  if (!output || typeof output !== "object") return "# Modernization Brief\n\nNo analyst output available.";
  const revised = String(output.human_revised_document_markdown || "").trim();
  if (revised) return revised;
  const mode = String(options.mode || "full").trim().toLowerCase();
  const includeDetailedAppendix = mode !== "summary";

  const report = buildAnalystReportV2(output);
  const metadata = report.metadata || {};
  const project = metadata.project || {};
  const brief = report.decision_brief || {};
  const glance = brief.at_a_glance || {};
  const inventory = glance.inventory_summary || {};
  const strategy = brief.recommended_strategy || {};
  const decisions = brief.decisions_required || {};
  const backlog = report.delivery_spec?.backlog?.items || [];
  const testing = report.delivery_spec?.testing_and_evidence || {};
  const appendix = report.appendix || {};
  const openQuestions = Array.isArray(report.delivery_spec?.open_questions) ? report.delivery_spec.open_questions : [];

  const lines = [
    `# Modernization Brief - ${String(project.name || "Untitled Project")}`,
    "",
    "## Header",
    `- Objective: ${String(project.objective || "Not provided")}`,
    `- Domain: ${String(project.domain || "software")}`,
    `- Repo: ${String(metadata.context_reference?.repo || "n/a")} @ ${String(metadata.context_reference?.branch || "main")} (${String(metadata.context_reference?.commit_sha || "n/a")})`,
    `- SIL Versions: SCM ${String(metadata.context_reference?.scm_version || "1.0")} / CP ${String(metadata.context_reference?.cp_version || "1.0")} / HA ${String(metadata.context_reference?.ha_version || "1.0")}`,
    `- Generated At: ${String(metadata.generated_at || "")}`,
    "",
    "## Decision Brief",
    "",
    "| Category | Summary |",
    "|---|---|",
    `| Modernization readiness | ${String(glance.readiness_score ?? "n/a")}/100 |`,
    `| Risk tier | ${String(glance.risk_tier || "n/a")} |`,
    `| Inventory | ${String(inventory.projects ?? 0)} project(s), ${String(inventory.forms ?? 0)} forms/usercontrols, ${String(inventory.dependencies ?? 0)} dependencies |`,
    `| Data touchpoints | ${Array.isArray(inventory.tables_touched) ? inventory.tables_touched.join(", ") : ""} |`,
    `| Headline | ${String(glance.headline || "")} |`,
    "",
    "### Recommended strategy",
    `- ${String(strategy.name || "Phased modernization")}: ${String(strategy.rationale || "")}`,
  ];

  const phases = Array.isArray(strategy.phases) ? strategy.phases : [];
  phases.forEach((phase) => lines.push(`- ${String(phase.id || "")} ${String(phase.title || "")}: ${String(phase.outcome || "")}`));

  lines.push("", "### Decisions Required (Blocking)");
  (Array.isArray(decisions.blocking) ? decisions.blocking : []).forEach((row) => {
    lines.push(`- ${String(row.id || "DEC")}: ${String(row.question || "")}`);
    lines.push(`  - Recommendation: ${String(row.default_recommendation || "")}`);
  });
  if (!Array.isArray(decisions.blocking) || !decisions.blocking.length) lines.push("- None");

  lines.push("", "### Decisions Required (Non-blocking)");
  (Array.isArray(decisions.non_blocking) ? decisions.non_blocking : []).forEach((row) => {
    lines.push(`- ${String(row.id || "DEC")}: ${String(row.question || "")}`);
  });
  if (!Array.isArray(decisions.non_blocking) || !decisions.non_blocking.length) lines.push("- None");

  lines.push("", "## Delivery Spec", "", "### Backlog");
  lines.push("| ID | Pri | Type | Outcome | Acceptance |");
  lines.push("|---|---|---|---|---|");
  backlog.slice(0, 80).forEach((item) => {
    const ac = Array.isArray(item.acceptance_criteria) ? item.acceptance_criteria.slice(0, 2).join(" / ") : "";
    lines.push(`| ${String(item.id || "")} | ${String(item.priority || "")} | ${String(item.type || "")} | ${String(item.title || item.outcome || "")} | ${String(ac || "n/a")} |`);
  });
  if (!backlog.length) lines.push("| - | - | - | No backlog items generated | - |");

  lines.push("", "### Testing and Evidence");
  lines.push("- Golden flows:");
  (Array.isArray(testing.golden_flows) ? testing.golden_flows : []).forEach((flow) => {
    lines.push(`  - ${String(flow.id || "GF")}: ${String(flow.name || "")} | entry=${String(flow.entrypoint || "")}`);
  });
  if (!Array.isArray(testing.golden_flows) || !testing.golden_flows.length) lines.push("  - None");
  lines.push("- Quality gates:");
  (Array.isArray(testing.quality_gates) ? testing.quality_gates : []).forEach((gate) => {
    lines.push(`  - ${String(gate.id || "gate")}: ${String(gate.result || "warn").toUpperCase()} | ${String(gate.description || "")}`);
  });
  if (!Array.isArray(testing.quality_gates) || !testing.quality_gates.length) lines.push("  - None");

  lines.push("", "### Open Questions");
  openQuestions.forEach((q, idx) => {
    const row = normalizeOpenQuestionEntry(q, idx);
    lines.push(`- [${String(row.severity || "medium").toUpperCase()}] ${row.id}: ${row.question} (owner: ${row.owner})`);
    if (row.context) lines.push(`  - Context: ${row.context}`);
  });
  if (!openQuestions.length) lines.push("- None");

  lines.push("", "## Evidence Appendix");
  const refs = appendix.artifact_refs || {};
  Object.entries(refs).forEach(([k, v]) => {
    if (String(v || "").trim()) lines.push(`- ${k}: ${String(v)}`);
  });
  lines.push("- High-volume sections included in structured artifact (inventory, dependencies, event map, SQL catalog, business rules).");
  lines.push("", "## Appendix Snapshot");
  const hv = appendix.high_volume_sections || {};
  const raw = (output.raw_artifacts && typeof output.raw_artifacts === "object") ? output.raw_artifacts : {};
  const rawEventMap = Array.isArray(raw.event_map?.entries) ? raw.event_map.entries : [];
  const rawSql = Array.isArray(raw.sql_catalog?.statements) ? raw.sql_catalog.statements : [];
  const rawSqlMap = Array.isArray(raw.sql_map?.entries) ? raw.sql_map.entries : [];
  const rawProcedures = Array.isArray(raw.procedure_summary?.procedures) ? raw.procedure_summary.procedures : [];
  const rawDeps = Array.isArray(raw.dependency_inventory?.dependencies) ? raw.dependency_inventory.dependencies : [];
  const rawRules = Array.isArray(raw.business_rule_catalog?.rules) ? raw.business_rule_catalog.rules : [];
  const rawConstitution = Array.isArray(raw.delivery_constitution?.principles) ? raw.delivery_constitution.principles : [];
  lines.push(`- Legacy inventory: ${raw.legacy_inventory ? "present" : (hv.legacy_inventory ? "present" : "missing")}`);
  lines.push(`- Event map rows: ${rawEventMap.length || (Array.isArray(hv.event_map) ? hv.event_map.length : 0)}`);
  lines.push(`- SQL catalog rows: ${rawSql.length || (Array.isArray(hv.sql_catalog) ? hv.sql_catalog.length : 0)}`);
  lines.push(`- SQL map rows: ${rawSqlMap.length}`);
  lines.push(`- Procedure summaries: ${rawProcedures.length}`);
  lines.push(`- Dependency rows: ${rawDeps.length || (Array.isArray(hv.dependencies) ? hv.dependencies.length : 0)}`);
  lines.push(`- Business rules: ${rawRules.length || (Array.isArray(hv.business_rules) ? hv.business_rules.length : 0)}`);
  lines.push(`- Constitution principles: ${rawConstitution.length}`);

  const rawLegacy = (raw.legacy_inventory && typeof raw.legacy_inventory === "object")
    ? raw.legacy_inventory
    : (hv.legacy_inventory && typeof hv.legacy_inventory === "object" ? hv.legacy_inventory : {});
  const projects = Array.isArray(rawLegacy.projects) ? rawLegacy.projects : [];
  const eventRows = rawEventMap.length ? rawEventMap : (Array.isArray(hv.event_map) ? hv.event_map : []);
  const sqlRows = rawSql.length ? rawSql : (Array.isArray(hv.sql_catalog) ? hv.sql_catalog : []);
  const depRows = rawDeps.length ? rawDeps : (Array.isArray(hv.dependencies) ? hv.dependencies : []);
  const ruleRows = rawRules.length ? rawRules : (Array.isArray(hv.business_rules) ? hv.business_rules : []);
  const sqlMapRows = rawSqlMap;
  const procedureRows = rawProcedures;
  const constitutionRows = rawConstitution;
  const detectorRows = Array.isArray(raw.detector_findings?.findings) ? raw.detector_findings.findings : [];
  const artifactIndexRows = Array.isArray(raw.artifact_index?.artifacts) ? raw.artifact_index.artifacts : [];

  if (includeDetailedAppendix) {
    lines.push("", "## Detailed Appendix", "");
    lines.push("### A. Legacy Inventory");
    lines.push(`- Projects: ${projects.length}`);
    lines.push(`- Data touchpoints: ${(Array.isArray(rawLegacy.summary?.data_touchpoints) ? rawLegacy.summary.data_touchpoints.join(", ") : "") || "None detected"}`);
    if (projects.length) {
      lines.push("| Project | Type | Startup | Members | Forms | Dependencies |");
      lines.push("|---|---|---|---:|---:|---:|");
      projects.slice(0, 250).forEach((project) => {
        const members = Array.isArray(project?.members) ? project.members.length : 0;
        const ui = Array.isArray(project?.ui_assets) ? project.ui_assets.length : 0;
        const deps = Array.isArray(project?.dependencies) ? project.dependencies.length : 0;
        lines.push(`| ${String(project?.name || project?.project_id || "")} | ${String(project?.type || "")} | ${String(project?.startup || "")} | ${members} | ${ui} | ${deps} |`);
      });
    } else {
      lines.push("- No project rows available.");
    }

    lines.push("", "### B. Dependency Inventory");
    if (depRows.length) {
      lines.push("| Name | Kind | Risk | Recommended action |");
      lines.push("|---|---|---|---|");
      depRows.slice(0, 500).forEach((dep) => {
        const risk = String(dep?.risk?.tier || dep?.tier || "unknown");
        const action = String(dep?.risk?.recommended_action || dep?.recommended_action || "");
        lines.push(`| ${String(dep?.name || dep || "")} | ${String(dep?.kind || "")} | ${risk} | ${action || "n/a"} |`);
      });
    } else {
      lines.push("- No dependency rows available.");
    }

    lines.push("", "### C. Event Map");
    if (eventRows.length) {
      lines.push("| Entry | Container | Trigger | Calls | Side effects |");
      lines.push("|---|---|---|---|---|");
      eventRows.slice(0, 600).forEach((entry) => {
        const name = String(entry?.name || entry?.entry_id || entry?.event_handler || "");
        const container = String(entry?.container || entry?.form || "");
        const trigger = String(entry?.trigger?.event || entry?.event || "");
        const calls = (Array.isArray(entry?.calls) ? entry.calls : Array.isArray(entry?.procedure_calls) ? entry.procedure_calls : []).slice(0, 4).join(", ");
        const effects = (Array.isArray(entry?.side_effects?.tables_or_files) ? entry.side_effects.tables_or_files : Array.isArray(entry?.sql_touches) ? entry.sql_touches : []).slice(0, 4).join(", ");
        lines.push(`| ${name} | ${container} | ${trigger} | ${calls || "n/a"} | ${effects || "n/a"} |`);
      });
    } else {
      lines.push("- No event map rows available.");
    }

    lines.push("", "### D. SQL Catalog");
    if (sqlRows.length) {
      lines.push("| SQL ID | Kind | Tables | Query |");
      lines.push("|---|---|---|---|");
      sqlRows.slice(0, 700).forEach((row) => {
        const sqlId = String(row?.sql_id || "");
        const kind = String(row?.kind || "");
        const tables = (Array.isArray(row?.tables) ? row.tables : []).slice(0, 6).join(", ");
        const query = String(row?.raw || row || "").replace(/\|/g, "\\|");
        lines.push(`| ${sqlId || "n/a"} | ${kind || "unknown"} | ${tables || "n/a"} | ${query} |`);
      });
    } else {
      lines.push("- No SQL rows available.");
    }

    lines.push("", "### E. Business Rules");
    if (ruleRows.length) {
      lines.push("| Rule ID | Category | Statement | Evidence |");
      lines.push("|---|---|---|---|");
      ruleRows.slice(0, 700).forEach((row) => {
        const ruleId = String(row?.rule_id || row?.id || "");
        const category = String(row?.category || row?.rule_type || "other");
        const statement = String(row?.statement || "").replace(/\|/g, "\\|");
        const ev = Array.isArray(row?.evidence)
          ? row.evidence.map((e) => String(e?.external_ref?.ref || e?.file_span?.path || e?.ref || "")).filter(Boolean).slice(0, 3).join(", ")
          : String(row?.evidence || "");
        lines.push(`| ${ruleId || "n/a"} | ${category} | ${statement || "n/a"} | ${ev || "n/a"} |`);
      });
    } else {
      lines.push("- No business rules available.");
    }

    lines.push("", "### F. Detector Findings");
    if (detectorRows.length) {
      lines.push("| Detector | Severity | Count | Summary | Required actions |");
      lines.push("|---|---|---:|---|---|");
      detectorRows.slice(0, 500).forEach((row) => {
        const actions = Array.isArray(row?.required_actions) ? row.required_actions.slice(0, 4).join(", ") : "";
        lines.push(`| ${String(row?.detector_id || "n/a")} | ${String(row?.severity || "medium")} | ${Number(row?.count || 0)} | ${String(row?.summary || "").replace(/\|/g, "\\|")} | ${actions || "n/a"} |`);
      });
    } else {
      lines.push("- No detector findings available.");
    }

    lines.push("", "### G. Artifact Index");
    if (artifactIndexRows.length) {
      lines.push("| Type | Ref |");
      lines.push("|---|---|");
      artifactIndexRows.slice(0, 200).forEach((row) => {
        lines.push(`| ${String(row?.type || "")} | ${String(row?.ref || "")} |`);
      });
    } else {
      lines.push("- No artifact index entries.");
    }

    lines.push("", "### H. SQL Map");
    if (sqlMapRows.length) {
      lines.push("| Form | Procedure | Operation | Tables | Risks |");
      lines.push("|---|---|---|---|---|");
      sqlMapRows.slice(0, 700).forEach((row) => {
        const tables = Array.isArray(row?.tables) ? row.tables.slice(0, 6).join(", ") : "";
        const risks = Array.isArray(row?.risk_flags) ? row.risk_flags.slice(0, 6).join(", ") : "";
        lines.push(`| ${String(row?.form || "n/a")} | ${String(row?.procedure || "n/a")} | ${String(row?.operation || "unknown")} | ${tables || "n/a"} | ${risks || "none"} |`);
      });
    } else {
      lines.push("- No SQL map rows available.");
    }

    lines.push("", "### I. Procedure Summaries");
    if (procedureRows.length) {
      lines.push("| Procedure | Form | SQL IDs | Steps | Risks |");
      lines.push("|---|---|---|---|---|");
      procedureRows.slice(0, 700).forEach((row) => {
        const sqlIds = Array.isArray(row?.sql_ids) ? row.sql_ids.slice(0, 6).join(", ") : "";
        const steps = Array.isArray(row?.steps) ? row.steps.slice(0, 2).join(" / ").replace(/\|/g, "\\|") : "";
        const risks = Array.isArray(row?.risks) ? row.risks.slice(0, 5).join(", ") : "";
        lines.push(`| ${String(row?.procedure_name || row?.procedure_id || "n/a")} | ${String(row?.form || "n/a")} | ${sqlIds || "n/a"} | ${steps || "n/a"} | ${risks || "none"} |`);
      });
    } else {
      lines.push("- No procedure summaries available.");
    }

    lines.push("", "### J. Delivery Constitution");
    if (constitutionRows.length) {
      constitutionRows.slice(0, 100).forEach((line) => lines.push(`- ${String(line || "")}`));
    } else {
      lines.push("- No delivery constitution principles available.");
    }
  }
  return lines.join("\n");
}

async function uploadAnalystTechReq(runId, file) {
  const text = await file.text();
  const filename = String(file.name || "").toLowerCase();
  const format = filename.endsWith(".json") ? "json" : "markdown";
  const data = await api(`/api/runs/${encodeURIComponent(runId)}/analyst-doc`, {
    format,
    content: text,
  });
  return data?.run || null;
}

function wireAnalystDocActions(rootNode, run) {
  if (!rootNode || !run?.run_id) return;
  const exportSummaryBtn = rootNode.querySelector("[data-analyst-export-summary]");
  const exportFullBtn = rootNode.querySelector("[data-analyst-export-full]");
  const uploadTrigger = rootNode.querySelector("[data-analyst-upload-trigger]");
  const uploadInput = rootNode.querySelector("[data-analyst-upload-file]");
  const statusNode = rootNode.querySelector("[data-analyst-doc-status]");
  const setStatus = (text, isError = false) => {
    if (!statusNode) return;
    statusNode.textContent = String(text || "");
    statusNode.className = `mt-1 text-[11px] ${isError ? "text-rose-700" : "text-slate-700"}`;
  };

  if (exportSummaryBtn) {
    exportSummaryBtn.addEventListener("click", () => {
      const output = getAnalystOutput(run);
      const markdown = buildAnalystTechReqMarkdown(output, { mode: "summary" });
      const stamp = new Date().toISOString().replace(/[:.]/g, "-");
      downloadText(`analyst-tech-req-summary-${run.run_id}-${stamp}.md`, markdown, "text/markdown;charset=utf-8");
      setStatus("Summary technical requirements document exported.");
    });
  }

  if (exportFullBtn) {
    exportFullBtn.addEventListener("click", () => {
      const output = getAnalystOutput(run);
      const markdown = buildAnalystTechReqMarkdown(output, { mode: "full" });
      const stamp = new Date().toISOString().replace(/[:.]/g, "-");
      downloadText(`analyst-tech-req-full-${run.run_id}-${stamp}.md`, markdown, "text/markdown;charset=utf-8");
      setStatus("Full evidence technical requirements document exported.");
    });
  }

  if (uploadTrigger && uploadInput) {
    uploadTrigger.addEventListener("click", () => uploadInput.click());
    uploadInput.addEventListener("change", async () => {
      const file = uploadInput.files?.[0];
      if (!file) return;
      try {
        uploadTrigger.setAttribute("disabled", "true");
        setStatus(`Uploading ${file.name}...`);
        const updatedRun = await uploadAnalystTechReq(String(run.run_id), file);
        if (updatedRun?.run_id) {
          state.currentRun = updatedRun;
          state.currentRunId = String(updatedRun.run_id || state.currentRunId || "");
          state.dashboardRunDetails[state.currentRunId] = updatedRun;
          renderRun();
          await refreshRunHistory().catch(() => {});
        }
        setStatus("Modified technical requirements uploaded to Analyst stage.");
      } catch (err) {
        setStatus(`Upload failed: ${err.message || err}`, true);
      } finally {
        uploadTrigger.removeAttribute("disabled");
        uploadInput.value = "";
      }
    });
  }
}

function resolveAnalystOutputForRun(run) {
  const result = latestResultByStage(run, 1);
  const output = (result?.output && typeof result.output === "object") ? result.output : {};
  return output;
}

function setAnalystTabView(rootNode, tab) {
  if (!rootNode) return;
  const selected = String(tab || "brief");
  rootNode.querySelectorAll("[data-analyst-view-tab]").forEach((btn) => {
    const id = String(btn.getAttribute("data-analyst-view-tab") || "");
    const active = id === selected;
    btn.classList.toggle("btn-dark", active);
    btn.classList.toggle("btn-light", !active);
  });
  rootNode.querySelectorAll("[data-analyst-view-panel]").forEach((panel) => {
    const id = String(panel.getAttribute("data-analyst-view-panel") || "");
    panel.classList.toggle("hidden", id !== selected);
  });
}

function wireAnalystViewTabs(rootNode, run) {
  if (!rootNode) return;
  const tabRoot = rootNode.querySelector("[data-analyst-tab-root]");
  if (!tabRoot) return;
  const available = Array.from(tabRoot.querySelectorAll("[data-analyst-view-tab]"))
    .map((btn) => String(btn.getAttribute("data-analyst-view-tab") || "").trim())
    .filter(Boolean);
  if (!available.length) return;

  const stateTab = String(state.analyst?.selectedTab || "").trim();
  const initial = available.includes(stateTab) ? stateTab : (String(tabRoot.getAttribute("data-analyst-default") || "").trim() || available[0]);
  state.analyst.selectedTab = initial;
  setAnalystTabView(tabRoot, initial);

  tabRoot.querySelectorAll("[data-analyst-view-tab]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const tab = String(btn.getAttribute("data-analyst-view-tab") || "").trim();
      if (!available.includes(tab)) return;
      state.analyst.selectedTab = tab;
      setAnalystTabView(tabRoot, tab);
    });
  });

  const output = resolveAnalystOutputForRun(run);
  const raw = (output.raw_artifacts && typeof output.raw_artifacts === "object") ? output.raw_artifacts : {};
  const statusNode = tabRoot.querySelector("[data-analyst-view-status]");
  const setStatus = (message, isError = false) => {
    if (!statusNode) return;
    statusNode.textContent = String(message || "").trim();
    statusNode.className = `mt-1 text-[11px] ${isError ? "text-rose-700" : "text-slate-700"}`;
  };

  tabRoot.querySelectorAll("[data-artifact-copy-ref]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const ref = String(btn.getAttribute("data-artifact-copy-ref") || "").trim();
      if (!ref) return;
      try {
        if (navigator.clipboard?.writeText) {
          await navigator.clipboard.writeText(ref);
          setStatus(`Copied artifact ref: ${ref}`);
        } else {
          setStatus("Clipboard API is unavailable in this browser context.", true);
        }
      } catch (err) {
        setStatus(`Copy failed: ${err?.message || err}`, true);
      }
    });
  });

  tabRoot.querySelectorAll("[data-artifact-download-key]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const key = String(btn.getAttribute("data-artifact-download-key") || "").trim();
      const artifact = raw[key];
      if (!key || !artifact || typeof artifact !== "object") {
        setStatus(`No artifact payload found for ${key || "selection"}.`, true);
        return;
      }
      const stamp = new Date().toISOString().replace(/[:.]/g, "-");
      downloadText(`analyst-${key}-${stamp}.json`, JSON.stringify(artifact, null, 2), "application/json;charset=utf-8");
      setStatus(`Downloaded ${key}.json`);
    });
  });
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
  let view = String(state.discoverResultsView || "");
  if (!view) {
    view = "city";
    state.discoverResultsView = view;
  }
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
  state.discoverResultsView = String(view || "city");
  renderDiscoverResultsView();
}

function _scmViewFromState() {
  const p = state.currentRun?.pipeline_state || {};
  const scm = (p.system_context_model && typeof p.system_context_model === "object") ? p.system_context_model : {};
  const graph = (scm.graph && typeof scm.graph === "object") ? scm.graph : {};
  const nodesRaw = Array.isArray(graph.nodes) ? graph.nodes : (Array.isArray(scm.nodes) ? scm.nodes : []);
  const edgesRaw = Array.isArray(graph.edges) ? graph.edges : (Array.isArray(scm.edges) ? scm.edges : []);
  const nodes = nodesRaw.filter((x) => x && typeof x === "object");
  const edges = edgesRaw.filter((x) => x && typeof x === "object");
  return { nodes, edges };
}

function _discoverPreviewData() {
  const tree = (state.discoverGithubTree?.tree && typeof state.discoverGithubTree.tree === "object")
    ? state.discoverGithubTree.tree
    : {};
  const entries = Array.isArray(tree.entries) ? tree.entries : [];
  const analyst = (state.discoverAnalystBrief?.data && typeof state.discoverAnalystBrief.data === "object")
    ? state.discoverAnalystBrief.data
    : {};
  const analystSummary = (analyst.analyst_brief && typeof analyst.analyst_brief === "object" && analyst.analyst_brief.summary && typeof analyst.analyst_brief.summary === "object")
    ? analyst.analyst_brief.summary
    : {};
  const issues = Array.isArray(state.discoverLinearIssues?.issues) ? state.discoverLinearIssues.issues : [];

  const nodeMap = {};
  const edgeMap = {};
  const rules = [];
  const findings = [];
  const backlog = [];

  const addNode = (id, name, type, confidence = 0.72) => {
    const key = String(id || "").trim();
    if (!key) return null;
    if (!nodeMap[key]) {
      nodeMap[key] = { id: key, name: String(name || key), type: String(type || "module"), confidence: { score: confidence } };
    }
    return nodeMap[key];
  };

  const addEdge = (from, to, type, confidence = 0.65) => {
    const a = String(from || "").trim();
    const b = String(to || "").trim();
    if (!a || !b || a === b) return;
    const key = `${a}|${b}|${type}`;
    if (!edgeMap[key]) {
      edgeMap[key] = { id: `edge:${key}`, from: a, to: b, type: String(type || "depends_on"), confidence: { score: confidence } };
    }
  };

  addNode("repo:root", "repository", "system", 0.95);

  const moduleScores = {};
  entries.forEach((entry) => {
    if (!entry || typeof entry !== "object") return;
    const path = String(entry.path || "").trim();
    if (!path) return;
    const seg = path.split("/").filter(Boolean);
    if (!seg.length) return;

    const top = seg[0];
    addNode(`pkg:${top}`, top, "boundary", 0.78);
    addEdge("repo:root", `pkg:${top}`, "owns");
    moduleScores[`pkg:${top}`] = (moduleScores[`pkg:${top}`] || 0) + 1;

    if (seg.length > 1) {
      const branch = `${top}/${seg[1]}`;
      const nodeId = `mod:${branch}`;
      const lower = branch.toLowerCase();
      const type = lower.includes("service")
        ? "service"
        : (lower.includes("db") || lower.includes("database") ? "database" : "module");
      addNode(nodeId, branch, type, 0.75);
      addEdge(`pkg:${top}`, nodeId, "depends_on");
      moduleScores[nodeId] = (moduleScores[nodeId] || 0) + 1;
    }
  });

  const components = Array.isArray(analystSummary.key_components) ? analystSummary.key_components : [];
  components.slice(0, 20).forEach((component) => {
    const label = String(component || "").trim();
    if (!label) return;
    const id = `cmp:${label}`;
    const lower = label.toLowerCase();
    const type = lower.includes("service")
      ? "service"
      : (lower.includes("db") || lower.includes("database") ? "database" : "component");
    addNode(id, label, type, 0.7);
    addEdge("repo:root", id, "depends_on", 0.62);
  });

  const interfaces = Array.isArray(analystSummary.interfaces) ? analystSummary.interfaces : [];
  interfaces.slice(0, 24).forEach((iface, idx) => {
    const raw = String(iface || "").trim();
    if (!raw) return;
    const ifaceId = `ep:${idx}:${raw.toLowerCase().replace(/[^a-z0-9/_-]+/g, "-").slice(0, 36)}`;
    addNode(ifaceId, raw, "endpoint", 0.68);
    const lower = raw.toLowerCase();
    const ownerNode = Object.values(nodeMap).find((n) => {
      if (!n || typeof n !== "object") return false;
      if (!["service", "component", "module"].includes(String(n.type || "").toLowerCase())) return false;
      const name = String(n.name || "").toLowerCase();
      return name && (lower.includes(name.split("/").pop() || "") || lower.includes(name.replace("-service", "")));
    });
    addEdge(ownerNode ? ownerNode.id : "repo:root", ifaceId, "owns", 0.61);
  });

  const ioContracts = Array.isArray(analystSummary.input_output_contracts) ? analystSummary.input_output_contracts : [];
  ioContracts.slice(0, 12).forEach((statement, idx) => {
    const s = String(statement || "").trim();
    if (!s) return;
    const lower = s.toLowerCase();
    let category = "api_design";
    if (lower.includes("log") || lower.includes("trace")) category = "logging_observability";
    else if (lower.includes("test")) category = "testing";
    else if (lower.includes("error")) category = "error_handling";
    else if (lower.includes("auth")) category = "authn_authz";
    rules.push({ id: `disc-rule-${idx + 1}`, category, statement: s });
  });

  const unknowns = Array.isArray(analystSummary.unknowns) ? analystSummary.unknowns : [];
  unknowns.slice(0, 8).forEach((item, idx) => {
    const text = String(item || "").trim();
    if (!text) return;
    findings.push({
      id: `disc-unk-${idx + 1}`,
      severity: "medium",
      title: "Analyst unknown",
      description: text,
    });
  });

  issues.slice(0, 24).forEach((issue, idx) => {
    const priority = Number(issue?.priority || 0);
    const severity = priority <= 1 ? "high" : (priority <= 2 ? "medium" : "low");
    findings.push({
      id: String(issue?.identifier || issue?.id || `issue-${idx + 1}`),
      severity,
      title: String(issue?.title || "Issue"),
      description: `State: ${String(issue?.state || "Unknown")} | Assignee: ${String(issue?.assignee || "Unassigned")}`,
    });
    backlog.push({
      id: `backlog-${idx + 1}`,
      priority: priority <= 1 ? "P1" : (priority <= 2 ? "P2" : "P3"),
      title: `${String(issue?.identifier || issue?.id || `Issue ${idx + 1}`)} - ${String(issue?.title || "")}`,
    });
  });

  const hasLegacy = entries.some((entry) => {
    const p = String(entry?.path || "").toLowerCase();
    return p.includes("legacy/") || p.includes("monolith");
  });
  const hasService = entries.some((entry) => String(entry?.path || "").toLowerCase().includes("service"));
  const hasTests = entries.some((entry) => {
    const p = String(entry?.path || "").toLowerCase();
    return p.includes("/test/") || p.includes("/tests/") || p.startsWith("test/") || p.startsWith("tests/");
  });

  const hotspotModules = Object.entries(moduleScores)
    .filter(([, score]) => Number(score || 0) >= 4)
    .sort((a, b) => Number(b[1]) - Number(a[1]))
    .slice(0, 4);
  hotspotModules.forEach(([moduleId, score], idx) => {
    const moduleNode = nodeMap[moduleId];
    if (!moduleNode) return;
    findings.push({
      id: `disc-hotspot-${idx + 1}`,
      severity: String(moduleNode.name || "").toLowerCase().includes("legacy") ? "high" : "medium",
      title: `Hotspot candidate: ${moduleNode.name}`,
      description: `Detected ${score} source paths under this module in current scope.`,
    });
  });

  if (hasLegacy && hasService) {
    findings.push({
      id: "disc-arch-legacy-service",
      severity: "high",
      title: "Legacy and modern service boundaries overlap",
      description: "Repository contains both legacy/monolith and service modules; review coupling and migration sequencing.",
    });
  }
  if (!hasTests && entries.length) {
    findings.push({
      id: "disc-test-coverage-gap",
      severity: "medium",
      title: "Potential coverage gap",
      description: "No obvious test directories detected in scoped paths. Validate unit/integration coverage baseline.",
    });
  }

  if (!backlog.length && findings.length) {
    findings.slice(0, 6).forEach((finding, idx) => {
      const sev = String(finding?.severity || "medium").toLowerCase();
      backlog.push({
        id: `disc-rem-${idx + 1}`,
        priority: sev === "high" ? "P1" : "P2",
        title: `Remediate: ${String(finding?.title || finding?.id || "finding")}`,
      });
    });
  }

  if (!rules.length && interfaces.length) {
    rules.push({
      id: "disc-rule-api-contract",
      category: "api_design",
      statement: "Discovered endpoints should be documented and versioned consistently before modernization changes.",
    });
  }
  if (!rules.length && entries.length) {
    rules.push({
      id: "disc-rule-structure",
      category: "packaging_build",
      statement: "Preserve existing repository module boundaries unless architecture plan explicitly approves refactoring.",
    });
  }

  const nodes = Object.values(nodeMap);
  const edges = Object.values(edgeMap);
  if (!nodes.length && !rules.length && !findings.length && !backlog.length) {
    return { nodes: [], edges: [], rules: [], findings: [], backlog: [] };
  }
  return { nodes, edges, rules, findings, backlog };
}

function _discoverData() {
  const p = state.currentRun?.pipeline_state || {};
  const { nodes, edges } = _scmViewFromState();
  const cp = (p.convention_profile && typeof p.convention_profile === "object") ? p.convention_profile : {};
  const health = (p.health_assessment && typeof p.health_assessment === "object") ? p.health_assessment : {};
  const rules = Array.isArray(cp.rules) ? cp.rules : [];
  const findings = Array.isArray(health.findings) ? health.findings : [];
  const backlog = Array.isArray(p.remediation_backlog)
    ? p.remediation_backlog
    : (Array.isArray(health.backlog) ? health.backlog : []);

  if (nodes.length || edges.length || rules.length || findings.length || backlog.length) {
    return { nodes, edges, rules, findings, backlog, demo: false };
  }

  const preview = _discoverPreviewData();
  if (preview.nodes.length || preview.rules.length || preview.findings.length || preview.backlog.length) {
    return { ...preview, demo: false };
  }

  if (!state.projectState.sampleDatasetEnabled) {
    return { nodes: [], edges: [], rules: [], findings: [], backlog: [], demo: false };
  }

  const demoNodes = [
    { id: "svc:orders", name: "orders-service", type: "service" },
    { id: "svc:payments", name: "payments-service", type: "service" },
    { id: "svc:inventory", name: "inventory-service", type: "service" },
    { id: "svc:billing", name: "billing-monolith", type: "service" },
    { id: "db:commerce", name: "commerce_db", type: "database" },
    { id: "db:payments", name: "payments_db", type: "database" },
    { id: "topic:order_created", name: "order.created", type: "message_topic" },
  ];
  const demoEdges = [
    { from: "svc:orders", to: "svc:payments", type: "calls_http", confidence: { score: 0.93 } },
    { from: "svc:orders", to: "topic:order_created", type: "publishes", confidence: { score: 0.9 } },
    { from: "topic:order_created", to: "svc:inventory", type: "consumes", confidence: { score: 0.84 } },
    { from: "svc:orders", to: "db:commerce", type: "writes", confidence: { score: 0.88 } },
    { from: "svc:billing", to: "db:commerce", type: "reads", confidence: { score: 0.79 } },
  ];
  const demoRules = [
    { id: "cp-1", category: "logging_observability", statement: "Logs include correlationId and traceId." },
    { id: "cp-2", category: "testing", statement: "API handlers require unit tests in test/handlers." },
    { id: "cp-3", category: "api_design", statement: "Error responses follow shared error envelope." },
  ];
  const demoFindings = [
    { id: "f-1", severity: "high", title: "Billing monolith hotspot", description: "High churn and complexity in billing engine." },
    { id: "f-2", severity: "high", title: "Shared DB smell", description: "orders-service and billing-monolith share commerce_db." },
    { id: "f-3", severity: "medium", title: "Coverage gap in payments", description: "Low tests in payment retry handlers." },
  ];
  const demoBacklog = [
    { id: "rem-001", priority: "P0", title: "Introduce payment idempotency store" },
    { id: "rem-002", priority: "P1", title: "Strangler facade for billing monolith" },
    { id: "rem-003", priority: "P1", title: "Upgrade vulnerable inventory dependency" },
  ];
  return { nodes: demoNodes, edges: demoEdges, rules: demoRules, findings: demoFindings, backlog: demoBacklog, demo: true };
}

function _nodeKey(node, idx) {
  return String(node?.id || node?.node_id || node?.name || `node-${idx}`);
}

function _edgeFrom(edge) {
  return String(edge?.from || edge?.source || edge?.source_id || "");
}

function _edgeTo(edge) {
  return String(edge?.to || edge?.target || edge?.target_id || "");
}

function _seedHash(value) {
  const s = String(value || "");
  let h = 2166136261;
  for (let i = 0; i < s.length; i += 1) {
    h ^= s.charCodeAt(i);
    h += (h << 1) + (h << 4) + (h << 7) + (h << 8) + (h << 24);
  }
  return Math.abs(h >>> 0);
}

function _heatColor(score) {
  const s = Math.max(0, Math.min(1, Number(score || 0)));
  const r = Math.round(56 + (215 * s));
  const g = Math.round(189 - (120 * s));
  const b = Math.round(248 - (190 * s));
  return `rgb(${r},${g},${b})`;
}

function _baseNodeColor(type) {
  const t = String(type || "").toLowerCase();
  if (t.includes("database") || t.includes("table")) return "#c4b5fd";
  if (t.includes("topic") || t.includes("message")) return "#86efac";
  if (t.includes("infra")) return "#fdba74";
  if (t.includes("external")) return "#f9a8d4";
  return "#93c5fd";
}

function _normalizedGraph(data) {
  const nodeMap = {};
  const nodes = (data.nodes || []).map((n, idx) => {
    const id = _nodeKey(n, idx);
    const out = {
      id,
      name: String(n?.name || id),
      type: String(n?.type || "unknown"),
      confidence: Number(n?.confidence?.score ?? n?.confidence ?? 0.7),
      raw: n,
    };
    nodeMap[id] = out;
    return out;
  });
  const edges = [];
  (data.edges || []).forEach((e, idx) => {
    const from = _edgeFrom(e);
    const to = _edgeTo(e);
    if (!from || !to || !nodeMap[from] || !nodeMap[to]) return;
    edges.push({
      id: String(e?.id || `edge-${idx}`),
      from,
      to,
      type: String(e?.type || "edge"),
      confidence: Number(e?.confidence?.score ?? e?.confidence ?? 0.65),
      raw: e,
    });
  });
  const degree = {};
  nodes.forEach((n) => { degree[n.id] = 0; });
  edges.forEach((e) => {
    degree[e.from] = (degree[e.from] || 0) + 1;
    degree[e.to] = (degree[e.to] || 0) + 1;
  });
  return { nodes, edges, degree };
}

function _cityZoneForNode(node) {
  const id = String(node?.id || "").toLowerCase();
  const name = String(node?.name || "").trim();
  const type = String(node?.type || "").toLowerCase();
  if (type.includes("endpoint")) return "API Surface";
  if (type.includes("database") || type.includes("table")) return "Data Layer";
  if (type.includes("message") || type.includes("topic")) return "Event Mesh";
  if (type.includes("infra")) return "Infrastructure";
  if (id.startsWith("pkg:")) return `Domain: ${name}`;
  if (id.startsWith("mod:") || id.startsWith("cmp:")) {
    const seg = name.split("/").filter(Boolean)[0] || name;
    return `Domain: ${seg}`;
  }
  if (type.includes("service") || type.includes("component") || type.includes("module")) {
    const seg = name.split(/[/._:-]/).filter(Boolean)[0] || name;
    return `Domain: ${seg}`;
  }
  return "Platform";
}

function _cityGraphFromGraph(graph) {
  const zoneByNode = {};
  const zones = {};
  graph.nodes.forEach((node) => {
    const zone = _cityZoneForNode(node);
    zoneByNode[node.id] = zone;
    if (!zones[zone]) {
      zones[zone] = {
        id: `zone:${zone.toLowerCase().replace(/[^a-z0-9]+/g, "-")}`,
        name: zone,
        type: "boundary",
        members: [],
        confidenceTotal: 0,
      };
    }
    zones[zone].members.push(node);
    zones[zone].confidenceTotal += Number(node.confidence || 0.7);
  });

  const cityNodes = Object.values(zones).map((zone) => ({
    id: zone.id,
    name: zone.name,
    type: zone.type,
    confidence: zone.members.length ? (zone.confidenceTotal / zone.members.length) : 0.7,
    raw: {
      size: zone.members.length,
      members: zone.members.map((m) => m.name).slice(0, 16),
      member_ids: zone.members.map((m) => m.id).slice(0, 64),
    },
  }));

  const cityNodeByName = {};
  cityNodes.forEach((n) => { cityNodeByName[n.name] = n; });
  const edgeMap = {};
  graph.edges.forEach((edge) => {
    const fromZone = zoneByNode[edge.from];
    const toZone = zoneByNode[edge.to];
    if (!fromZone || !toZone || fromZone === toZone) return;
    const fromNode = cityNodeByName[fromZone];
    const toNode = cityNodeByName[toZone];
    if (!fromNode || !toNode) return;
    const key = `${fromNode.id}|${toNode.id}`;
    if (!edgeMap[key]) {
      edgeMap[key] = {
        id: `city-edge:${key}`,
        from: fromNode.id,
        to: toNode.id,
        type: "depends_on",
        confidence: 0,
        _count: 0,
      };
    }
    edgeMap[key].confidence += Number(edge.confidence || 0.6);
    edgeMap[key]._count += 1;
  });

  const cityEdges = Object.values(edgeMap).map((e) => ({
    id: e.id,
    from: e.from,
    to: e.to,
    type: e.type,
    confidence: e._count ? (e.confidence / e._count) : 0.6,
    raw: { count: e._count },
  }));

  const degree = {};
  cityNodes.forEach((n) => { degree[n.id] = 0; });
  cityEdges.forEach((e) => {
    degree[e.from] = (degree[e.from] || 0) + 1;
    degree[e.to] = (degree[e.to] || 0) + 1;
  });
  return { nodes: cityNodes, edges: cityEdges, degree };
}

function _buildTypeAnchors(types, width, height) {
  const entries = Array.from(types);
  if (!entries.length) return {};
  const anchors = {};
  const cx = width / 2;
  const cy = height / 2;
  const radius = Math.min(width, height) * 0.32;
  entries.forEach((type, idx) => {
    const angle = (Math.PI * 2 * idx) / Math.max(entries.length, 1);
    anchors[type] = {
      x: cx + (Math.cos(angle) * radius),
      y: cy + (Math.sin(angle) * radius),
    };
  });
  return anchors;
}

function _layoutNodes(graph, width, height) {
  const types = new Set(graph.nodes.map((n) => n.type));
  const anchors = _buildTypeAnchors(types, width, height);
  const bucket = {};
  graph.nodes.forEach((n) => {
    const t = n.type || "unknown";
    if (!bucket[t]) bucket[t] = [];
    bucket[t].push(n);
  });
  const out = {};
  Object.entries(bucket).forEach(([type, nodes]) => {
    const anchor = anchors[type] || { x: width / 2, y: height / 2 };
    const r = Math.max(24, Math.min(width, height) * 0.12);
    nodes.forEach((node, idx) => {
      const angle = (Math.PI * 2 * idx) / Math.max(nodes.length, 1);
      const jitter = (_seedHash(node.id) % 11) - 5;
      out[node.id] = {
        x: anchor.x + (Math.cos(angle) * r) + jitter,
        y: anchor.y + (Math.sin(angle) * r) + jitter,
      };
    });
  });
  return out;
}

function _nodeOverlayScore(node, overlay, graph, findings) {
  const degree = graph.degree[node.id] || 0;
  const maxDegree = Math.max(1, ...Object.values(graph.degree || { x: 1 }));
  const degreeNorm = degree / maxDegree;
  const idText = `${node.id} ${node.name}`.toLowerCase();
  const findingHits = (findings || []).filter((f) => {
    const t = `${f?.title || ""} ${f?.description || ""}`.toLowerCase();
    return idText.includes(String(node.name || "").toLowerCase()) || t.includes(String(node.name || "").toLowerCase());
  });
  const seed = (_seedHash(`${overlay}:${node.id}`) % 100) / 100;
  if (overlay === "churn") return Math.min(1, (idText.includes("legacy") || idText.includes("billing") ? 0.45 : 0.1) + (degreeNorm * 0.45) + (seed * 0.2));
  if (overlay === "complexity") return Math.min(1, (degreeNorm * 0.7) + (seed * 0.3));
  if (overlay === "bug_density") return Math.min(1, Math.min(0.9, findingHits.length * 0.22) + (seed * 0.25));
  if (overlay === "security_risk") {
    const secHits = findingHits.filter((f) => `${f?.title || ""} ${f?.description || ""}`.toLowerCase().includes("security")
      || `${f?.title || ""} ${f?.description || ""}`.toLowerCase().includes("vulnerab"));
    return Math.min(1, (secHits.length * 0.3) + (seed * 0.25));
  }
  if (overlay === "coverage_gaps") {
    const covHits = findingHits.filter((f) => `${f?.title || ""} ${f?.description || ""}`.toLowerCase().includes("coverage")
      || `${f?.title || ""} ${f?.description || ""}`.toLowerCase().includes("test"));
    return Math.min(1, (covHits.length * 0.3) + (seed * 0.2));
  }
  if (overlay === "blast_radius") return Math.min(1, (degreeNorm * 0.85) + (seed * 0.15));
  return 0;
}

function _svgEl(tag, attrs = {}) {
  const elx = document.createElementNS("http://www.w3.org/2000/svg", tag);
  Object.entries(attrs).forEach(([k, v]) => elx.setAttribute(k, String(v)));
  return elx;
}

function _bindGraphInteractions(svgEl, viewKey) {
  if (!svgEl || svgEl.dataset.bindDone === "1") return;
  let dragging = false;
  let startX = 0;
  let startY = 0;
  svgEl.addEventListener("mousedown", (evt) => {
    dragging = true;
    startX = evt.clientX;
    startY = evt.clientY;
  });
  window.addEventListener("mouseup", () => { dragging = false; });
  window.addEventListener("mousemove", (evt) => {
    if (!dragging) return;
    const view = state.graphView[viewKey];
    view.x += evt.clientX - startX;
    view.y += evt.clientY - startY;
    startX = evt.clientX;
    startY = evt.clientY;
    renderDiscoverInsights();
  });
  svgEl.addEventListener("wheel", (evt) => {
    evt.preventDefault();
    const view = state.graphView[viewKey];
    const factor = evt.deltaY > 0 ? 0.92 : 1.08;
    view.scale = Math.max(0.45, Math.min(2.8, view.scale * factor));
    renderDiscoverInsights();
  }, { passive: false });
  svgEl.dataset.bindDone = "1";
}

function _renderGraph(svgEl, graph, opts) {
  if (!svgEl) return;
  const width = 900;
  const height = 360;
  svgEl.innerHTML = "";

  const view = state.graphView[opts.viewKey];
  const container = _svgEl("g", {
    transform: `translate(${view.x},${view.y}) scale(${view.scale})`,
  });
  svgEl.appendChild(container);

  const positions = _layoutNodes(graph, width, height);
  const filterText = String(opts.filterText || "").toLowerCase();
  const selectedId = String(opts.selectedId || "");

  graph.edges.forEach((edge) => {
    const from = positions[edge.from];
    const to = positions[edge.to];
    if (!from || !to) return;
    const edgeEl = _svgEl("line", {
      x1: from.x,
      y1: from.y,
      x2: to.x,
      y2: to.y,
      "stroke-width": Math.max(1, Math.min(3, 1 + (edge.confidence * 1.2))),
      class: "graph-edge",
    });
    const edgeVisible = !filterText
      || edge.from.toLowerCase().includes(filterText)
      || edge.to.toLowerCase().includes(filterText)
      || edge.type.toLowerCase().includes(filterText);
    if (!edgeVisible) edgeEl.setAttribute("opacity", "0.15");
    container.appendChild(edgeEl);
  });

  graph.nodes.forEach((node) => {
    const p = positions[node.id];
    if (!p) return;
    const g = _svgEl("g", { class: "graph-node" });
    const overlayScore = _nodeOverlayScore(node, opts.overlay, graph, opts.findings || []);
    const fill = opts.overlay === "none" ? _baseNodeColor(node.type) : _heatColor(overlayScore);
    const radius = 8 + Math.round(Math.min(12, (graph.degree[node.id] || 0) * 1.4));
    const nodeVisible = !filterText
      || node.id.toLowerCase().includes(filterText)
      || node.name.toLowerCase().includes(filterText)
      || node.type.toLowerCase().includes(filterText);
    const isSelected = selectedId && selectedId === node.id;
    const c = _svgEl("circle", {
      cx: p.x,
      cy: p.y,
      r: radius,
      fill,
      stroke: isSelected ? "#0f172a" : "#334155",
      "stroke-width": isSelected ? "2.2" : "1.2",
      opacity: nodeVisible ? "1" : "0.15",
    });
    const lbl = _svgEl("text", {
      x: p.x + radius + 2,
      y: p.y + 3,
      class: "graph-node-label",
      opacity: nodeVisible ? "1" : "0.2",
    });
    lbl.textContent = node.name.length > 24 ? `${node.name.slice(0, 24)}...` : node.name;
    g.appendChild(c);
    g.appendChild(lbl);
    g.addEventListener("click", () => opts.onNodeClick(node));
    container.appendChild(g);
  });
}

function _cityInspectorHtml(node, graph, data) {
  if (!node) {
    return `
      <p><strong>Map summary</strong></p>
      <p class="mt-1">Nodes: ${graph.nodes.length} | Edges: ${graph.edges.length}</p>
      <p class="mt-1">Overlay: ${escapeHtml(state.cityOverlay)}</p>
      <p class="mt-1 text-slate-600">${data.demo ? "Sample dataset mode." : "Live context data."}</p>
    `;
  }
  const members = Array.isArray(node?.raw?.members) ? node.raw.members : [];
  const memberCount = Number(node?.raw?.size || members.length || 0);
  const neighbors = graph.edges
    .filter((e) => e.from === node.id || e.to === node.id)
    .slice(0, 8)
    .map((e) => `${e.from === node.id ? "→" : "←"} ${e.from === node.id ? e.to : e.from} (${e.type})`);
  const findingMatches = (data.findings || []).filter((f) => {
    const hay = `${f?.title || ""} ${f?.description || ""}`.toLowerCase();
    return members.some((m) => hay.includes(String(m).toLowerCase()));
  }).slice(0, 5);
  return `
    <p><strong>${escapeHtml(node.name)}</strong></p>
    <p class="mt-1">Zone size: ${memberCount} components | Degree: ${graph.degree[node.id] || 0}</p>
    <p class="mt-1">Confidence: ${Number(node.confidence || 0).toFixed(2)}</p>
    <p class="mt-1 font-semibold">Contained components</p>
    <ul class="mt-1 list-disc pl-4">${members.slice(0, 8).map((m) => `<li>${escapeHtml(m)}</li>`).join("") || "<li>None</li>"}</ul>
    <p class="mt-1 font-semibold">Dependencies</p>
    <ul class="mt-1 list-disc pl-4">${neighbors.map((x) => `<li>${escapeHtml(x)}</li>`).join("") || "<li>None</li>"}</ul>
    <p class="mt-1 font-semibold">Risk signals</p>
    <ul class="mt-1 list-disc pl-4">${findingMatches.map((f) => `<li>${escapeHtml(f.title || f.id || "finding")}</li>`).join("") || "<li>No explicit findings mapped yet.</li>"}</ul>
  `;
}

function _systemInspectorHtml(node, graph) {
  if (!node) {
    return `<p>Search and click nodes to inspect evidence, confidence, and adjacency.</p>`;
  }
  const inEdges = graph.edges.filter((e) => e.to === node.id).slice(0, 6);
  const outEdges = graph.edges.filter((e) => e.from === node.id).slice(0, 6);
  return `
    <p><strong>${escapeHtml(node.name)}</strong></p>
    <p class="mt-1">ID: ${escapeHtml(node.id)}</p>
    <p class="mt-1">Type: ${escapeHtml(node.type)} | Confidence: ${Number(node.confidence || 0).toFixed(2)}</p>
    <div class="mt-1">
      <p class="font-semibold">Outbound</p>
      <ul class="list-disc pl-4">${outEdges.map((e) => `<li>${escapeHtml(e.to)} (${escapeHtml(e.type)})</li>`).join("") || "<li>None</li>"}</ul>
    </div>
    <div class="mt-1">
      <p class="font-semibold">Inbound</p>
      <ul class="list-disc pl-4">${inEdges.map((e) => `<li>${escapeHtml(e.from)} (${escapeHtml(e.type)})</li>`).join("") || "<li>None</li>"}</ul>
    </div>
  `;
}

function _renderCityAndSystemGraphs(data) {
  const systemGraph = _normalizedGraph(data);
  if (!systemGraph.nodes.length) {
    if (el.cityMapSvg) el.cityMapSvg.innerHTML = "";
    if (el.systemMapSvg) el.systemMapSvg.innerHTML = "";
    if (el.cityMapInspector) el.cityMapInspector.textContent = "No components detected. Check scope settings or run ingestion.";
    if (el.systemMapInspector) el.systemMapInspector.textContent = "No graph data available yet.";
    return;
  }
  const cityGraph = _cityGraphFromGraph(systemGraph);

  _bindGraphInteractions(el.cityMapSvg, "city");
  _bindGraphInteractions(el.systemMapSvg, "system");

  const citySelected = state.graphSelected?.city || "";
  if (citySelected && !cityGraph.nodes.some((n) => n.id === citySelected)) {
    state.graphSelected.city = "";
  }
  _renderGraph(el.cityMapSvg, cityGraph, {
    viewKey: "city",
    overlay: state.cityOverlay,
    selectedId: state.graphSelected?.city || "",
    findings: data.findings,
    onNodeClick: (node) => {
      state.graphSelected = state.graphSelected || {};
      state.graphSelected.city = node.id;
      if (el.cityMapInspector) el.cityMapInspector.innerHTML = _cityInspectorHtml(node, cityGraph, data);
      _renderCityAndSystemGraphs(data);
    },
  });
  if (el.cityMapInspector && !(state.graphSelected?.city || "")) {
    el.cityMapInspector.innerHTML = _cityInspectorHtml(null, cityGraph, data);
  }

  const systemSelected = state.graphSelected?.system || "";
  if (systemSelected && !systemGraph.nodes.some((n) => n.id === systemSelected)) {
    state.graphSelected.system = "";
  }
  _renderGraph(el.systemMapSvg, systemGraph, {
    viewKey: "system",
    overlay: "none",
    filterText: state.systemSearch,
    selectedId: state.graphSelected?.system || "",
    onNodeClick: (node) => {
      state.graphSelected = state.graphSelected || {};
      state.graphSelected.system = node.id;
      if (el.systemMapInspector) el.systemMapInspector.innerHTML = _systemInspectorHtml(node, systemGraph);
      _renderCityAndSystemGraphs(data);
    },
  });
  if (el.systemMapInspector && !(state.graphSelected?.system || "")) {
    el.systemMapInspector.innerHTML = _systemInspectorHtml(null, systemGraph);
  }
}

function renderDiscoverInsights() {
  const data = _discoverData();
  const { nodes, edges, rules, findings, backlog, demo } = data;
  _renderCityAndSystemGraphs(data);

  if (el.discoverCityMapContent && el.discoverCityMapContent.dataset) {
    el.discoverCityMapContent.dataset.mode = demo ? "demo" : "live";
  }
  if (el.discoverSystemMapContent && el.discoverSystemMapContent.dataset) {
    el.discoverSystemMapContent.dataset.mode = demo ? "demo" : "live";
  }

  if (el.discoverHealthContent) {
    const findingRows = findings.slice(0, 8).map((f) => `
      <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1">
        <strong>${escapeHtml(String(f.severity || "info").toUpperCase())}</strong> · ${escapeHtml(f.title || f.id || "")}
      </div>
    `).join("");
    const backlogRows = backlog.slice(0, 8).map((b) => `
      <div class="rounded border border-slate-300 bg-white px-2 py-1">
        ${escapeHtml(String(b.priority || "P2"))} · ${escapeHtml(b.title || b.id || "")}
      </div>
    `).join("");
    el.discoverHealthContent.innerHTML = (findingRows || backlogRows)
      ? `
          <div class="grid gap-2 sm:grid-cols-2">
            <div><p class="mb-1 font-semibold text-slate-900">Findings</p>${findingRows || "<p>No findings</p>"}</div>
            <div><p class="mb-1 font-semibold text-slate-900">Backlog</p>${backlogRows || "<p>No backlog items</p>"}</div>
          </div>
        `
      : `<p class="text-slate-700">No findings detected in the current scope.</p>`;
  }

  if (el.discoverConventionsContent) {
    const categoryCounts = {};
    rules.forEach((r) => {
      const cat = String(r.category || "uncategorized");
      categoryCounts[cat] = (categoryCounts[cat] || 0) + 1;
    });
    const categories = Object.entries(categoryCounts)
      .map(([cat, count]) => `<div class="rounded border border-slate-300 bg-slate-50 px-2 py-1">${escapeHtml(cat)}: <strong>${count}</strong></div>`)
      .join("");
    const examples = rules.slice(0, 6).map((r) => `<li>${escapeHtml(r.statement || r.title || r.id || "")}</li>`).join("");
    el.discoverConventionsContent.innerHTML = rules.length
      ? `
          <div class="flex flex-wrap gap-1">${categories}</div>
          <ul class="mt-2 list-disc pl-4">${examples}</ul>
        `
      : `<p class="text-slate-700">No convention rules available yet.</p>`;
  }
}

function applySampleDatasetPreset() {
  state.projectState.sampleDatasetEnabled = true;
  if (el.projectStateMode) el.projectStateMode.value = "brownfield";
  if (el.bfRepoProvider) el.bfRepoProvider.value = "github";
  if (el.bfRepoUrl) el.bfRepoUrl.value = "https://github.com/acme/acme-commerce-platform";
  if (el.bfIssueProvider) el.bfIssueProvider.value = "jira";
  if (el.bfIssueProject) el.bfIssueProject.value = "ACME";
  if (el.bfDocsUrl) el.bfDocsUrl.value = "https://confluence.acme.local/commerce";
  if (el.bfRuntimeTelemetry) el.bfRuntimeTelemetry.checked = true;
  if (el.analysisDepth) el.analysisDepth.value = "deep";
  if (el.telemetryMode) el.telemetryMode.value = "staging";
  if (el.includePaths) el.includePaths.value = "services/\nlegacy/\ninfra/";
  if (el.excludePaths) el.excludePaths.value = "node_modules/\ndist/\nvendor/";
  if (el.domainPackSelect) el.domainPackSelect.value = "auto";
  if (el.domainJurisdiction) el.domainJurisdiction.value = "AUTO";
  if (el.domainDataClassification) el.domainDataClassification.value = "";
  if (el.domainPackJson) el.domainPackJson.value = "";
  if (el.taskType) el.taskType.value = "business_objectives";
  if (el.objectives) {
    el.objectives.value = "Improve payment reliability and compliance for ACME checkout while reducing duplicate charges and increasing traceability.";
    el.objectives.dataset.autogen = "0";
  }
  applyProjectStateResult({
    detected: "brownfield",
    confidence: 0.99,
    reason: "Using sample ACME brownfield dataset for guided demo.",
  });
  autoTriggerDiscoverExternalViews({ force: true });
  renderDomainPackControls();
  setDiscoverStep(4);
  setDiscoverResultsView("city");
  renderDiscoverInsights();
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

function settingsLlmRefs(provider) {
  if (provider === "anthropic") {
    return {
      status: el.settingsLlmAnthropicStatus,
      message: el.settingsLlmAnthropicMessage,
      fields: {
        model: el.settingsLlmAnthropicModel,
        base_url: el.settingsLlmAnthropicBaseUrl,
        api_key: el.settingsLlmAnthropicKey,
      },
      secretKey: "api_key",
    };
  }
  return {
    status: el.settingsLlmOpenaiStatus,
    message: el.settingsLlmOpenaiMessage,
    fields: {
      model: el.settingsLlmOpenaiModel,
      base_url: el.settingsLlmOpenaiBaseUrl,
      api_key: el.settingsLlmOpenaiKey,
    },
    secretKey: "api_key",
  };
}

function collectLlmPayload(provider) {
  const refs = settingsLlmRefs(provider);
  const payload = {};
  Object.entries(refs.fields || {}).forEach(([key, node]) => {
    if (!node) return;
    payload[key] = String(node.value || "").trim();
  });
  return payload;
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
        run_export_enabled: el.settingsGithubRunExportEnabled,
        export_base_url: el.settingsGithubExportBaseUrl,
        export_owner: el.settingsGithubExportOwner,
        export_repository: el.settingsGithubExportRepository,
        export_branch: el.settingsGithubExportBranch,
        export_prefix: el.settingsGithubExportPrefix,
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
      const writeScope = data.read_only ? "none (read-only mode)" : "pull requests + run artifact exports";
      parts.push(`Permissions: read scope=${readScope}; write scope=${writeScope}`);
      if (data.run_export_enabled) {
        const exportBranch = String(data.export_branch || "default branch");
        const exportPrefix = String(data.export_prefix || "synthetix");
        const targetOwner = String(data.export_owner || data.owner || "-");
        const targetRepo = String(data.export_repository || data.repository || "-");
        parts.push(`Run export: enabled (${targetOwner}/${targetRepo} -> ${exportPrefix}/runs/<run_id> on ${exportBranch})`);
      } else {
        parts.push("Run export: disabled");
      }
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

function updateLlmForm(provider, cfg) {
  const refs = settingsLlmRefs(provider);
  const data = (cfg && typeof cfg === "object") ? cfg : {};
  if (refs.status) {
    const status = String(data.status || (data.connected ? "connected" : "disconnected"));
    refs.status.textContent = status === "connected" ? "Connected" : "Not connected";
    refs.status.className = integrationStatusClass(status);
  }
  Object.entries(refs.fields || {}).forEach(([key, node]) => {
    if (!node) return;
    if (key === refs.secretKey) {
      node.value = "";
      return;
    }
    node.value = String(data[key] || "");
  });
  if (refs.message) {
    const masked = String(data.api_key_masked || "");
    const parts = [];
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
  const llm = state.settings?.llm || {};
  const defaultProvider = String(llm.default_provider || "anthropic").toLowerCase();
  if (!state.currentRun) {
    if (el.provider && ["anthropic", "openai"].includes(defaultProvider)) {
      el.provider.value = defaultProvider;
    }
    setDefaultModelByProvider();
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
  const llm = settings.llm || {};
  const llmProviders = llm.providers || {};
  updateLlmForm("anthropic", llmProviders.anthropic || {});
  updateLlmForm("openai", llmProviders.openai || {});

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
  const payload = collectIntegrationPayload(provider);
  const data = await api(`/api/settings/integrations/${encodeURIComponent(provider)}/test`, payload, "POST");
  state.settings = data.settings || state.settings;
  renderSettings();
  const ok = !!data.test_ok;
  const checks = Array.isArray(data?.checks) ? data.checks : [];
  const passCount = checks.filter((x) => !!x?.ok).length;
  const failed = checks.filter((x) => !x?.ok);
  const failedSummary = failed.length
    ? ` Failed checks: ${failed.map((x) => `${x.name}: ${x.message}`).join(" | ")}`
    : "";
  setSettingsMessage(
    `${provider.toUpperCase()} integration test ${ok ? "passed" : "failed"}${checks.length ? ` (${passCount}/${checks.length} checks)` : ""}.${failedSummary}`,
    !ok
  );
}

async function disconnectIntegration(provider) {
  const data = await api(`/api/settings/integrations/${encodeURIComponent(provider)}/disconnect`, { clear_secret: false }, "POST");
  state.settings = data.settings || state.settings;
  renderSettings();
  setSettingsMessage(`${provider.toUpperCase()} integration disconnected.`);
}

async function saveLlmProvider(provider) {
  const payload = collectLlmPayload(provider);
  const data = await api(`/api/settings/llm/${encodeURIComponent(provider)}/connect`, payload, "POST");
  state.settings = data.settings || state.settings;
  renderSettings();
  setSettingsMessage(`${provider.toUpperCase()} LLM credentials saved.`);
}

async function testLlmProvider(provider) {
  const payload = collectLlmPayload(provider);
  const data = await api(`/api/settings/llm/${encodeURIComponent(provider)}/test`, payload, "POST");
  state.settings = data.settings || state.settings;
  renderSettings();
  const ok = !!data.test_ok;
  const checks = Array.isArray(data?.checks) ? data.checks : [];
  const passCount = checks.filter((x) => !!x?.ok).length;
  const failed = checks.filter((x) => !x?.ok);
  const failedSummary = failed.length
    ? ` Failed checks: ${failed.map((x) => `${x.name}: ${x.message}`).join(" | ")}`
    : "";
  setSettingsMessage(
    `${provider.toUpperCase()} LLM test ${ok ? "passed" : "failed"}${checks.length ? ` (${passCount}/${checks.length} checks)` : ""}.${failedSummary}`,
    !ok
  );
}

async function disconnectLlmProvider(provider) {
  const data = await api(`/api/settings/llm/${encodeURIComponent(provider)}/disconnect`, { clear_secret: false }, "POST");
  state.settings = data.settings || state.settings;
  renderSettings();
  setSettingsMessage(`${provider.toUpperCase()} LLM credentials disconnected.`);
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
  const provider = String(el.provider?.value || "anthropic").toLowerCase();
  const savedModel = String(state.settings?.llm?.providers?.[provider]?.model || "").trim();
  if (savedModel) {
    el.model.value = savedModel;
    return;
  }
  el.model.value = provider === "anthropic" ? "claude-sonnet-4-20250514" : "gpt-4o";
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

function modernizationSourceMode() {
  return String(el.modernizationSourceMode?.value || "manual");
}

function isModernizationRepoScanMode() {
  return isCodeModernizationMode() && modernizationSourceMode() === "repo_scan";
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
  if (el.databasePanel) {
    el.databasePanel.classList.remove("hidden");
  }
  if (el.modernizationManualInputs) {
    const hideManual = codeMode && isModernizationRepoScanMode();
    el.modernizationManualInputs.classList.toggle("hidden", hideManual);
  }
  if (el.modernizationSourceHelp) {
    if (!codeMode) {
      el.modernizationSourceHelp.textContent = "";
    } else if (isModernizationRepoScanMode()) {
      el.modernizationSourceHelp.textContent = "The analyst will scan the connected GitHub repository to infer current functionality.";
    } else {
      el.modernizationSourceHelp.textContent = "Use pasted/uploaded code when migrating a specific legacy file or module.";
    }
  }

  if ((codeMode || dbMode) && ((el.objectives.value || "").trim() === "" || el.objectives.dataset.autogen === "1")) {
    setAutogeneratedObjective();
  }
  renderDiscoverStepper();
}

function toggleCloudConfig() {
  const cloudRequested = el.deploymentTarget.value === "cloud";
  const cloudAllowed = !!el.enableCloudPromotion?.checked;
  el.cloudConfigBox.classList.toggle("hidden", !(cloudRequested && cloudAllowed));
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
  const sampleDatasetEnabled = !!state.projectState.sampleDatasetEnabled;
  state.projectState = {
    mode: String(el.projectStateMode?.value || "auto"),
    detected,
    confidence: Number(result?.confidence || 0),
    reason: String(result?.reason || ""),
    sampleDatasetEnabled,
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

function renderDiscoverGitHubTreePreview() {
  if (!el.bfGithubTreeStatus || !el.bfGithubTreePreview) return;
  const view = state.discoverGithubTree || {};
  if (view.loading) {
    el.bfGithubTreeStatus.textContent = "Loading repository tree...";
    el.bfGithubTreeStatus.className = "text-[11px] text-slate-700";
    el.bfGithubTreePreview.innerHTML = `<p class="text-slate-700">Fetching repository structure from GitHub API.</p>`;
    return;
  }
  if (view.error) {
    el.bfGithubTreeStatus.textContent = `Load failed: ${view.error}`;
    el.bfGithubTreeStatus.className = "text-[11px] text-rose-700";
    el.bfGithubTreePreview.innerHTML = `<p class="text-rose-700">${escapeHtml(view.error)}</p>`;
    return;
  }
  const tree = (view.tree && typeof view.tree === "object") ? view.tree : {};
  const repo = (view.repo && typeof view.repo === "object") ? view.repo : {};
  const entries = Array.isArray(tree.entries) ? tree.entries : [];
  if (!entries.length) {
    el.bfGithubTreeStatus.textContent = "No repository tree loaded.";
    el.bfGithubTreeStatus.className = "text-[11px] text-slate-700";
    el.bfGithubTreePreview.innerHTML = `<p class="text-slate-700">Enter a GitHub repository URL and click <strong>Load repo tree</strong> to preview folders and files.</p>`;
    return;
  }
  const owner = escapeHtml(repo.owner || "-");
  const repository = escapeHtml(repo.repository || "-");
  const branch = escapeHtml(repo.default_branch || "-");
  const folders = Number(tree.folders || 0);
  const files = Number(tree.files || 0);
  const total = Number(tree.total_entries || entries.length || 0);
  const truncated = !!tree.truncated;
  el.bfGithubTreeStatus.textContent = `${owner}/${repository} @ ${branch} | ${folders} folders | ${files} files${truncated ? " | truncated" : ""}`;
  el.bfGithubTreeStatus.className = "text-[11px] text-emerald-700";

  const maxRender = 240;
  const rows = entries.slice(0, maxRender).map((entry) => {
    const path = escapeHtml(entry.path || "");
    const depth = Math.max(0, Math.min(24, Number(entry.depth || 0)));
    const indent = "&nbsp;".repeat(depth * 2);
    const kind = String(entry.type || "").toLowerCase() === "dir" ? "[DIR]" : "[FILE]";
    return `<li class="leading-5">${indent}<span class="text-slate-500">${kind}</span> <span class="text-slate-900">${path}</span></li>`;
  }).join("");
  const hiddenCount = Math.max(0, total - maxRender);
  el.bfGithubTreePreview.innerHTML = `
    <div class="text-[11px] text-slate-700">Showing ${Math.min(total, maxRender)} of ${total} entries.</div>
    <ul class="mt-1 font-mono text-[11px] text-slate-900">${rows}</ul>
    ${hiddenCount ? `<p class="mt-1 text-[11px] text-slate-700">${hiddenCount} more entries not shown.</p>` : ""}
  `;
}

function renderDiscoverLinearIssuesPreview() {
  if (!el.bfLinearIssuesStatus || !el.bfLinearIssuesPreview) return;
  const view = state.discoverLinearIssues || {};
  const issueProvider = String(el.bfIssueProvider?.value || "").toLowerCase();
  const providerLabel = issueProvider ? issueProvider.toUpperCase() : "issue tracker";
  if (el.bfLoadLinearIssues) {
    el.bfLoadLinearIssues.textContent = issueProvider ? `Load ${providerLabel} issues` : "Load issues";
  }
  if (view.loading) {
    el.bfLinearIssuesStatus.textContent = `Loading ${providerLabel} issues...`;
    el.bfLinearIssuesStatus.className = "text-[11px] text-slate-700";
    el.bfLinearIssuesPreview.innerHTML = `<p class="text-slate-700">Fetching issue list from ${escapeHtml(providerLabel)} API.</p>`;
    return;
  }
  if (view.error) {
    el.bfLinearIssuesStatus.textContent = `Load failed: ${view.error}`;
    el.bfLinearIssuesStatus.className = "text-[11px] text-rose-700";
    el.bfLinearIssuesPreview.innerHTML = `<p class="text-rose-700">${escapeHtml(view.error)}</p>`;
    return;
  }
  const issues = Array.isArray(view.issues) ? view.issues : [];
  if (!issues.length) {
    el.bfLinearIssuesStatus.textContent = "No issue list loaded.";
    el.bfLinearIssuesStatus.className = "text-[11px] text-slate-700";
    el.bfLinearIssuesPreview.innerHTML = `<p class="text-slate-700">Set issue provider and click <strong>Load issues</strong>.</p>`;
    return;
  }
  const team = (view.team && typeof view.team === "object") ? view.team : {};
  const teamLabel = [team.key, team.name].filter(Boolean).join(" - ");
  el.bfLinearIssuesStatus.textContent = `${providerLabel} | ${teamLabel || "board"} | ${issues.length} issues loaded`;
  el.bfLinearIssuesStatus.className = "text-[11px] text-emerald-700";
  const rows = issues.slice(0, 80).map((issue) => {
    const id = escapeHtml(issue.identifier || issue.id || "-");
    const title = escapeHtml(issue.title || "");
    const stateLabel = escapeHtml(issue.state || "Unknown");
    const priority = Number(issue.priority || 0);
    const project = escapeHtml(issue.project || "-");
    const assignee = escapeHtml(issue.assignee || "Unassigned");
    const updated = escapeHtml(String(issue.updated_at || "").slice(0, 10) || "-");
    return `
      <div class="mb-1 rounded border border-slate-300 bg-slate-50 px-2 py-1">
        <p class="text-[11px] font-semibold text-slate-900">${id} - ${title}</p>
        <p class="text-[10px] text-slate-700">State: ${stateLabel} | Priority: ${priority} | Project: ${project}</p>
        <p class="text-[10px] text-slate-700">Assignee: ${assignee} | Updated: ${updated}</p>
      </div>
    `;
  }).join("");
  el.bfLinearIssuesPreview.innerHTML = rows;
}

function renderDiscoverIntegrationPreviews() {
  renderDiscoverGitHubTreePreview();
  renderDiscoverLinearIssuesPreview();
}

function analystBriefRequestKey() {
  const integration = getIntegrationContext();
  const brownfield = integration?.brownfield || {};
  const payload = [
    String(currentUseCase() || ""),
    String(modernizationSourceMode() || "manual"),
    String(integration?.project_state_detected || ""),
    String(brownfield.repo_provider || ""),
    String(brownfield.repo_url || ""),
    String(el.objectives?.value || "").trim().slice(0, 500),
    String(el.legacyCode?.value || "").trim().slice(0, 500),
    String(el.dbSchema?.value || "").trim().slice(0, 500),
    String(el.includePaths?.value || "").trim(),
    String(el.excludePaths?.value || "").trim(),
    String(integration?.sample_dataset_enabled ? "sample" : "live"),
  ];
  return payload.join("|");
}

function renderDiscoverAnalystBrief() {
  if (!el.discoverAnalystBriefStatus || !el.discoverAnalystBriefPreview) return;
  const view = state.discoverAnalystBrief || {};
  if (view.loading) {
    el.discoverAnalystBriefStatus.textContent = "Analyst Agent is reading sampled source files and inferring functionality...";
    el.discoverAnalystBriefStatus.className = "mt-1 text-[11px] text-slate-700";
    el.discoverAnalystBriefPreview.innerHTML = `<p class="text-slate-700">Running source-aware analysis...</p>`;
    return;
  }
  if (view.error) {
    el.discoverAnalystBriefStatus.textContent = `Load failed: ${view.error}`;
    el.discoverAnalystBriefStatus.className = "mt-1 text-[11px] text-rose-700";
    el.discoverAnalystBriefPreview.innerHTML = `<p class="text-rose-700">${escapeHtml(view.error)}</p>`;
    return;
  }
  const payload = (view.data && typeof view.data === "object") ? view.data : {};
  const brief = (payload.analyst_brief && typeof payload.analyst_brief === "object") ? payload.analyst_brief : {};
  const summary = (brief.summary && typeof brief.summary === "object") ? brief.summary : {};
  const aas = (payload.aas && typeof payload.aas === "object") ? payload.aas : {};
  const requirementsPack = (payload.requirements_pack && typeof payload.requirements_pack === "object")
    ? payload.requirements_pack
    : ((aas.requirements_pack && typeof aas.requirements_pack === "object") ? aas.requirements_pack : {});
  const aasSummary = String(payload.assistant_summary || aas.assistant_summary || "").trim();
  const qualityGates = Array.isArray(payload.quality_gates)
    ? payload.quality_gates
    : (Array.isArray(aas.quality_gates) ? aas.quality_gates : []);
  const gateRows = qualityGates
    .map((row) => {
      const name = escapeHtml(String(row?.name || row?.gate || "gate"));
      const status = escapeHtml(String(row?.status || "unknown"));
      const message = escapeHtml(String(row?.message || ""));
      return `<li><strong>${name}</strong>: ${status}${message ? ` — ${message}` : ""}</li>`;
    })
    .join("");
  const source = escapeHtml(String(payload.source || "-"));
  const repo = (payload.repo && typeof payload.repo === "object") ? payload.repo : {};
  const repoLabel = [repo.owner, repo.repository].filter(Boolean).join("/");

  const overview = escapeHtml(String(summary.overview || ""));
  const caps = Array.isArray(summary.likely_capabilities) ? summary.likely_capabilities : [];
  const io = Array.isArray(summary.input_output_contracts) ? summary.input_output_contracts : [];
  const components = Array.isArray(summary.key_components) ? summary.key_components : [];
  const interfaces = Array.isArray(summary.interfaces) ? summary.interfaces : [];
  const dataHints = Array.isArray(summary.data_and_state) ? summary.data_and_state : [];
  const domainFunctions = Array.isArray(summary.domain_functions) ? summary.domain_functions : [];
  const dataEntities = Array.isArray(summary.data_entities) ? summary.data_entities : [];
  const unknowns = Array.isArray(summary.unknowns) ? summary.unknowns : [];
  const evidenceFiles = Array.isArray(summary.evidence_files) ? summary.evidence_files : [];
  const legacySkillProfile = (summary.legacy_skill_profile && typeof summary.legacy_skill_profile === "object")
    ? summary.legacy_skill_profile
    : {};
  const stats = (summary.stats && typeof summary.stats === "object") ? summary.stats : {};
  const rpFunctional = Array.isArray(requirementsPack?.requirements?.functional) ? requirementsPack.requirements.functional : [];
  const rpControls = Array.isArray(requirementsPack?.compliance?.controls_triggered) ? requirementsPack.compliance.controls_triggered : [];
  const rpOpenQuestions = Array.isArray(requirementsPack?.open_questions) ? requirementsPack.open_questions : [];

  if (!overview && !caps.length && !components.length) {
    el.discoverAnalystBriefStatus.textContent = "Waiting to analyze connected source code.";
    el.discoverAnalystBriefStatus.className = "mt-1 text-[11px] text-slate-700";
    el.discoverAnalystBriefPreview.innerHTML = "Analyst summary will appear here after Scope → Scan.";
    return;
  }

  const list = (rows) => rows.map((row) => `<li>${escapeHtml(String(row || ""))}</li>`).join("");
  el.discoverAnalystBriefStatus.textContent = `Analyst brief ready (${source}${repoLabel ? ` | ${repoLabel}` : ""})`;
  el.discoverAnalystBriefStatus.className = "mt-1 text-[11px] text-emerald-700";
  el.discoverAnalystBriefPreview.innerHTML = `
    <p class="font-semibold text-slate-900">${overview}</p>
    ${Object.keys(legacySkillProfile).length ? `<p class="mt-1 text-[11px] text-slate-800"><strong>Selected legacy skill:</strong> ${escapeHtml(String(legacySkillProfile.selected_skill_name || "Generic Legacy Skill"))} (${escapeHtml(String(legacySkillProfile.selected_skill_id || "generic_legacy"))}), confidence=${escapeHtml(String(legacySkillProfile.confidence || "n/a"))}</p>` : ""}
    ${aasSummary ? `<p class="mt-2 rounded-md border border-sky-300 bg-sky-50 px-2 py-1 text-slate-900"><strong>Analyst AAS summary:</strong> ${escapeHtml(aasSummary)}</p>` : ""}
    <div class="mt-2 grid gap-2 sm:grid-cols-2">
      <div><p class="font-semibold text-slate-900">Likely functionality</p><ul class="mt-1 list-disc pl-4">${list(caps.slice(0, 8))}</ul></div>
      <div><p class="font-semibold text-slate-900">Input/output behavior</p><ul class="mt-1 list-disc pl-4">${list(io.slice(0, 6))}</ul></div>
      <div><p class="font-semibold text-slate-900">Key components</p><ul class="mt-1 list-disc pl-4">${list(components.slice(0, 8))}</ul></div>
      <div><p class="font-semibold text-slate-900">Interface hints</p><ul class="mt-1 list-disc pl-4">${list(interfaces.slice(0, 8))}</ul></div>
      <div><p class="font-semibold text-slate-900">Data/state clues</p><ul class="mt-1 list-disc pl-4">${list(dataHints.slice(0, 6))}</ul></div>
      <div><p class="font-semibold text-slate-900">Domain functions</p><ul class="mt-1 list-disc pl-4">${list((domainFunctions.length ? domainFunctions : ["No explicit function names extracted."]).slice(0, 8))}</ul></div>
      <div><p class="font-semibold text-slate-900">Data entities</p><ul class="mt-1 list-disc pl-4">${list((dataEntities.length ? dataEntities : ["No table/entity hints extracted."]).slice(0, 8))}</ul></div>
      <div><p class="font-semibold text-slate-900">Unknowns</p><ul class="mt-1 list-disc pl-4">${list((unknowns.length ? unknowns : ["No major unknowns reported."]).slice(0, 6))}</ul></div>
      <div><p class="font-semibold text-slate-900">Evidence files</p><ul class="mt-1 list-disc pl-4">${list((evidenceFiles.length ? evidenceFiles : ["No files were read during this analysis."]).slice(0, 10))}</ul></div>
    </div>
    ${requirementsPack && Object.keys(requirementsPack).length ? `
      <div class="mt-2 rounded-md border border-slate-300 bg-white px-2 py-2">
        <p class="font-semibold text-slate-900">Requirements pack</p>
        <p class="text-[10px] text-slate-700">Artifact: ${escapeHtml(String(requirementsPack.artifact_id || "n/a"))}</p>
        <p class="text-[11px] text-slate-800">Functional requirements: ${rpFunctional.length} | Controls: ${rpControls.length} | Open questions: ${rpOpenQuestions.length}</p>
        ${gateRows ? `<ul class="mt-1 list-disc pl-4 text-[11px] text-slate-800">${gateRows}</ul>` : ""}
      </div>
    ` : ""}
    <p class="mt-2 text-[10px] text-slate-700">Sampled files: ${Number(stats.sampled_files || 0)} | Tree entries considered: ${Number(stats.sampled_tree_entries || 0)} | Route hints: ${Number(stats.route_hints || 0)}</p>
  `;
}

async function loadDiscoverAnalystBrief({ force = false } = {}) {
  const reqKey = analystBriefRequestKey();
  if (!force && reqKey && reqKey === state.discoverAnalystBrief.requestKey && state.discoverAnalystBrief.data) {
    renderDiscoverAnalystBrief();
    return;
  }
  const previousThreadId = String(state.discoverAnalystBrief?.threadId || state.discoverAnalystBrief?.data?.thread_id || "").trim();
  state.discoverAnalystBrief = { loading: true, error: "", data: null, requestKey: reqKey, threadId: previousThreadId };
  renderDiscoverAnalystBrief();
  const integration = getIntegrationContext();
  try {
    const discoverData = await api("/api/discover/analyst-brief", {
      integration_context: integration,
      objectives: String(el.objectives?.value || "").trim(),
      use_case: currentUseCase(),
      legacy_code: String(el.legacyCode?.value || "").trim(),
      database_source: String(el.dbSource?.value || "").trim(),
      database_target: String(el.dbTarget?.value || "").trim(),
      database_schema: String(el.dbSchema?.value || "").trim(),
      repo_provider: String(integration?.brownfield?.repo_provider || ""),
      repo_url: String(integration?.brownfield?.repo_url || "").trim(),
    }, "POST");

    const candidateThreadParts = [
      String(integration?.project_state_detected || ""),
      String(integration?.brownfield?.repo_url || ""),
      String(integration?.greenfield?.repo_target || ""),
      String(currentUseCase() || ""),
    ].filter(Boolean);
    const inferredThreadId = previousThreadId || `discover-${slugifyValue(candidateThreadParts.join("-")) || "thread"}`;

    const requirement = String(el.objectives?.value || "").trim()
      || String(discoverData?.analyst_brief?.summary?.overview || "").trim()
      || "Analyze repository context and produce a requirements pack.";

    let aasData = (discoverData?.aas && typeof discoverData.aas === "object") ? discoverData.aas : null;
    if (!aasData) {
      try {
        aasData = await api("/api/agents/analyst/analyze-requirement", {
          requirement,
          business_objective: requirement,
          use_case: currentUseCase(),
          thread_id: inferredThreadId,
          workspace_id: "default-workspace",
          client_id: "default-client",
          project_id: slugifyValue(String(integration?.brownfield?.repo_url || integration?.greenfield?.repo_target || "default-project")) || "default-project",
          domain_pack_id: String(integration?.domain_pack_id || ""),
          domain_pack: integration?.custom_domain_pack || null,
          jurisdiction: String(integration?.jurisdiction || ""),
          data_classification: Array.isArray(integration?.data_classification) ? integration.data_classification : [],
          integration_context: integration,
        }, "POST");
      } catch (_aasErr) {
        aasData = null;
      }
    }

    const mergedData = {
      ...(discoverData && typeof discoverData === "object" ? discoverData : {}),
    };
    if (aasData && typeof aasData === "object") {
      mergedData.aas = aasData;
      mergedData.assistant_summary = String(aasData.assistant_summary || "");
      mergedData.requirements_pack = (aasData.requirements_pack && typeof aasData.requirements_pack === "object")
        ? aasData.requirements_pack
        : {};
      mergedData.quality_gates = Array.isArray(aasData.quality_gates) ? aasData.quality_gates : [];
      mergedData.thread_id = String(aasData.thread_id || inferredThreadId);
    } else {
      mergedData.thread_id = inferredThreadId;
    }

    state.discoverAnalystBrief = {
      loading: false,
      error: "",
      data: mergedData,
      requestKey: reqKey,
      threadId: String(mergedData.thread_id || inferredThreadId),
    };
  } catch (err) {
    state.discoverAnalystBrief = {
      loading: false,
      error: String(err?.message || err || "Failed to run analyst brief."),
      data: null,
      requestKey: reqKey,
      threadId: previousThreadId,
    };
  }
  renderDiscoverAnalystBrief();
  renderDiscoverInsights();
}

async function loadDiscoverGithubTree() {
  const integration = getIntegrationContext();
  const repoUrl = String(integration?.brownfield?.repo_url || "").trim();
  if (!repoUrl) {
    state.discoverGithubTree = { loading: false, error: "Repo URL is required.", repo: null, tree: null };
    renderDiscoverIntegrationPreviews();
    renderDiscoverInsights();
    return;
  }
  state.discoverGithubTree = { loading: true, error: "", repo: null, tree: null };
  renderDiscoverIntegrationPreviews();
  try {
    const data = await api("/api/discover/github/tree", {
      integration_context: integration,
      repo_url: repoUrl,
      max_entries: 1200,
      sample_dataset_enabled: !!integration.sample_dataset_enabled,
    }, "POST");
    state.discoverGithubTree = {
      loading: false,
      error: "",
      repo: data.repo || null,
      tree: data.tree || null,
    };
  } catch (err) {
    state.discoverGithubTree = { loading: false, error: String(err?.message || err || "Failed to load repo tree."), repo: null, tree: null };
  }
  renderDiscoverIntegrationPreviews();
  renderDiscoverInsights();
}

async function loadDiscoverLinearIssues() {
  const integration = getIntegrationContext();
  const issueProvider = String(integration?.brownfield?.issue_provider || "").toLowerCase();
  if (!integration.sample_dataset_enabled && issueProvider && !["linear", "jira"].includes(issueProvider)) {
    state.discoverLinearIssues = {
      loading: false,
      error: `Issue provider ${issueProvider.toUpperCase()} is not supported yet for preview.`,
      team: null,
      issues: [],
      source: "",
    };
    renderDiscoverIntegrationPreviews();
    renderDiscoverInsights();
    return;
  }
  state.discoverLinearIssues = { loading: true, error: "", team: null, issues: [], source: "" };
  renderDiscoverIntegrationPreviews();
  try {
    const data = await api("/api/discover/issues", {
      integration_context: integration,
      issue_provider: issueProvider || "linear",
      issue_project: String(integration?.brownfield?.issue_project || "").trim(),
      max_issues: 80,
      sample_dataset_enabled: !!integration.sample_dataset_enabled,
    }, "POST");
    state.discoverLinearIssues = {
      loading: false,
      error: "",
      team: data.team || null,
      issues: Array.isArray(data.issues) ? data.issues : [],
      source: String(data.source || ""),
    };
  } catch (err) {
    state.discoverLinearIssues = {
      loading: false,
      error: String(err?.message || err || "Failed to load issues."),
      team: null,
      issues: [],
      source: "",
    };
  }
  renderDiscoverIntegrationPreviews();
  renderDiscoverInsights();
}

function autoTriggerDiscoverExternalViews({ force = false } = {}) {
  const integration = getIntegrationContext();
  const detectedState = String(integration?.project_state_detected || state.projectState.detected || "").toLowerCase();
  if (detectedState !== "brownfield") return;

  const repoProvider = String(integration?.brownfield?.repo_provider || "").toLowerCase();
  const repoUrl = String(integration?.brownfield?.repo_url || "").trim();
  const issueProvider = String(integration?.brownfield?.issue_provider || "").toLowerCase();
  const issueProject = String(integration?.brownfield?.issue_project || "").trim();

  if (repoProvider === "github" && repoUrl) {
    const githubKey = `${repoProvider}|${repoUrl}|${integration.sample_dataset_enabled ? "sample" : "live"}`;
    if (force || githubKey !== state.discoverAutoFetch.githubKey) {
      state.discoverAutoFetch.githubKey = githubKey;
      loadDiscoverGithubTree().catch((err) => {
        state.discoverGithubTree = {
          loading: false,
          error: String(err?.message || err || "Failed to load repo tree."),
          repo: null,
          tree: null,
        };
        renderDiscoverIntegrationPreviews();
      });
    }
  }

  if (["linear", "jira"].includes(issueProvider) && issueProject) {
    const linearKey = `${issueProvider}|${issueProject}|${integration.sample_dataset_enabled ? "sample" : "live"}`;
    if (force || linearKey !== state.discoverAutoFetch.linearKey) {
      state.discoverAutoFetch.linearKey = linearKey;
      loadDiscoverLinearIssues().catch((err) => {
        state.discoverLinearIssues = {
          loading: false,
          error: String(err?.message || err || "Failed to load issues."),
          team: null,
          issues: [],
          source: "",
        };
        renderDiscoverIntegrationPreviews();
      });
    }
  }
}

function getIntegrationContext() {
  const analystData = (state.discoverAnalystBrief?.data && typeof state.discoverAnalystBrief.data === "object")
    ? state.discoverAnalystBrief.data
    : {};
  const analystSummary = (analystData.analyst_brief && typeof analystData.analyst_brief === "object" && analystData.analyst_brief.summary && typeof analystData.analyst_brief.summary === "object")
    ? analystData.analyst_brief.summary
    : null;
  const analystReqPack = (analystData.requirements_pack && typeof analystData.requirements_pack === "object")
    ? analystData.requirements_pack
    : null;
  const analystAas = (analystData.aas && typeof analystData.aas === "object")
    ? analystData.aas
    : {};
  const domainPack = currentDomainPackConfig();
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
      modernization_source_mode: String(el.modernizationSourceMode?.value || "manual"),
      include_paths: parseLines(el.includePaths?.value || ""),
      exclude_paths: parseLines(el.excludePaths?.value || ""),
    },
    domain_pack_selection: String(domainPack.selected || "auto"),
    domain_pack_id: String(domainPack.domain_pack_id || ""),
    custom_domain_pack: domainPack.custom_domain_pack || null,
    domain_pack_error: String(domainPack.error || ""),
    jurisdiction: String(domainPack.jurisdiction || ""),
    data_classification: Array.isArray(domainPack.data_classification) ? domainPack.data_classification : [],
    discover_cache: {
      analyst_source: String(analystData.source || ""),
      analyst_repo: (analystData.repo && typeof analystData.repo === "object") ? analystData.repo : {},
      analyst_thread_id: String(state.discoverAnalystBrief?.threadId || analystData.thread_id || analystAas.thread_id || ""),
      analyst_aas_summary: String(analystData.assistant_summary || analystAas.assistant_summary || ""),
      analyst_summary: analystSummary ? {
        overview: String(analystSummary.overview || ""),
        likely_capabilities: Array.isArray(analystSummary.likely_capabilities) ? analystSummary.likely_capabilities.slice(0, 12) : [],
        key_components: Array.isArray(analystSummary.key_components) ? analystSummary.key_components.slice(0, 12) : [],
        evidence_files: Array.isArray(analystSummary.evidence_files) ? analystSummary.evidence_files.slice(0, 24) : [],
        input_output_contracts: Array.isArray(analystSummary.input_output_contracts) ? analystSummary.input_output_contracts.slice(0, 16) : [],
        domain_functions: Array.isArray(analystSummary.domain_functions) ? analystSummary.domain_functions.slice(0, 40) : [],
        data_entities: Array.isArray(analystSummary.data_entities) ? analystSummary.data_entities.slice(0, 40) : [],
        legacy_skill_profile: (analystSummary.legacy_skill_profile && typeof analystSummary.legacy_skill_profile === "object")
          ? analystSummary.legacy_skill_profile
          : {},
        vb6_analysis: (analystSummary.vb6_analysis && typeof analystSummary.vb6_analysis === "object")
          ? analystSummary.vb6_analysis
          : {},
      } : {},
      analyst_requirements_pack: analystReqPack ? analystReqPack : null,
    },
    cloud_promotion_enabled: !!el.enableCloudPromotion?.checked,
    sample_dataset_enabled: !!state.projectState.sampleDatasetEnabled,
  };
}

function applyIntegrationContext(ctx) {
  if (!ctx || typeof ctx !== "object") return;
  const mode = String(ctx.project_state_mode || "auto");
  if (el.projectStateMode) el.projectStateMode.value = mode;
  const detected = String(ctx.project_state_detected || "");
  const confidence = Number(ctx.project_state_confidence || 0);
  const reason = String(ctx.project_state_reason || "");
  state.projectState.sampleDatasetEnabled = !!ctx.sample_dataset_enabled;
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
  if (el.modernizationSourceMode) el.modernizationSourceMode.value = String(scope.modernization_source_mode || "manual");
  if (el.includePaths) el.includePaths.value = Array.isArray(scope.include_paths) ? scope.include_paths.join("\n") : "";
  if (el.excludePaths) el.excludePaths.value = Array.isArray(scope.exclude_paths) ? scope.exclude_paths.join("\n") : "";

  const selection = String(
    ctx.domain_pack_selection
    || (ctx.custom_domain_pack ? "custom" : "")
    || (ctx.domain_pack_id || "auto")
  ).trim() || "auto";
  if (el.domainPackSelect) {
    renderDomainPackCatalog();
    if ([...el.domainPackSelect.options].some((opt) => String(opt.value) === selection)) {
      el.domainPackSelect.value = selection;
    } else if (selection && selection !== "custom" && selection !== "auto") {
      const opt = document.createElement("option");
      opt.value = selection;
      opt.textContent = `${selection} (from run context)`;
      el.domainPackSelect.appendChild(opt);
      el.domainPackSelect.value = selection;
    } else {
      el.domainPackSelect.value = "auto";
    }
  }
  if (el.domainJurisdiction) el.domainJurisdiction.value = String(ctx.jurisdiction || "AUTO") || "AUTO";
  if (el.domainDataClassification) {
    const dc = Array.isArray(ctx.data_classification) ? ctx.data_classification : [];
    el.domainDataClassification.value = dc.join(", ");
  }
  if (el.domainPackJson) {
    const customPack = (ctx.custom_domain_pack && typeof ctx.custom_domain_pack === "object") ? ctx.custom_domain_pack : null;
    el.domainPackJson.value = customPack ? JSON.stringify(customPack, null, 2) : "";
  }
  const discoverCache = (ctx.discover_cache && typeof ctx.discover_cache === "object") ? ctx.discover_cache : {};
  const cachedSummary = (discoverCache.analyst_summary && typeof discoverCache.analyst_summary === "object")
    ? discoverCache.analyst_summary
    : {};
  const cachedReqPack = (discoverCache.analyst_requirements_pack && typeof discoverCache.analyst_requirements_pack === "object")
    ? discoverCache.analyst_requirements_pack
    : null;
  const cachedAasSummary = String(discoverCache.analyst_aas_summary || "").trim();
  const cachedThreadId = String(discoverCache.analyst_thread_id || "").trim();
  const hasCachedAnalyst = !!String(cachedSummary.overview || "").trim()
    || (Array.isArray(cachedSummary.evidence_files) && cachedSummary.evidence_files.length > 0)
    || !!cachedReqPack
    || !!cachedAasSummary;
  if (hasCachedAnalyst) {
    state.discoverAnalystBrief = {
      loading: false,
      error: "",
      data: {
        source: String(discoverCache.analyst_source || ""),
        repo: (discoverCache.analyst_repo && typeof discoverCache.analyst_repo === "object") ? discoverCache.analyst_repo : {},
        analyst_brief: {
          title: "Analyst functionality understanding",
          summary: cachedSummary,
        },
        requirements_pack: cachedReqPack || {},
        assistant_summary: cachedAasSummary,
        thread_id: cachedThreadId,
      },
      requestKey: analystBriefRequestKey(),
      threadId: cachedThreadId,
    };
  } else if (cachedThreadId) {
    state.discoverAnalystBrief.threadId = cachedThreadId;
  }
  if (el.enableCloudPromotion) el.enableCloudPromotion.checked = !!ctx.cloud_promotion_enabled;
  renderDomainPackControls();
  toggleCloudConfig();
  renderDiscoverStepper();
}

function discoverStepCompletion() {
  const integration = getIntegrationContext();
  const sampleMode = !!integration.sample_dataset_enabled;
  const projectStateReady = !!integration.project_state_detected;
  let connectComplete = projectStateReady;
  if (sampleMode) connectComplete = true;
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
  const customDomainPackValid = !String(integration.domain_pack_error || "").trim()
    && (
      String(integration.domain_pack_selection || "auto") !== "custom"
      || !!(integration.custom_domain_pack && typeof integration.custom_domain_pack === "object")
    );
  const scopeComplete = !!objective
    && (
      !isCodeModernizationMode()
      || (
        isModernizationRepoScanMode()
          ? (
            String(integration.brownfield.repo_provider || "").toLowerCase() === "github"
            && !!String(integration.brownfield.repo_url || "").trim()
          )
          : !!String(el.legacyCode?.value || "").trim()
      )
    )
    && (!isDatabaseConversionMode() || !!String(el.dbSchema?.value || "").trim())
    && customDomainPackValid;
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
  if (target === 3) {
    loadDiscoverAnalystBrief({ force: false }).catch(() => {});
  }
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
      const sampleNote = integration.sample_dataset_enabled ? "Demo dataset mode enabled." : "";
      el.discoverResultsSummary.textContent = `${ready} ${sampleNote} ${integration.project_state_reason || ""}`.trim();
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
    const domainLabel = String(integration.domain_pack_selection || "auto") === "auto"
      ? "auto"
      : (String(integration.domain_pack_selection || "") === "custom"
        ? (integration.custom_domain_pack?.id || "custom")
        : String(integration.domain_pack_id || integration.domain_pack_selection || "auto"));
    el.discoverResultsScan.textContent = `Scan profile: ${integration.scan_scope.analysis_depth} | telemetry=${integration.scan_scope.telemetry_mode} | domain_pack=${domainLabel} | include=${includeCount} | exclude=${excludeCount}`;
  }
  renderDiscoverAnalystBrief();
  renderDiscoverIntegrationPreviews();
  renderDiscoverInsights();
  renderDiscoverResultsView();
}

function validateDiscoverStep(step) {
  const c = discoverStepCompletion();
  if (step === 1 && !c.connectComplete) {
    alert("Complete Connect sources, or use the sample dataset option to continue without external integrations.");
    return false;
  }
  if (step === 2 && !c.scopeComplete) {
    alert("Complete Define scope: provide objectives, required legacy/database inputs, and a valid domain pack configuration.");
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
      requirements_pack_profile: agent?.requirements_pack_profile || "",
      requirements_pack_template: (agent?.requirements_pack_template && typeof agent.requirements_pack_template === "object")
        ? agent.requirements_pack_template
        : {},
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
    const reqPack = Number(stage) === 1
      ? String(persona.requirements_pack_profile || "").trim()
      : "";
    return `
      <div class="rounded-lg border border-slate-300 bg-white p-2 text-xs text-slate-800">
        <div class="font-semibold text-slate-900">Stage ${stage}: ${escapeHtml(stageName)}</div>
        <div class="mt-1 text-slate-900">${escapeHtml(persona.display_name || "Unassigned")}</div>
        <div class="mt-1 text-slate-700">${escapeHtml(persona.persona || "")}</div>
        ${reqPack ? `<div class="mt-1 text-[11px] text-sky-800"><strong>Requirements Pack:</strong> ${escapeHtml(reqPack)}</div>` : ""}
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
  const reqPack = Number(stage) === 1 ? String(current.requirements_pack_profile || "").trim() : "";
  return `
    <div class="rounded-lg border border-slate-300 bg-slate-50 p-3" data-stage="${stage}">
      <label class="mb-1 block text-xs font-semibold uppercase tracking-[0.14em] text-slate-700">Stage ${stage}: ${escapeHtml(stageInfo?.name || "")}</label>
      <select data-team-stage="${stage}" class="w-full rounded-md border border-slate-300 bg-white px-2 py-2 text-xs text-slate-900">${selectOptions}</select>
      <p class="mt-2 text-xs text-slate-700" data-team-persona="${stage}">${escapeHtml(current.persona || "")}</p>
      ${reqPack ? `<p class="mt-1 text-[11px] text-sky-800"><strong>Requirements Pack:</strong> ${escapeHtml(reqPack)}</p>` : ""}
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
      ${Number(agent.stage || 0) === 1 && String(agent.requirements_pack_profile || "").trim() ? `<p class="mt-1 text-[11px] text-sky-800"><strong>Req Pack:</strong> ${escapeHtml(agent.requirements_pack_profile)}</p>` : ""}
    </div>
  `).join("") : "<p class='text-xs text-slate-700'>No agents available.</p>";

  el.cloneBaseAgent.innerHTML = all.map((agent) => {
    const reqPack = Number(agent.stage || 0) === 1 && String(agent.requirements_pack_profile || "").trim()
      ? ` | RP: ${agent.requirements_pack_profile}`
      : "";
    return `<option value="${escapeHtml(agent.id)}">S${agent.stage} | ${escapeHtml(agent.display_name || agent.role || agent.id)}${escapeHtml(reqPack)}</option>`;
  }).join("");
  refreshCloneRequirementsPackFields();
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
  if (mode === MODES.VERIFY) renderVerifyPanels();
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

function refreshCloneRequirementsPackFields() {
  const selectedProfile = String(el.cloneRequirementsPackProfile?.value || "").trim();
  const baseAgentId = String(el.cloneBaseAgent?.value || "").trim();
  const baseAgent = (state.agents.all || []).find((agent) => String(agent.id || "") === baseAgentId) || {};
  const isAnalyst = Number(baseAgent.stage || 0) === 1;
  if (el.cloneRequirementsPackProfile) {
    el.cloneRequirementsPackProfile.disabled = !isAnalyst;
  }
  if (el.cloneRequirementsPackTemplate) {
    const showTemplate = isAnalyst && selectedProfile === "custom-template";
    el.cloneRequirementsPackTemplate.classList.toggle("hidden", !showTemplate);
  }
}

async function cloneAgentFromBuilder() {
  const baseAgentId = String(el.cloneBaseAgent.value || "").trim();
  if (!baseAgentId) {
    alert("Choose a base agent to clone.");
    return;
  }
  const selectedProfile = String(el.cloneRequirementsPackProfile?.value || "").trim();
  const profile = selectedProfile === "custom-template" ? "requirements-pack-v2-custom" : selectedProfile;
  let template = {};
  if (selectedProfile === "custom-template") {
    const rawTemplate = String(el.cloneRequirementsPackTemplate?.value || "").trim();
    if (!rawTemplate) {
      alert("Provide a custom requirements pack template JSON.");
      return;
    }
    try {
      const parsed = JSON.parse(rawTemplate);
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
        alert("Custom requirements pack template must be a JSON object.");
        return;
      }
      template = parsed;
    } catch (err) {
      alert(`Invalid custom requirements pack template JSON: ${err.message || err}`);
      return;
    }
  }
  const data = await api("/api/agents/clone", {
    base_agent_id: baseAgentId,
    display_name: String(el.cloneAgentName.value || "").trim(),
    persona: String(el.cloneAgentPersona.value || "").trim(),
    requirements_pack_profile: profile,
    requirements_pack_template: template,
  });
  el.cloneAgentMessage.textContent = `Cloned ${data.agent?.display_name || "agent"}`;
  el.cloneAgentName.value = "";
  el.cloneAgentPersona.value = "";
  if (el.cloneRequirementsPackProfile) el.cloneRequirementsPackProfile.value = "";
  if (el.cloneRequirementsPackTemplate) {
    el.cloneRequirementsPackTemplate.value = "";
    el.cloneRequirementsPackTemplate.classList.add("hidden");
  }
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
  renderVerifyPanels();
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

function _statusPill(status) {
  const s = String(status || "").toLowerCase();
  if (s === "done" || s === "completed") return "border-emerald-300 bg-emerald-50 text-emerald-900";
  if (s === "in_progress" || s === "running") return "border-sky-300 bg-sky-50 text-sky-900";
  if (s === "blocked" || s === "failed") return "border-rose-300 bg-rose-50 text-rose-900";
  return "border-slate-300 bg-slate-100 text-slate-800";
}

async function refreshWorkItems() {
  const data = await api("/api/work-items", null);
  state.workItems = Array.isArray(data.work_items) ? data.work_items : [];
  renderWorkItems();
}

function renderWorkItems() {
  if (!el.workItemsList) return;
  const rows = Array.isArray(state.workItems) ? state.workItems : [];
  if (!rows.length) {
    el.workItemsList.innerHTML = "<div class='rounded-lg border border-slate-300 bg-white p-3 text-xs text-slate-700'>No work items yet. Create one to start planning.</div>";
    return;
  }
  el.workItemsList.innerHTML = rows.map((item) => `
    <div class="rounded-lg border border-slate-300 bg-white p-3">
      <div class="flex flex-wrap items-center justify-between gap-2">
        <div>
          <p class="text-sm font-semibold text-slate-900">${escapeHtml(item.title || "")}</p>
          <p class="text-[11px] text-slate-700">${escapeHtml((item.type || "").toUpperCase())} · recommended=${escapeHtml((item.recommended_type || "").toUpperCase())} · governance=${escapeHtml(item.governance_tier || "")}</p>
        </div>
        <span class="rounded border px-2 py-0.5 text-[10px] font-semibold ${_statusPill(item.status)}">${escapeHtml(String(item.status || "open").toUpperCase())}</span>
      </div>
      <p class="mt-2 text-xs text-slate-700">${escapeHtml(item.description || "")}</p>
      <p class="mt-1 text-[11px] text-slate-600">Risk: ${escapeHtml(item.risk_tier || "n/a")} · Complexity: ${escapeHtml(item.complexity_score)} · Blast radius: ${escapeHtml(item.blast_radius)}</p>
      <div class="mt-2 flex flex-wrap gap-2">
        <button data-work-status="${escapeHtml(item.id)}::in_progress" class="btn-light rounded-md px-2 py-1 text-xs font-semibold">Start</button>
        <button data-work-status="${escapeHtml(item.id)}::done" class="btn-light rounded-md px-2 py-1 text-xs font-semibold">Mark done</button>
        <button data-work-status="${escapeHtml(item.id)}::blocked" class="btn-light rounded-md px-2 py-1 text-xs font-semibold">Block</button>
      </div>
    </div>
  `).join("");
  el.workItemsList.querySelectorAll("[data-work-status]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const raw = btn.getAttribute("data-work-status") || "";
      const [id, status] = raw.split("::");
      if (!id || !status) return;
      try {
        await api(`/api/work-items/${encodeURIComponent(id)}/status`, { status });
        await refreshWorkItems();
      } catch (err) {
        alert(`Failed to update work item: ${err.message}`);
      }
    });
  });
}

async function createWorkItem() {
  const title = String(el.workItemTitle?.value || "").trim();
  const description = String(el.workItemDescription?.value || "").trim();
  if (!title || !description) {
    alert("Work item title and description are required.");
    return;
  }
  const payload = {
    title,
    description,
    type: String(el.workItemType?.value || "auto").toLowerCase(),
    governance_tier: String(el.workItemGovernance?.value || "standard").toLowerCase(),
    linked_issue: String(el.workItemLinkedIssue?.value || "").trim(),
    run_id: String(state.currentRunId || ""),
    source: "manual",
  };
  const data = await api("/api/work-items", payload);
  if (el.workItemRecommendation) {
    const rec = data.recommendation || {};
    el.workItemRecommendation.textContent = `Recommended: ${(rec.recommended_type || "").toUpperCase()} | risk=${rec.risk_tier || "n/a"} | complexity=${rec.complexity_score || 0} | blast radius=${rec.blast_radius || 0}`;
  }
  if (el.workItemTitle) el.workItemTitle.value = "";
  if (el.workItemDescription) el.workItemDescription.value = "";
  await refreshWorkItems();
}

function _runPipelineState(run) {
  return (run?.pipeline_state && typeof run.pipeline_state === "object") ? run.pipeline_state : {};
}

function _stageResult(run, stageNum) {
  const p = _runPipelineState(run);
  const rows = Array.isArray(p.agent_results) ? p.agent_results : [];
  for (let idx = rows.length - 1; idx >= 0; idx -= 1) {
    const row = rows[idx];
    if (!row || typeof row !== "object") continue;
    if (Number(row.stage || 0) === Number(stageNum)) return row;
  }
  return null;
}

function _stageOutput(run, stageNum) {
  const row = _stageResult(run, stageNum);
  const out = row?.output;
  return (out && typeof out === "object") ? out : {};
}

function _topologySnapshot(run) {
  const p = _runPipelineState(run);
  const scm = (p.system_context_model && typeof p.system_context_model === "object") ? p.system_context_model : {};
  const graph = (scm.graph && typeof scm.graph === "object") ? scm.graph : {};
  const nodes = Array.isArray(graph.nodes) ? graph.nodes : [];
  const edges = Array.isArray(graph.edges) ? graph.edges : [];
  const nodeSet = new Set(
    nodes
      .map((n) => (n && typeof n === "object" ? String(n.id || n.name || "").trim() : ""))
      .filter(Boolean)
  );
  const edgeSet = new Set(
    edges
      .map((e) => {
        if (!e || typeof e !== "object") return "";
        return `${String(e.from || "").trim()}|${String(e.type || "").trim()}|${String(e.to || "").trim()}`;
      })
      .filter(Boolean)
  );
  return { nodeSet, edgeSet, nodes: nodeSet.size, edges: edgeSet.size };
}

function _contractSnapshot(run) {
  const endpoints = new Set();
  const p = _runPipelineState(run);
  const scm = (p.system_context_model && typeof p.system_context_model === "object") ? p.system_context_model : {};
  const graph = (scm.graph && typeof scm.graph === "object") ? scm.graph : {};
  const nodes = Array.isArray(graph.nodes) ? graph.nodes : [];
  nodes.forEach((node) => {
    if (!node || typeof node !== "object") return;
    const t = String(node.type || "").toLowerCase();
    if (t !== "endpoint" && t !== "route") return;
    const name = String(node.name || node.id || "").trim();
    if (name) endpoints.add(name);
  });

  const architectOut = _stageOutput(run, 2);
  const candidates = []
    .concat(Array.isArray(architectOut.api_contracts) ? architectOut.api_contracts : [])
    .concat(Array.isArray(architectOut.endpoints) ? architectOut.endpoints : []);
  candidates.forEach((row) => {
    if (typeof row === "string") {
      const x = row.trim();
      if (x) endpoints.add(x);
      return;
    }
    if (!row || typeof row !== "object") return;
    const method = String(row.method || "").trim().toUpperCase();
    const path = String(row.path || row.route || "").trim();
    if (path) endpoints.add(`${method ? `${method} ` : ""}${path}`.trim());
  });

  const cp = (p.convention_profile && typeof p.convention_profile === "object") ? p.convention_profile : {};
  const rules = Array.isArray(cp.rules) ? cp.rules : [];
  const ruleSet = new Set(rules.map((r) => String((r && typeof r === "object" ? (r.id || r.title || "") : "")).trim()).filter(Boolean));
  return { endpoints, ruleSet };
}

function _diffSnapshot(currentRun, previousRun) {
  if (!currentRun || !previousRun) {
    return {
      topology: { added_nodes: 0, removed_nodes: 0, added_edges: 0, removed_edges: 0, examples: [] },
      contract: { added_endpoints: 0, removed_endpoints: 0, rule_changes: 0, examples: [] },
    };
  }
  const curTopo = _topologySnapshot(currentRun);
  const prevTopo = _topologySnapshot(previousRun);
  const addedNodes = [...curTopo.nodeSet].filter((x) => !prevTopo.nodeSet.has(x));
  const removedNodes = [...prevTopo.nodeSet].filter((x) => !curTopo.nodeSet.has(x));
  const addedEdges = [...curTopo.edgeSet].filter((x) => !prevTopo.edgeSet.has(x));
  const removedEdges = [...prevTopo.edgeSet].filter((x) => !curTopo.edgeSet.has(x));

  const curContract = _contractSnapshot(currentRun);
  const prevContract = _contractSnapshot(previousRun);
  const addedEndpoints = [...curContract.endpoints].filter((x) => !prevContract.endpoints.has(x));
  const removedEndpoints = [...prevContract.endpoints].filter((x) => !curContract.endpoints.has(x));
  const addedRules = [...curContract.ruleSet].filter((x) => !prevContract.ruleSet.has(x));
  const removedRules = [...prevContract.ruleSet].filter((x) => !curContract.ruleSet.has(x));

  return {
    topology: {
      added_nodes: addedNodes.length,
      removed_nodes: removedNodes.length,
      added_edges: addedEdges.length,
      removed_edges: removedEdges.length,
      examples: []
        .concat(addedNodes.slice(0, 2).map((x) => `+ node ${x}`))
        .concat(removedNodes.slice(0, 2).map((x) => `- node ${x}`))
        .concat(addedEdges.slice(0, 2).map((x) => `+ edge ${x}`))
        .concat(removedEdges.slice(0, 2).map((x) => `- edge ${x}`)),
    },
    contract: {
      added_endpoints: addedEndpoints.length,
      removed_endpoints: removedEndpoints.length,
      rule_changes: addedRules.length + removedRules.length,
      examples: []
        .concat(addedEndpoints.slice(0, 3).map((x) => `+ endpoint ${x}`))
        .concat(removedEndpoints.slice(0, 3).map((x) => `- endpoint ${x}`))
        .concat(addedRules.slice(0, 2).map((x) => `+ rule ${x}`))
        .concat(removedRules.slice(0, 2).map((x) => `- rule ${x}`)),
    },
  };
}

function _controlsFromRun(run) {
  const analystOut = _stageOutput(run, 1);
  const reqPack = (analystOut.requirements_pack && typeof analystOut.requirements_pack === "object")
    ? analystOut.requirements_pack
    : {};
  const compliance = (reqPack.compliance && typeof reqPack.compliance === "object")
    ? reqPack.compliance
    : {};
  const controls = Array.isArray(compliance.controls_triggered) ? compliance.controls_triggered : [];
  const status = String(run?.status || "").toLowerCase();
  return controls.map((row, idx) => ({
    control_id: String((row && typeof row === "object" ? (row.id || row.control_id || `CTRL-${idx + 1}`) : `CTRL-${idx + 1}`)),
    framework: String((row && typeof row === "object" ? (row.framework || "CONTROL") : "CONTROL")),
    objective: String((row && typeof row === "object" ? (row.control_objective || row.objective || row.name || "") : "")),
    status: status === "completed" ? "PASS" : (status === "failed" ? "FAIL" : "AT_RISK"),
  }));
}

function _testsFromRun(run) {
  const testerOut = _stageOutput(run, 6);
  const overall = (testerOut.overall_results && typeof testerOut.overall_results === "object") ? testerOut.overall_results : {};
  const failedChecks = Array.isArray(testerOut.failed_checks) ? testerOut.failed_checks : [];
  const total = Number(overall.total_tests || 0);
  const passed = Number(overall.passed || 0);
  const failed = Number(overall.failed || failedChecks.length || 0);
  const passRate = total > 0 ? passed / total : 0;
  return {
    summary: {
      p0_required: true,
      p0_automated_percent: Math.round(passRate * 100),
      overall_pass_rate: Math.round(passRate * 1000) / 1000,
      status: failed > 0 ? "NEEDS_CHANGES" : "PASS",
    },
    failed_scenarios: failedChecks.slice(0, 12).map((x, idx) => ({
      scenario_id: String(x?.name || `CHECK-${idx + 1}`),
      reason: String(x?.reason || x?.root_cause || "Check failed"),
      maps_to: Array.isArray(x?.maps_to) ? x.maps_to : [],
    })),
  };
}

function buildEvidencePackFragment(run, baselineRun = null) {
  const r = run || {};
  const p = _runPipelineState(r);
  const ref = (p.context_vault_ref && typeof p.context_vault_ref === "object") ? p.context_vault_ref : {};
  const runStatus = String(r.status || "unknown").toLowerCase();
  const testerOut = _stageOutput(r, 6);
  const qualityGate = String(testerOut?.overall_results?.quality_gate || "").toLowerCase();
  const readiness = (runStatus === "completed" && qualityGate !== "fail") ? "PASS" : "BLOCKED";
  const pendingApprovals = []
    .concat(p.pending_approval ? [p.pending_approval] : [])
    .concat(Array.isArray(p.approvals_required) ? p.approvals_required : []);
  const tests = _testsFromRun(r);
  const diff = _diffSnapshot(r, baselineRun);
  const controls = _controlsFromRun(r);
  const traceabilityLinks = Array.isArray(_stageOutput(r, 1)?.requirements_pack?.traceability?.links)
    ? _stageOutput(r, 1).requirements_pack.traceability.links
    : [];

  return {
    artifact_type: "evidence_pack_fragment",
    artifact_version: "1.0",
    artifact_id: `EVID-${String(r.run_id || "unknown")}`,
    generated_at: new Date().toISOString(),
    generated_by: {
      agent_name: "Validation Analyst Agent",
      persona_version: "1.0",
      mode: "evidence_compiler",
    },
    references: {
      run_context: {
        run_id: String(r.run_id || ""),
        pipeline: "Discover -> Plan -> Build -> Verify",
        governance_tier: p.strict_security_mode ? "Strict" : "Standard",
        environment: "local",
      },
      change_context: {
        repo: String(ref.repo || ""),
        branch: String(ref.branch || ""),
        commit_sha: String(ref.commit_sha || ""),
        scm_version: String(p?.system_context_model?.version || "1.0"),
        cp_version: String(p?.convention_profile?.version || "1.0"),
        ha_version: String(p?.health_assessment?.version || "1.0"),
      },
    },
    client_readout: {
      release_readiness: {
        status: readiness,
        summary: readiness === "PASS"
          ? "Release gates are satisfied for current run."
          : "Release is blocked due to failed stages, quality gate issues, or pending approvals.",
        blocking_items: runStatus === "completed" ? [] : [{ id: "BLOCK-001", type: "pipeline", title: `Run status is ${String(r.status || "").toUpperCase()}`, recommended_action: "Resolve failed stages and rerun verify gates." }],
        pending_approvals: pendingApprovals.map((appr, idx) => ({
          id: String(appr.id || `APR-${idx + 1}`),
          role: String(appr.role || "Reviewer"),
          title: String(appr.title || appr.message || "Approval required"),
          status: "PENDING",
        })),
      },
      what_changed: [
        `Topology deltas: +${diff.topology.added_nodes} nodes, -${diff.topology.removed_nodes} nodes, +${diff.topology.added_edges} edges, -${diff.topology.removed_edges} edges.`,
        `Contract deltas: +${diff.contract.added_endpoints} endpoints, -${diff.contract.removed_endpoints} endpoints, ${diff.contract.rule_changes} convention rule changes.`,
      ],
      risk_posture: {
        overall: readiness === "PASS" ? "LOW_TO_MEDIUM" : "HIGH",
        notes: readiness === "PASS" ? ["No blocking release issues detected in latest verify gate."] : ["One or more release blockers need remediation before production promotion."],
      },
    },
    engineering_detail: {
      system_map_delta: {
        topology_changes: diff.topology.examples,
        contract_changes: diff.contract.examples,
      },
    },
    controls_and_compliance: {
      controls_triggered: controls,
    },
    test_evidence: tests,
    security_gates: {
      status: runStatus === "completed" ? "PASS_WITH_NOTES" : "NEEDS_REVIEW",
      scans: [],
      notes: [p.strict_security_mode ? "Strict security mode was enabled for this run." : "Standard security mode used."],
    },
    traceability_matrix: {
      status: traceabilityLinks.length ? "PARTIAL" : "MISSING",
      links: traceabilityLinks.slice(0, 50),
    },
    approvals_and_exceptions: {
      approvals_required: pendingApprovals,
      exceptions: Array.isArray(p.policy_exceptions) ? p.policy_exceptions : [],
    },
    final_gate_summary: {
      overall_status: readiness,
      release_candidate: readiness === "PASS",
    },
    exports: {
      available: [
        { type: "json", path: `artifacts/evidence/evidence-pack-${String(r.run_id || "run")}.json` },
        { type: "pdf", path: `artifacts/evidence/evidence-pack-${String(r.run_id || "run")}.pdf` },
      ],
    },
  };
}

function downloadEvidencePackJson(pack, runId) {
  const blob = new Blob([JSON.stringify(pack, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `evidence-pack-${runId}.json`;
  a.click();
  URL.revokeObjectURL(url);
}

function generateEvidencePackPdf(pack, runId) {
  const safeJson = escapeHtml(JSON.stringify(pack, null, 2));
  const html = `
<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <title>Evidence Pack ${escapeHtml(runId)}</title>
    <style>
      body { font-family: Arial, sans-serif; margin: 24px; color: #0f172a; }
      h1 { font-size: 20px; margin-bottom: 8px; }
      h2 { font-size: 14px; margin-top: 20px; margin-bottom: 8px; }
      .meta { font-size: 12px; color: #334155; margin-bottom: 16px; }
      pre { background: #f8fafc; border: 1px solid #cbd5e1; border-radius: 8px; padding: 12px; font-size: 11px; white-space: pre-wrap; word-break: break-word; }
    </style>
  </head>
  <body>
    <h1>Synthetix Evidence Pack</h1>
    <div class="meta">Run: ${escapeHtml(runId)} | Generated: ${escapeHtml(new Date().toISOString())}</div>
    <h2>Client Readout</h2>
    <pre>${escapeHtml(JSON.stringify(pack.client_readout || {}, null, 2))}</pre>
    <h2>Controls, Traceability, Tests, Security</h2>
    <pre>${escapeHtml(JSON.stringify({
      controls_and_compliance: pack.controls_and_compliance || {},
      traceability_matrix: pack.traceability_matrix || {},
      test_evidence: pack.test_evidence || {},
      security_gates: pack.security_gates || {},
      approvals_and_exceptions: pack.approvals_and_exceptions || {},
      final_gate_summary: pack.final_gate_summary || {},
    }, null, 2))}</pre>
    <h2>Full JSON</h2>
    <pre>${safeJson}</pre>
  </body>
</html>`;
  const win = window.open("", "_blank", "noopener,noreferrer");
  if (!win) {
    alert("Popup blocked. Allow popups to generate PDF.");
    return;
  }
  win.document.open();
  win.document.write(html);
  win.document.close();
  win.focus();
  setTimeout(() => {
    win.print();
  }, 150);
}

async function exportEvidencePack(runId) {
  const data = await api(`/api/runs/${encodeURIComponent(runId)}`, null);
  const runs = Array.isArray(state.dashboardRuns) ? state.dashboardRuns : [];
  const currentRun = data.run || {};
  const baselineMeta = runs
    .filter((r) => String(r.run_id || "") !== String(runId) && String(r.status || "").toLowerCase() !== "running")
    .sort((a, b) => asMs(b.updated_at) - asMs(a.updated_at))[0] || null;
  const baselineRun = baselineMeta ? (runDetail(String(baselineMeta.run_id || "")) || baselineMeta) : null;
  const pack = buildEvidencePackFragment(currentRun, baselineRun);
  downloadEvidencePackJson(pack, runId);
}

function buildDiscoverBaselineReport() {
  const integration = getIntegrationContext();
  const data = _discoverData();
  const analyst = (state.discoverAnalystBrief?.data && typeof state.discoverAnalystBrief.data === "object")
    ? state.discoverAnalystBrief.data
    : {};
  const repo = (state.discoverGithubTree?.repo && typeof state.discoverGithubTree.repo === "object")
    ? state.discoverGithubTree.repo
    : {};
  const tree = (state.discoverGithubTree?.tree && typeof state.discoverGithubTree.tree === "object")
    ? state.discoverGithubTree.tree
    : {};
  return {
    generated_at: new Date().toISOString(),
    project_state: {
      mode: integration.project_state_mode,
      detected: integration.project_state_detected,
      confidence: integration.project_state_confidence,
      reason: integration.project_state_reason,
      sample_dataset_enabled: !!integration.sample_dataset_enabled,
    },
    integrations: {
      brownfield: integration.brownfield || {},
      greenfield: integration.greenfield || {},
    },
    scan_scope: integration.scan_scope || {},
    domain_pack: {
      selection: integration.domain_pack_selection || "auto",
      domain_pack_id: integration.domain_pack_id || "",
      jurisdiction: integration.jurisdiction || "",
      data_classification: Array.isArray(integration.data_classification) ? integration.data_classification : [],
      custom_domain_pack: (integration.custom_domain_pack && typeof integration.custom_domain_pack === "object")
        ? integration.custom_domain_pack
        : null,
    },
    repo_snapshot: {
      owner: repo.owner || "",
      repository: repo.repository || "",
      default_branch: repo.default_branch || "",
      total_entries: Number(tree.total_entries || 0),
      folders: Number(tree.folders || 0),
      files: Number(tree.files || 0),
      source: String(tree.source || ""),
    },
    analyst_brief: analyst.analyst_brief || {},
    analyst_aas: {
      thread_id: String(analyst.thread_id || analyst.aas?.thread_id || ""),
      assistant_summary: String(analyst.assistant_summary || analyst.aas?.assistant_summary || ""),
      requirements_pack: (analyst.requirements_pack && typeof analyst.requirements_pack === "object")
        ? analyst.requirements_pack
        : ((analyst.aas?.requirements_pack && typeof analyst.aas.requirements_pack === "object") ? analyst.aas.requirements_pack : {}),
      quality_gates: Array.isArray(analyst.quality_gates)
        ? analyst.quality_gates
        : (Array.isArray(analyst.aas?.quality_gates) ? analyst.aas.quality_gates : []),
    },
    metrics: {
      graph_nodes: Number((data.nodes || []).length),
      graph_edges: Number((data.edges || []).length),
      convention_rules: Number((data.rules || []).length),
      findings: Number((data.findings || []).length),
      backlog_items: Number((data.backlog || []).length),
      issue_preview_count: Number((state.discoverLinearIssues?.issues || []).length),
    },
    artifacts: {
      system_context_model_preview: {
        nodes: data.nodes || [],
        edges: data.edges || [],
      },
      convention_profile_preview: {
        rules: data.rules || [],
      },
      health_assessment_preview: {
        findings: data.findings || [],
        backlog: data.backlog || [],
      },
    },
  };
}

function exportDiscoverBaselineReport() {
  const report = buildDiscoverBaselineReport();
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const blob = new Blob([JSON.stringify(report, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `discover-baseline-report-${stamp}.json`;
  a.click();
  URL.revokeObjectURL(url);
}

function renderVerifyTabButtons() {
  if (!el.verifyTabButtons) return;
  const selected = String(state.verify.selectedTab || "summary");
  el.verifyTabButtons.querySelectorAll("[data-verify-tab]").forEach((btn) => {
    const tab = String(btn.getAttribute("data-verify-tab") || "");
    const active = tab === selected;
    btn.classList.toggle("btn-dark", active);
    btn.classList.toggle("btn-light", !active);
  });
}

function _verifyRunCandidates() {
  const runs = Array.isArray(state.dashboardRuns) ? state.dashboardRuns.slice() : [];
  return runs
    .filter((r) => {
      const s = String(r?.status || "").toLowerCase();
      return s === "completed" || s === "failed" || s === "aborted" || s === "waiting_approval";
    })
    .sort((a, b) => asMs(b.updated_at) - asMs(a.updated_at));
}

function _verifySelectedRun() {
  const candidates = _verifyRunCandidates();
  if (!candidates.length) return null;
  const selected = String(state.verify.selectedRunId || state.currentRunId || "").trim();
  const match = selected ? candidates.find((r) => String(r.run_id || "") === selected) : null;
  const chosen = match || candidates[0];
  state.verify.selectedRunId = String(chosen.run_id || "");
  if (el.verifyRunSelect && String(el.verifyRunSelect.value || "") !== state.verify.selectedRunId) {
    el.verifyRunSelect.value = state.verify.selectedRunId;
  }
  return runDetail(state.verify.selectedRunId) || chosen;
}

async function ensureVerifyRunDetail(runId) {
  const id = String(runId || "").trim();
  if (!id) return;
  if (runDetail(id)) return;
  if (state.verify.loadingRunId === id) return;
  state.verify.loadingRunId = id;
  try {
    const data = await api(`/api/runs/${encodeURIComponent(id)}`, null);
    if (data?.run?.run_id) {
      state.dashboardRunDetails[id] = data.run;
      if (state.currentRun?.run_id === id) state.currentRun = data.run;
    }
  } catch (_err) {
    // keep verify view usable on summary metadata only
  } finally {
    state.verify.loadingRunId = "";
    renderVerifyPanels();
  }
}

function _verifyBaselineRun(selectedRunId) {
  const candidates = _verifyRunCandidates().filter((r) => String(r.run_id || "") !== String(selectedRunId || ""));
  if (!candidates.length) return null;
  const baselineMeta = candidates[0];
  return runDetail(String(baselineMeta.run_id || "")) || baselineMeta;
}

function _renderVerifySummaryTab(pack) {
  const readout = (pack.client_readout && typeof pack.client_readout === "object") ? pack.client_readout : {};
  const rr = (readout.release_readiness && typeof readout.release_readiness === "object") ? readout.release_readiness : {};
  const whatChanged = Array.isArray(readout.what_changed) ? readout.what_changed : [];
  const risk = (readout.risk_posture && typeof readout.risk_posture === "object") ? readout.risk_posture : {};
  return `
    <div class="grid gap-3 lg:grid-cols-2">
      <div class="rounded-lg border border-slate-300 bg-slate-50 p-2">
        <p class="text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-700">Release Summary</p>
        <p class="mt-1 text-sm font-semibold text-slate-900">${escapeHtml(String(rr.status || "UNKNOWN"))}</p>
        <p class="mt-1 text-xs text-slate-700">${escapeHtml(String(rr.summary || ""))}</p>
      </div>
      <div class="rounded-lg border border-slate-300 bg-slate-50 p-2">
        <p class="text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-700">Risk Posture</p>
        <p class="mt-1 text-sm font-semibold text-slate-900">${escapeHtml(String(risk.overall || "n/a"))}</p>
        <ul class="mt-1 list-disc pl-4 text-xs text-slate-700">
          ${(Array.isArray(risk.notes) ? risk.notes : []).slice(0, 4).map((x) => `<li>${escapeHtml(String(x || ""))}</li>`).join("") || "<li>No risk notes.</li>"}
        </ul>
      </div>
    </div>
    <div class="mt-3 rounded-lg border border-slate-300 bg-white p-2">
      <p class="text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-700">What Changed Since Last Baseline</p>
      <ul class="mt-1 list-disc pl-4 text-xs text-slate-700">
        ${whatChanged.slice(0, 6).map((x) => `<li>${escapeHtml(String(x || ""))}</li>`).join("") || "<li>No change summary available.</li>"}
      </ul>
    </div>
  `;
}

function _renderVerifyControlsTab(pack) {
  const rows = Array.isArray(pack?.controls_and_compliance?.controls_triggered)
    ? pack.controls_and_compliance.controls_triggered
    : [];
  if (!rows.length) return "<p class='text-slate-700'>No controls captured for this run.</p>";
  return rows.map((row) => `
    <div class="mb-2 rounded-lg border border-slate-300 bg-slate-50 p-2">
      <p class="text-sm font-semibold text-slate-900">${escapeHtml(String(row.control_id || row.framework || "Control"))} · ${escapeHtml(String(row.status || "UNKNOWN"))}</p>
      <p class="text-xs text-slate-700">${escapeHtml(String(row.objective || ""))}</p>
    </div>
  `).join("");
}

function _renderVerifyTraceabilityTab(pack) {
  const links = Array.isArray(pack?.traceability_matrix?.links) ? pack.traceability_matrix.links : [];
  if (!links.length) return "<p class='text-slate-700'>No traceability links found.</p>";
  return `
    <div class="space-y-1">
      ${links.slice(0, 80).map((l) => `<div class="rounded border border-slate-300 bg-slate-50 px-2 py-1">${escapeHtml(String(l.from || ""))} → ${escapeHtml(String(l.to || ""))} <span class="text-slate-500">(${escapeHtml(String(l.type || ""))})</span></div>`).join("")}
    </div>
  `;
}

function _renderVerifyTestsTab(pack) {
  const summary = (pack?.test_evidence?.summary && typeof pack.test_evidence.summary === "object") ? pack.test_evidence.summary : {};
  const failed = Array.isArray(pack?.test_evidence?.failed_scenarios) ? pack.test_evidence.failed_scenarios : [];
  return `
    <div class="rounded-lg border border-slate-300 bg-slate-50 p-2">
      <p class="text-sm font-semibold text-slate-900">Status: ${escapeHtml(String(summary.status || "UNKNOWN"))}</p>
      <p class="text-xs text-slate-700">Overall pass rate: ${escapeHtml(String(summary.overall_pass_rate ?? "n/a"))}</p>
      <p class="text-xs text-slate-700">P0 automated: ${escapeHtml(String(summary.p0_automated_percent ?? "n/a"))}%</p>
    </div>
    <div class="mt-2 space-y-1">
      ${(failed.length ? failed : [{ scenario_id: "none", reason: "No failing scenarios." }]).map((f) => `
        <div class="rounded border border-slate-300 bg-white px-2 py-1">
          <strong>${escapeHtml(String(f.scenario_id || ""))}</strong>: ${escapeHtml(String(f.reason || ""))}
        </div>
      `).join("")}
    </div>
  `;
}

function _renderVerifySecurityTab(pack) {
  const security = (pack?.security_gates && typeof pack.security_gates === "object") ? pack.security_gates : {};
  const scans = Array.isArray(security.scans) ? security.scans : [];
  const notes = Array.isArray(security.notes) ? security.notes : [];
  return `
    <div class="rounded-lg border border-slate-300 bg-slate-50 p-2">
      <p class="text-sm font-semibold text-slate-900">Security gate status: ${escapeHtml(String(security.status || "UNKNOWN"))}</p>
    </div>
    <div class="mt-2">
      <p class="text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-700">Scans</p>
      <div class="mt-1 space-y-1">
        ${(scans.length ? scans : [{ type: "n/a", status: "No scan entries." }]).map((s) => `
          <div class="rounded border border-slate-300 bg-white px-2 py-1">${escapeHtml(String(s.type || ""))} · ${escapeHtml(String(s.status || ""))}</div>
        `).join("")}
      </div>
      <ul class="mt-2 list-disc pl-4 text-xs text-slate-700">
        ${(notes.length ? notes : ["No security notes."]).map((n) => `<li>${escapeHtml(String(n))}</li>`).join("")}
      </ul>
    </div>
  `;
}

function _renderVerifyApprovalsTab(pack) {
  const approvals = Array.isArray(pack?.approvals_and_exceptions?.approvals_required)
    ? pack.approvals_and_exceptions.approvals_required
    : [];
  const exceptions = Array.isArray(pack?.approvals_and_exceptions?.exceptions)
    ? pack.approvals_and_exceptions.exceptions
    : [];
  return `
    <div>
      <p class="text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-700">Approvals</p>
      <div class="mt-1 space-y-1">
        ${(approvals.length ? approvals : [{ message: "No pending approvals." }]).map((a, idx) => `
          <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1">
            <strong>${escapeHtml(String(a.approval_id || a.id || `APR-${idx + 1}`))}</strong> · ${escapeHtml(String(a.role || "Reviewer"))} · ${escapeHtml(String(a.status || "PENDING"))}
          </div>
        `).join("")}
      </div>
      <p class="mt-3 text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-700">Exceptions</p>
      <div class="mt-1 space-y-1">
        ${(exceptions.length ? exceptions : [{ policy: "none", status: "No exceptions." }]).map((e) => `
          <div class="rounded border border-slate-300 bg-white px-2 py-1">${escapeHtml(String(e.policy || e.exception_id || "Exception"))} · ${escapeHtml(String(e.status || ""))}</div>
        `).join("")}
      </div>
    </div>
  `;
}

function _renderVerifyExportsTab(pack, runId) {
  const exports = Array.isArray(pack?.exports?.available) ? pack.exports.available : [];
  return `
    <div class="rounded-lg border border-slate-300 bg-slate-50 p-2">
      <p class="text-xs text-slate-700">Use one-click exports above to generate audit artifacts for run <strong>${escapeHtml(runId)}</strong>.</p>
    </div>
    <div class="mt-2 space-y-1">
      ${(exports.length ? exports : [{ type: "json", path: "n/a" }]).map((x) => `
        <div class="rounded border border-slate-300 bg-white px-2 py-1">${escapeHtml(String(x.type || "").toUpperCase())} · ${escapeHtml(String(x.path || ""))}</div>
      `).join("")}
    </div>
  `;
}

function renderVerifyPanels() {
  if (!el.verifyTabContent || !el.verifyRunSelect) return;
  const candidates = _verifyRunCandidates();
  if (!candidates.length) {
    el.verifyRunSelect.innerHTML = "<option value=''>No runs available</option>";
    if (el.verifyReleaseReadiness) el.verifyReleaseReadiness.textContent = "Unknown";
    if (el.verifyApprovalsPending) el.verifyApprovalsPending.textContent = "0";
    if (el.verifyLastUpdated) el.verifyLastUpdated.textContent = "n/a";
    if (el.verifyBaselineDiff) el.verifyBaselineDiff.innerHTML = "No baseline comparison available yet.";
    el.verifyTabContent.innerHTML = "<p class='text-slate-700'>No verification records yet. Run a pipeline to generate evidence.</p>";
    renderVerifyTabButtons();
    return;
  }

  const selectedId = String(state.verify.selectedRunId || state.currentRunId || "");
  el.verifyRunSelect.innerHTML = candidates
    .map((r) => `<option value="${escapeHtml(String(r.run_id || ""))}" ${String(r.run_id || "") === selectedId ? "selected" : ""}>${escapeHtml(String(r.run_id || ""))} · ${escapeHtml(String(r.status || "").toUpperCase())}</option>`)
    .join("");

  const selectedRun = _verifySelectedRun();
  if (!selectedRun) return;
  const selectedRunId = String(selectedRun.run_id || "");
  const selectedPipeline = _runPipelineState(selectedRun);
  if ((!selectedPipeline || !Object.keys(selectedPipeline).length) && selectedRunId) {
    ensureVerifyRunDetail(selectedRunId).catch(() => {});
  }
  const baselineRun = _verifyBaselineRun(String(selectedRun.run_id || ""));
  const pack = buildEvidencePackFragment(selectedRun, baselineRun);
  state.verify.currentPack = pack;

  const readiness = String(pack?.client_readout?.release_readiness?.status || "UNKNOWN").toUpperCase();
  const readinessClass = readiness === "PASS" ? "text-emerald-700" : "text-rose-700";
  if (el.verifyReleaseReadiness) {
    el.verifyReleaseReadiness.textContent = readiness;
    el.verifyReleaseReadiness.classList.remove("text-emerald-700", "text-rose-700", "text-slate-900");
    el.verifyReleaseReadiness.classList.add(readinessClass);
  }
  const pendingApprovals = Array.isArray(pack?.client_readout?.release_readiness?.pending_approvals)
    ? pack.client_readout.release_readiness.pending_approvals.length
    : 0;
  if (el.verifyApprovalsPending) el.verifyApprovalsPending.textContent = String(pendingApprovals);
  if (el.verifyLastUpdated) el.verifyLastUpdated.textContent = String(selectedRun.updated_at || selectedRun.created_at || "n/a").replace("T", " ").slice(0, 19);
  if (el.verifyHeaderSubtitle) {
    el.verifyHeaderSubtitle.textContent = `Selected run ${String(selectedRun.run_id || "")} · ${String(selectedRun.status || "").toUpperCase()} · governance=${_runPipelineState(selectedRun).strict_security_mode ? "Strict" : "Standard"}`;
  }

  const topo = pack?.engineering_detail?.system_map_delta || {};
  const topoChanges = Array.isArray(topo.topology_changes) ? topo.topology_changes : [];
  const contractChanges = Array.isArray(topo.contract_changes) ? topo.contract_changes : [];
  if (el.verifyBaselineDiff) {
    el.verifyBaselineDiff.innerHTML = `
      <p class="text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-700">What changed since last baseline</p>
      <p class="mt-1 text-xs text-slate-700">Topology deltas: ${escapeHtml(String(topoChanges.length))} | Contract deltas: ${escapeHtml(String(contractChanges.length))}</p>
      <ul class="mt-1 list-disc pl-4 text-xs text-slate-700">
        ${topoChanges.slice(0, 3).map((x) => `<li>${escapeHtml(String(x || ""))}</li>`).join("")}
        ${contractChanges.slice(0, 3).map((x) => `<li>${escapeHtml(String(x || ""))}</li>`).join("")}
      </ul>
    `;
  }

  const tab = String(state.verify.selectedTab || "summary");
  if (tab === "controls") el.verifyTabContent.innerHTML = _renderVerifyControlsTab(pack);
  else if (tab === "traceability") el.verifyTabContent.innerHTML = _renderVerifyTraceabilityTab(pack);
  else if (tab === "tests") el.verifyTabContent.innerHTML = _renderVerifyTestsTab(pack);
  else if (tab === "security") el.verifyTabContent.innerHTML = _renderVerifySecurityTab(pack);
  else if (tab === "approvals") el.verifyTabContent.innerHTML = _renderVerifyApprovalsTab(pack);
  else if (tab === "exports") el.verifyTabContent.innerHTML = _renderVerifyExportsTab(pack, String(selectedRun.run_id || ""));
  else el.verifyTabContent.innerHTML = _renderVerifySummaryTab(pack);

  renderVerifyTabButtons();
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
  const domainPackSelection = String(integration.domain_pack_selection || "auto");
  const domainPackId = String(
    domainPackSelection === "custom"
      ? (integration.custom_domain_pack?.id || "custom")
      : (integration.domain_pack_id || domainPackSelection || "auto")
  );
  const cloudPromotionEnabled = Boolean(
    integration.cloud_promotion_enabled
      ?? (deploymentTarget === "cloud" && el.enableCloudPromotion?.checked)
      ?? false
  );

  const detailItems = [
    { label: "Team", value: teamName },
    { label: "Deployment", value: deploymentTarget === "cloud" ? "Cloud" : "Local Docker" },
    { label: "Cloud Promotion", value: cloudPromotionEnabled ? "Enabled" : "Disabled" },
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
  detailItems.push({ label: "Domain Pack", value: domainPackId });

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
  const graph = (scm.graph && typeof scm.graph === "object") ? scm.graph : {};
  const nodeList = Array.isArray(graph.nodes) ? graph.nodes : (Array.isArray(scm.nodes) ? scm.nodes : []);
  const edgeList = Array.isArray(graph.edges) ? graph.edges : (Array.isArray(scm.edges) ? scm.edges : []);
  const nodes = nodeList.length;
  const edges = edgeList.length;
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
  let evidence = status === "completed" ? "Ready for export" : (status === "failed" ? "Run failed; partial evidence" : "In progress");
  const githubExport = (p.github_export && typeof p.github_export === "object") ? p.github_export : {};
  const exportStatus = String(githubExport.status || "").toLowerCase();
  if (exportStatus === "exported") {
    evidence = `Exported to GitHub (${githubExport.base_path || "configured path"})`;
  } else if (exportStatus === "partial") {
    evidence = `GitHub export partial (${Number(githubExport.exported_files || 0)} files exported)`;
  } else if (exportStatus === "failed") {
    evidence = `GitHub export failed: ${String(githubExport.reason || "unknown error")}`;
  } else if (exportStatus === "skipped") {
    evidence = `GitHub export skipped: ${String(githubExport.reason || "disabled")}`;
  }

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

  if (el.contextOpsOutput) {
    const existing = String(el.contextOpsOutput.textContent || "").trim();
    if (!existing || existing === "-") {
      const scm = (p.system_context_model && typeof p.system_context_model === "object") ? p.system_context_model : {};
      const graph = (scm.graph && typeof scm.graph === "object") ? scm.graph : {};
      const nodes = Array.isArray(graph.nodes) ? graph.nodes.length : (Array.isArray(scm.nodes) ? scm.nodes.length : 0);
      const edges = Array.isArray(graph.edges) ? graph.edges.length : (Array.isArray(scm.edges) ? scm.edges.length : 0);
      const cp = (p.convention_profile && typeof p.convention_profile === "object") ? p.convention_profile : {};
      const rules = Array.isArray(cp.rules) ? cp.rules.length : 0;
      const findings = Array.isArray(p.remediation_backlog) ? p.remediation_backlog.length : 0;
      const statusText = String(p.context_layer_status || (p.sil_ready ? "ready" : "pending")).toUpperCase();
      el.contextOpsOutput.textContent = [
        `SIL status: ${statusText}`,
        `Context bundle: ${version} @ ${commit}`,
        `Graph nodes: ${nodes} | edges: ${edges} | CP rules: ${rules} | backlog: ${findings}`,
        "Use Impact Forecast or Drift Scan to generate deeper analysis output here.",
      ].join("\n");
    }
  }
}

function renderImpactDiff() {
  if (!el.impactDiffContent) return;
  const run = state.currentRun;
  const p = run?.pipeline_state || {};
  const scm = (p.system_context_model && typeof p.system_context_model === "object") ? p.system_context_model : {};
  const graph = (scm.graph && typeof scm.graph === "object") ? scm.graph : {};
  const nodes = Array.isArray(graph.nodes) ? graph.nodes : (Array.isArray(scm.nodes) ? scm.nodes : []);
  const edges = Array.isArray(graph.edges) ? graph.edges : (Array.isArray(scm.edges) ? scm.edges : []);
  const cp = (p.convention_profile && typeof p.convention_profile === "object") ? p.convention_profile : {};
  const rules = Array.isArray(cp.rules) ? cp.rules : [];
  const health = (p.health_assessment && typeof p.health_assessment === "object") ? p.health_assessment : {};
  const findings = Array.isArray(health.findings) ? health.findings : [];
  const backlog = Array.isArray(p.remediation_backlog) ? p.remediation_backlog : [];
  const validation = (p.context_contract_validation && typeof p.context_contract_validation === "object")
    ? p.context_contract_validation
    : {};
  const semIssues = Array.isArray(validation.semantic_issues) ? validation.semantic_issues : [];
  const schemaIssues = Array.isArray(validation.schema_issues) ? validation.schema_issues : [];
  const delta = (p.context_vault_ref && typeof p.context_vault_ref === "object" && p.context_vault_ref.delta && typeof p.context_vault_ref.delta === "object")
    ? p.context_vault_ref.delta
    : {};
  const testerResult = latestResultByStage(run, 6);
  const retryPlan = (p.retry_plan && typeof p.retry_plan === "object") ? p.retry_plan : {};

  const tab = String(state.impactDiffTab || "topology");
  let html = "";
  if (tab === "topology") {
    const samples = edges.slice(0, 12).map((e) => `<li>${escapeHtml(e.from || "")} → ${escapeHtml(e.to || "")} (${escapeHtml(e.type || "")})</li>`).join("");
    html = `
      <p><strong>Topology summary:</strong> nodes=${nodes.length}, edges=${edges.length}, delta=${escapeHtml(delta.status || "n/a")}</p>
      <p class="mt-1 text-slate-700">This tab highlights dependency and boundary movement inferred from SCM.</p>
      <ul class="mt-2 list-disc pl-4">${samples || "<li>No topology edges available.</li>"}</ul>
    `;
  } else if (tab === "contract") {
    const ruleRows = rules.slice(0, 8).map((r) => `<li>${escapeHtml(r.category || "rule")} · ${escapeHtml(r.title || r.statement || r.id || "")}</li>`).join("");
    const issues = semIssues.concat(schemaIssues).slice(0, 8).map((x) => `<li>${escapeHtml(String(x))}</li>`).join("");
    html = `
      <p><strong>Contract summary:</strong> convention rules=${rules.length}, semantic issues=${semIssues.length}, schema issues=${schemaIssues.length}</p>
      <div class="mt-2 grid gap-2 sm:grid-cols-2">
        <div><p class="font-semibold text-slate-900">Key convention rules</p><ul class="mt-1 list-disc pl-4">${ruleRows || "<li>No convention rules found.</li>"}</ul></div>
        <div><p class="font-semibold text-slate-900">Validation issues</p><ul class="mt-1 list-disc pl-4">${issues || "<li>No contract validation issues.</li>"}</ul></div>
      </div>
    `;
  } else if (tab === "data") {
    const dataEdges = edges.filter((e) => ["reads", "writes"].includes(String(e.type || "").toLowerCase())).slice(0, 12);
    const dataRows = dataEdges.map((e) => `<li>${escapeHtml(e.from || "")} ${escapeHtml(e.type || "")} ${escapeHtml(e.to || "")}</li>`).join("");
    const dbNodes = nodes.filter((n) => String(n.type || "").toLowerCase().includes("database") || String(n.type || "").toLowerCase().includes("table"));
    html = `
      <p><strong>Data summary:</strong> database/table nodes=${dbNodes.length}, read/write edges=${dataEdges.length}</p>
      <p class="mt-1 text-slate-700">Use this view to assess schema/flow impact and migration risk.</p>
      <ul class="mt-2 list-disc pl-4">${dataRows || "<li>No read/write edges detected.</li>"}</ul>
    `;
  } else {
    const critical = Array.isArray(retryPlan.critical_failures) ? retryPlan.critical_failures : [];
    const planned = Array.isArray(retryPlan.planned_self_heal_actions) ? retryPlan.planned_self_heal_actions : [];
    html = `
      <p><strong>Risk/quality summary:</strong> findings=${findings.length}, backlog=${backlog.length}, run status=${escapeHtml(run?.status || "idle")}</p>
      <p class="mt-1 text-slate-700">Tester: ${escapeHtml(testerResult?.summary || "No tester summary yet.")}</p>
      <div class="mt-2 grid gap-2 sm:grid-cols-2">
        <div><p class="font-semibold text-slate-900">Critical failures</p><ul class="mt-1 list-disc pl-4">${critical.slice(0, 6).map((x) => `<li>${escapeHtml(String(x))}</li>`).join("") || "<li>None</li>"}</ul></div>
        <div><p class="font-semibold text-slate-900">Planned mitigations</p><ul class="mt-1 list-disc pl-4">${planned.slice(0, 6).map((x) => `<li>${escapeHtml(String(x))}</li>`).join("") || "<li>No self-heal plan yet</li>"}</ul></div>
      </div>
    `;
  }
  el.impactDiffContent.innerHTML = html;
  document.querySelectorAll("[data-impact-tab]").forEach((btn) => {
    if (!(btn instanceof HTMLElement)) return;
    const active = String(btn.getAttribute("data-impact-tab") || "") === tab;
    btn.classList.toggle("btn-dark", active);
    btn.classList.toggle("btn-light", !active);
  });
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
  const report = buildAnalystReportV2(output);
  const perspective = currentPerspective();
  const clientMode = perspective === "executive";
  const showDeepEvidence = perspective === "engineering" || perspective === "security";
  const metadata = report.metadata || {};
  const project = metadata.project || {};
  const brief = report.decision_brief || {};
  const glance = brief.at_a_glance || {};
  const inventory = glance.inventory_summary || {};
  const strategy = brief.recommended_strategy || {};
  const delivery = report.delivery_spec || {};
  const backlog = Array.isArray(delivery.backlog?.items) ? delivery.backlog.items : [];
  const testing = delivery.testing_and_evidence || {};
  const openQuestions = Array.isArray(delivery.open_questions) ? delivery.open_questions : [];
  const appendix = report.appendix || {};
  const appendixRefs = (appendix.artifact_refs && typeof appendix.artifact_refs === "object")
    ? appendix.artifact_refs
    : {};
  const highVolume = appendix.high_volume_sections || {};
  const rawArtifacts = (output.raw_artifacts && typeof output.raw_artifacts === "object")
    ? output.raw_artifacts
    : {};
  const rawLegacyInventory = (rawArtifacts.legacy_inventory && typeof rawArtifacts.legacy_inventory === "object")
    ? rawArtifacts.legacy_inventory
    : null;
  const rawDependencyInventory = (rawArtifacts.dependency_inventory && typeof rawArtifacts.dependency_inventory === "object")
    ? rawArtifacts.dependency_inventory
    : null;
  const rawEventMap = (rawArtifacts.event_map && typeof rawArtifacts.event_map === "object")
    ? rawArtifacts.event_map
    : null;
  const rawSqlCatalog = (rawArtifacts.sql_catalog && typeof rawArtifacts.sql_catalog === "object")
    ? rawArtifacts.sql_catalog
    : null;
  const rawSqlMap = (rawArtifacts.sql_map && typeof rawArtifacts.sql_map === "object")
    ? rawArtifacts.sql_map
    : null;
  const rawProcedureSummary = (rawArtifacts.procedure_summary && typeof rawArtifacts.procedure_summary === "object")
    ? rawArtifacts.procedure_summary
    : null;
  const rawBusinessRules = (rawArtifacts.business_rule_catalog && typeof rawArtifacts.business_rule_catalog === "object")
    ? rawArtifacts.business_rule_catalog
    : null;
  const rawDetectorFindings = (rawArtifacts.detector_findings && typeof rawArtifacts.detector_findings === "object")
    ? rawArtifacts.detector_findings
    : null;
  const rawDeliveryConstitution = (rawArtifacts.delivery_constitution && typeof rawArtifacts.delivery_constitution === "object")
    ? rawArtifacts.delivery_constitution
    : null;
  const artifactIndex = (rawArtifacts.artifact_index && typeof rawArtifacts.artifact_index === "object")
    ? rawArtifacts.artifact_index
    : null;
  const gateBadge = (result) => {
    const r = String(result || "").toLowerCase();
    if (r === "pass") return "inline-flex rounded border border-emerald-300 bg-emerald-50 px-1.5 py-0.5 text-[10px] font-semibold text-emerald-900";
    if (r === "fail") return "inline-flex rounded border border-rose-300 bg-rose-50 px-1.5 py-0.5 text-[10px] font-semibold text-rose-900";
    return "inline-flex rounded border border-amber-300 bg-amber-50 px-1.5 py-0.5 text-[10px] font-semibold text-amber-900";
  };
  const revised = String(output.human_revised_document_markdown || "").trim();
  const blocking = Array.isArray(brief.decisions_required?.blocking) ? brief.decisions_required.blocking : [];
  const nonBlocking = Array.isArray(brief.decisions_required?.non_blocking) ? brief.decisions_required.non_blocking : [];
  const riskRows = Array.isArray(brief.top_risks) ? brief.top_risks : [];
  const strategyPhases = Array.isArray(strategy.phases) ? strategy.phases : [];
  const qualityGates = Array.isArray(testing.quality_gates) ? testing.quality_gates : [];
  const testMatrix = Array.isArray(testing.test_matrix) ? testing.test_matrix : [];
  const goldenFlows = Array.isArray(testing.golden_flows) ? testing.golden_flows : [];
  const evidenceOutputs = Array.isArray(testing.evidence_outputs) ? testing.evidence_outputs : [];
  const eventMap = Array.isArray(rawEventMap?.entries)
    ? rawEventMap.entries
    : (Array.isArray(highVolume.event_map) ? highVolume.event_map : []);
  const sqlCatalog = Array.isArray(rawSqlCatalog?.statements)
    ? rawSqlCatalog.statements
    : (Array.isArray(highVolume.sql_catalog) ? highVolume.sql_catalog : []);
  const sqlMapEntries = Array.isArray(rawSqlMap?.entries) ? rawSqlMap.entries : [];
  const procedureSummaries = Array.isArray(rawProcedureSummary?.procedures) ? rawProcedureSummary.procedures : [];
  const dependencies = Array.isArray(rawDependencyInventory?.dependencies)
    ? rawDependencyInventory.dependencies
    : (Array.isArray(highVolume.dependencies) ? highVolume.dependencies : []);
  const businessRules = Array.isArray(rawBusinessRules?.rules)
    ? rawBusinessRules.rules
    : (Array.isArray(highVolume.business_rules) ? highVolume.business_rules : []);
  const detectorFindings = Array.isArray(rawDetectorFindings?.findings)
    ? rawDetectorFindings.findings
    : [];
  const constitutionPrinciples = Array.isArray(rawDeliveryConstitution?.principles) ? rawDeliveryConstitution.principles : [];
  const specKitDecomposition = (report.spec_kit_decomposition && typeof report.spec_kit_decomposition === "object")
    ? report.spec_kit_decomposition
    : {};
  const discoverySpec = (specKitDecomposition.discovery_spec && typeof specKitDecomposition.discovery_spec === "object")
    ? specKitDecomposition.discovery_spec
    : {};
  const modernizationPlan = (specKitDecomposition.modernization_plan && typeof specKitDecomposition.modernization_plan === "object")
    ? specKitDecomposition.modernization_plan
    : {};
  const executableContracts = (specKitDecomposition.executable_contracts && typeof specKitDecomposition.executable_contracts === "object")
    ? specKitDecomposition.executable_contracts
    : {};
  const clarificationRows = Array.isArray(discoverySpec.needs_clarification) ? discoverySpec.needs_clarification : [];
  const refToRaw = {
    legacy_inventory_ref: "legacy_inventory",
    dependency_inventory_ref: "dependency_inventory",
    dependency_list_ref: "dependency_inventory",
    event_map_ref: "event_map",
    sql_catalog_ref: "sql_catalog",
    sql_map_ref: "sql_map",
    procedure_summary_ref: "procedure_summary",
    business_rules_ref: "business_rule_catalog",
    detector_findings_ref: "detector_findings",
    delivery_constitution_ref: "delivery_constitution",
    artifact_index_ref: "artifact_index",
  };
  const artifactRefRows = Object.entries(appendixRefs);
  const allowedTabs = clientMode
    ? ["spec", "evidence", "history"]
    : ["spec", "plan", "tasks", "evidence", "maps", "history"];
  if (!allowedTabs.includes(String(state.analyst?.selectedTab || ""))) {
    state.analyst.selectedTab = allowedTabs[0];
  }
  const currentTab = String(state.analyst?.selectedTab || allowedTabs[0]);

  const mapNodes = new Set();
  const mapEdges = [];
  eventMap.slice(0, 300).forEach((entry, idx) => {
    const container = String(entry?.container || entry?.form || "").trim();
    const symbol = String(entry?.handler?.symbol || entry?.event_handler || entry?.entry_id || `event:${idx + 1}`).trim();
    if (container) mapNodes.add(container);
    if (symbol) mapNodes.add(symbol);
    if (container && symbol) mapEdges.push({ from: container, to: symbol, type: "handles_event" });
    const sideEffects = (entry?.side_effects && typeof entry.side_effects === "object") ? entry.side_effects : {};
    const tables = Array.isArray(sideEffects.tables_or_files) ? sideEffects.tables_or_files : [];
    tables.slice(0, 8).forEach((table) => {
      const t = String(table || "").trim();
      if (!t) return;
      mapNodes.add(t);
      if (symbol) mapEdges.push({ from: symbol, to: t, type: "data_touch" });
    });
  });
  dependencies.slice(0, 160).forEach((dep) => {
    const name = String(dep?.name || dep || "").trim();
    if (!name) return;
    mapNodes.add(name);
    mapEdges.push({ from: "legacy-system", to: name, type: "uses_dependency" });
  });

  const conversationAudit = (output.conversation_audit && typeof output.conversation_audit === "object")
    ? output.conversation_audit
    : {};
  const auditChanges = Array.isArray(conversationAudit.changes) ? conversationAudit.changes : [];
  const migrationState = (output.markdown_migration && typeof output.markdown_migration === "object")
    ? output.markdown_migration
    : {};

  const tabButton = (id, label) => (
    `<button data-analyst-view-tab="${id}" class="btn-light rounded-md px-2 py-1 text-[11px] font-semibold">${label}</button>`
  );

  return `
    <div data-analyst-tab-root data-analyst-default="${escapeHtml(currentTab)}">
      <h5 class="text-sm font-semibold text-ink-950">Analyst report</h5>
      ${revised ? `<div class="mt-2 rounded border border-slate-300 bg-white p-2"><strong>Human revised document:</strong><pre class="mono mt-1 whitespace-pre-wrap text-[11px] text-slate-800">${escapeHtml(revised.slice(0, 2000))}${revised.length > 2000 ? "\n...[truncated]" : ""}</pre></div>` : ""}
      <div class="mt-2 flex flex-wrap items-center gap-2">
        ${tabButton("spec", "Spec")}
        ${!clientMode ? tabButton("plan", "Plan") : ""}
        ${!clientMode ? tabButton("tasks", "Tasks") : ""}
        ${tabButton("evidence", "Evidence")}
        ${!clientMode ? tabButton("maps", "Maps") : ""}
        ${tabButton("history", "History")}
      </div>
      <p data-analyst-view-status class="mt-1 text-[11px] text-slate-700"></p>

      <section data-analyst-view-panel="spec" class="mt-2 rounded border border-slate-300 bg-white p-2">
        <div class="rounded border border-slate-300 bg-white p-2 text-[11px] text-slate-900">
      <div><strong>Project:</strong> ${escapeHtml(String(project.name || "Untitled"))}</div>
      <div><strong>Objective:</strong> ${escapeHtml(String(project.objective || ""))}</div>
      <div><strong>Source -> Target:</strong> ${escapeHtml(String(metadata.context_reference?.repo || "n/a"))} @ ${escapeHtml(String(metadata.context_reference?.branch || "main"))}</div>
      <div><strong>SIL:</strong> SCM ${escapeHtml(String(metadata.context_reference?.scm_version || "1.0"))} / CP ${escapeHtml(String(metadata.context_reference?.cp_version || "1.0"))} / HA ${escapeHtml(String(metadata.context_reference?.ha_version || "1.0"))}</div>
      <div><strong>Generated:</strong> ${escapeHtml(String(metadata.generated_at || ""))}</div>
        </div>
        <div class="mt-2 rounded border border-slate-300 bg-white p-2">
          <div class="text-xs font-semibold text-slate-900">Decision brief</div>
      <table class="mt-2 w-full border-collapse text-[11px] text-slate-900">
        <tbody>
          <tr><td class="w-52 border border-slate-300 bg-slate-50 px-2 py-1 font-semibold">Readiness</td><td class="border border-slate-300 px-2 py-1">${escapeHtml(String(glance.readiness_score ?? "n/a"))}/100</td></tr>
          <tr><td class="border border-slate-300 bg-slate-50 px-2 py-1 font-semibold">Risk tier</td><td class="border border-slate-300 px-2 py-1">${escapeHtml(String(glance.risk_tier || "n/a"))}</td></tr>
          <tr><td class="border border-slate-300 bg-slate-50 px-2 py-1 font-semibold">Inventory</td><td class="border border-slate-300 px-2 py-1">${escapeHtml(`${String(inventory.projects ?? 0)} projects, ${String(inventory.forms ?? 0)} forms, ${String(inventory.dependencies ?? 0)} dependencies`)}</td></tr>
          <tr><td class="border border-slate-300 bg-slate-50 px-2 py-1 font-semibold">Top tables</td><td class="border border-slate-300 px-2 py-1">${escapeHtml((Array.isArray(inventory.tables_touched) ? inventory.tables_touched : []).join(", ") || "none")}</td></tr>
          <tr><td class="border border-slate-300 bg-slate-50 px-2 py-1 font-semibold">Headline</td><td class="border border-slate-300 px-2 py-1">${escapeHtml(String(glance.headline || ""))}</td></tr>
        </tbody>
      </table>
      <div class="mt-2 text-[11px]"><strong>Recommended strategy:</strong> ${escapeHtml(String(strategy.name || "Phased modernization"))}</div>
      <div class="text-[11px] text-slate-800">${escapeHtml(String(strategy.rationale || ""))}</div>
      <ul class="mt-1 list-disc pl-5 text-[11px]">${strategyPhases.map((phase) => `<li><strong>${escapeHtml(String(phase.id || ""))}</strong> ${escapeHtml(String(phase.title || ""))}: ${escapeHtml(String(phase.outcome || ""))}</li>`).join("") || "<li>No phase plan available</li>"}</ul>
      <div class="mt-2 text-[11px] font-semibold">Blocking decisions</div>
      <ul class="list-disc pl-5 text-[11px]">${blocking.map((row) => `<li><strong>${escapeHtml(String(row.id || "DEC"))}</strong> ${escapeHtml(String(row.question || ""))}<div class="text-slate-800">Recommendation: ${escapeHtml(String(row.default_recommendation || ""))}</div></li>`).join("") || "<li>None</li>"}</ul>
      <div class="mt-2 text-[11px] font-semibold">Non-blocking decisions</div>
      <ul class="list-disc pl-5 text-[11px]">${nonBlocking.map((row) => `<li><strong>${escapeHtml(String(row.id || "DEC"))}</strong> ${escapeHtml(String(row.question || ""))}</li>`).join("") || "<li>None</li>"}</ul>
      <div class="mt-2 text-[11px] font-semibold">Top risks</div>
      <ul class="list-disc pl-5 text-[11px]">${riskRows.map((row) => `<li><strong>${escapeHtml(String(row.id || "RISK"))}</strong> [${escapeHtml(String(row.severity || "medium").toUpperCase())}] ${escapeHtml(String(row.description || ""))}<div class="text-slate-800">Mitigation: ${escapeHtml(String(row.mitigation || ""))}</div></li>`).join("") || "<li>None</li>"}</ul>
      <div class="mt-2 text-[11px] font-semibold">Spec decomposition (Spec Kit style)</div>
      <div class="overflow-x-auto">
        <table class="mt-1 w-full border-collapse text-[11px] text-slate-900">
          <tbody>
            <tr><td class="border border-slate-300 bg-slate-50 px-2 py-1 font-semibold">Discovery spec</td><td class="border border-slate-300 px-2 py-1">projects=${escapeHtml(String(discoverySpec.inventory_counts?.projects ?? inventory.projects ?? 0))}, forms=${escapeHtml(String(discoverySpec.inventory_counts?.forms ?? inventory.forms ?? 0))}, SQL map=${escapeHtml(String(discoverySpec.inventory_counts?.sql_map_entries ?? sqlMapEntries.length))}</td></tr>
            <tr><td class="border border-slate-300 bg-slate-50 px-2 py-1 font-semibold">Modernization plan</td><td class="border border-slate-300 px-2 py-1">strategy=${escapeHtml(String(modernizationPlan.strategy || strategy.name || "Phased modernization"))}, backlog=${escapeHtml(String(modernizationPlan.backlog_items ?? backlog.length))}, blocking decisions=${escapeHtml(String(modernizationPlan.blocking_decisions ?? blocking.length))}</td></tr>
            <tr><td class="border border-slate-300 bg-slate-50 px-2 py-1 font-semibold">Executable contracts</td><td class="border border-slate-300 px-2 py-1">golden flows=${escapeHtml(String(executableContracts.golden_flow_count ?? goldenFlows.length))}, traceability links=${escapeHtml(String(executableContracts.traceability_links ?? 0))}, grounding=${escapeHtml(String(executableContracts.grounding_status || (qualityGates.some((g) => String(g.id) === "bdd_flow_grounding" && String(g.result) === "warn") ? "needs_improvement" : "grounded")))}</td></tr>
          </tbody>
        </table>
      </div>
      <div class="mt-2 text-[11px] font-semibold">Needs clarification (${clarificationRows.length})</div>
      <ul class="list-disc pl-5 text-[11px]">${clarificationRows.map((row) => `<li><strong>${escapeHtml(String(row.id || "Q"))}</strong> [${escapeHtml(String(row.severity || "medium").toUpperCase())}] ${escapeHtml(String(row.question || ""))} <span class="text-slate-700">(owner: ${escapeHtml(String(row.owner || "Unassigned"))})</span></li>`).join("") || "<li>No open clarification markers.</li>"}</ul>
        </div>
      </section>

      <section data-analyst-view-panel="plan" class="mt-2 rounded border border-slate-300 bg-white p-2 hidden">
        <div class="text-xs font-semibold text-slate-900">Delivery spec</div>
      <div class="mt-2 text-[11px] font-semibold">Backlog (${backlog.length})</div>
      <div class="overflow-x-auto">
        <table class="mt-1 w-full border-collapse text-[11px] text-slate-900">
          <thead>
            <tr>
              <th class="border border-slate-300 bg-slate-50 px-2 py-1 text-left">ID</th>
              <th class="border border-slate-300 bg-slate-50 px-2 py-1 text-left">Pri</th>
              <th class="border border-slate-300 bg-slate-50 px-2 py-1 text-left">Type</th>
              <th class="border border-slate-300 bg-slate-50 px-2 py-1 text-left">Outcome</th>
              <th class="border border-slate-300 bg-slate-50 px-2 py-1 text-left">Acceptance</th>
            </tr>
          </thead>
          <tbody>
            ${backlog.slice(0, 60).map((item) => `<tr>
              <td class="border border-slate-300 px-2 py-1">${escapeHtml(String(item.id || ""))}</td>
              <td class="border border-slate-300 px-2 py-1">${escapeHtml(String(item.priority || ""))}</td>
              <td class="border border-slate-300 px-2 py-1">${escapeHtml(String(item.type || ""))}</td>
              <td class="border border-slate-300 px-2 py-1">${escapeHtml(String(item.title || item.outcome || ""))}</td>
              <td class="border border-slate-300 px-2 py-1">${escapeHtml((Array.isArray(item.acceptance_criteria) ? item.acceptance_criteria.slice(0, 2).join(" / ") : "") || "n/a")}</td>
            </tr>`).join("") || `<tr><td class="border border-slate-300 px-2 py-1" colspan="5">No backlog items generated</td></tr>`}
          </tbody>
        </table>
      </div>
      <div class="mt-2 text-[11px] font-semibold">Golden flows (${goldenFlows.length})</div>
      <ul class="list-disc pl-5 text-[11px]">${goldenFlows.map((flow) => `<li><strong>${escapeHtml(String(flow.id || "GF"))}</strong> ${escapeHtml(String(flow.name || ""))} | entry=${escapeHtml(String(flow.entrypoint || ""))}</li>`).join("") || "<li>None</li>"}</ul>
      <div class="mt-2 text-[11px] font-semibold">Quality gates (${qualityGates.length})</div>
      <ul class="list-disc pl-5 text-[11px]">${qualityGates.map((gate) => `<li><span class="${gateBadge(gate.result)}">${escapeHtml(String(gate.result || "warn").toUpperCase())}</span> <strong class="ml-1">${escapeHtml(String(gate.id || "gate"))}</strong> ${escapeHtml(String(gate.description || ""))}</li>`).join("") || "<li>None</li>"}</ul>
      <div class="mt-2 text-[11px] font-semibold">Test matrix (${testMatrix.length})</div>
      <ul class="list-disc pl-5 text-[11px]">${testMatrix.slice(0, 20).map((row) => `<li>${escapeHtml(String(row.requirement_id || ""))} | tests: ${escapeHtml((Array.isArray(row.test_types) ? row.test_types.join(", ") : "") || "n/a")} | scenarios: ${escapeHtml((Array.isArray(row.scenario_ids) ? row.scenario_ids.join(", ") : "") || "n/a")}</li>`).join("") || "<li>None</li>"}</ul>
      <div class="mt-2 text-[11px] font-semibold">Evidence outputs (${evidenceOutputs.length})</div>
      <ul class="list-disc pl-5 text-[11px]">${evidenceOutputs.map((row) => `<li><strong>${escapeHtml(String(row.type || ""))}</strong> - ${escapeHtml(String(row.path_hint || ""))}</li>`).join("") || "<li>None</li>"}</ul>
      <div class="mt-2 text-[11px] font-semibold">Open questions (${openQuestions.length})</div>
      <ul class="list-disc pl-5 text-[11px]">${openQuestions.map((entry, idx) => {
        const q = normalizeOpenQuestionEntry(entry, idx);
        return `<li><strong>${escapeHtml(String(q.id || "Q"))}</strong> [${escapeHtml(String(q.severity || "medium").toUpperCase())}] ${escapeHtml(String(q.question || ""))} <span class="text-slate-700">(owner: ${escapeHtml(String(q.owner || "Unassigned"))})</span>${q.context ? `<div class="text-slate-800">${escapeHtml(String(q.context))}</div>` : ""}</li>`;
      }).join("") || "<li>None</li>"}</ul>
      </section>

      <section data-analyst-view-panel="tasks" class="mt-2 rounded border border-slate-300 bg-white p-2 hidden">
        <div class="text-xs font-semibold text-slate-900">Tasks</div>
        <p class="mt-1 text-[11px] text-slate-700">Execution checklist derived from plan backlog and grounded contracts.</p>
        <div class="mt-2 text-[11px] font-semibold">Execution checklist</div>
        <ul class="list-disc pl-5 text-[11px]">
          ${backlog.slice(0, 40).map((item) => `<li><strong>${escapeHtml(String(item.id || ""))}</strong> [${escapeHtml(String(item.priority || "P1"))}] ${escapeHtml(String(item.title || item.outcome || ""))}</li>`).join("") || "<li>No backlog tasks generated.</li>"}
        </ul>
        <div class="mt-2 grid gap-2 sm:grid-cols-4 text-[11px]">
          <div class="rounded border border-slate-300 bg-slate-50 p-2"><strong>Procedure summaries:</strong> ${procedureSummaries.length}</div>
          <div class="rounded border border-slate-300 bg-slate-50 p-2"><strong>SQL map entries:</strong> ${sqlMapEntries.length}</div>
          <div class="rounded border border-slate-300 bg-slate-50 p-2"><strong>Golden flows:</strong> ${goldenFlows.length}</div>
          <div class="rounded border border-slate-300 bg-slate-50 p-2"><strong>Traceability links:</strong> ${Array.isArray(delivery.traceability?.links) ? delivery.traceability.links.length : 0}</div>
        </div>
        <div class="mt-2 text-[11px] font-semibold">Contract tasks by SQL map</div>
        <div class="overflow-x-auto">
          <table class="mt-1 w-full border-collapse text-[11px] text-slate-900">
            <thead>
              <tr>
                <th class="border border-slate-300 bg-slate-50 px-2 py-1 text-left">Task</th>
                <th class="border border-slate-300 bg-slate-50 px-2 py-1 text-left">Form</th>
                <th class="border border-slate-300 bg-slate-50 px-2 py-1 text-left">Procedure</th>
                <th class="border border-slate-300 bg-slate-50 px-2 py-1 text-left">Operation</th>
                <th class="border border-slate-300 bg-slate-50 px-2 py-1 text-left">Tables</th>
                <th class="border border-slate-300 bg-slate-50 px-2 py-1 text-left">Risks</th>
              </tr>
            </thead>
            <tbody>
              ${sqlMapEntries.slice(0, 80).map((row, idx) => `<tr>
                <td class="border border-slate-300 px-2 py-1">TASK-${String(idx + 1).padStart(3, "0")}</td>
                <td class="border border-slate-300 px-2 py-1">${escapeHtml(String(row.form || "n/a"))}</td>
                <td class="border border-slate-300 px-2 py-1">${escapeHtml(String(row.procedure || "n/a"))}</td>
                <td class="border border-slate-300 px-2 py-1">${escapeHtml(String(row.operation || "unknown").toUpperCase())}</td>
                <td class="border border-slate-300 px-2 py-1">${escapeHtml((Array.isArray(row.tables) ? row.tables.join(", ") : "") || "n/a")}</td>
                <td class="border border-slate-300 px-2 py-1">${escapeHtml((Array.isArray(row.risk_flags) ? row.risk_flags.join(", ") : "") || "none")}</td>
              </tr>`).join("") || "<tr><td class='border border-slate-300 px-2 py-1' colspan='6'>No SQL map entries generated.</td></tr>"}
            </tbody>
          </table>
        </div>
        <details class="mt-2"><summary class="cursor-pointer text-[11px] font-semibold text-slate-900">Procedure summaries (${procedureSummaries.length})</summary><pre class="mono mt-1 max-h-56 overflow-auto whitespace-pre-wrap rounded border border-slate-300 bg-slate-50 p-2 text-[11px] text-slate-800">${escapeHtml(JSON.stringify(procedureSummaries.slice(0, 120), null, 2))}</pre></details>
        <div class="mt-2 text-[11px] font-semibold">Delivery constitution (${constitutionPrinciples.length})</div>
        <ul class="list-disc pl-5 text-[11px]">${constitutionPrinciples.map((line) => `<li>${escapeHtml(String(line || ""))}</li>`).join("") || "<li>No constitution principles available.</li>"}</ul>
      </section>

      <section data-analyst-view-panel="evidence" class="mt-2 rounded border border-slate-300 bg-white p-2 hidden">
        <div class="text-xs font-semibold text-slate-900">Evidence explorer</div>
        <p class="mt-1 text-[11px] text-slate-700">Evidence is source-of-truth. Brief and Plan content is derived from these artifacts.</p>
        <div class="mt-2 grid gap-2 md:grid-cols-2">
          ${artifactRefRows.map(([refKey, refValue]) => {
            const rawKey = refToRaw[refKey] || "";
            return `
              <div class="rounded border border-slate-300 bg-slate-50 p-2 text-[11px]">
                <div class="font-semibold text-slate-900">${escapeHtml(refKey)}</div>
                <div class="mono mt-1 break-all text-slate-700">${escapeHtml(String(refValue || ""))}</div>
                <div class="mt-2 flex flex-wrap gap-1">
                  <button data-artifact-copy-ref="${escapeHtml(String(refValue || ""))}" class="btn-light rounded-md px-2 py-1 text-[10px] font-semibold">Copy ref</button>
                  ${rawKey ? `<button data-artifact-download-key="${escapeHtml(rawKey)}" class="btn-dark rounded-md px-2 py-1 text-[10px] font-semibold">Download JSON</button>` : ""}
                </div>
              </div>
            `;
          }).join("") || "<div class='rounded border border-slate-300 bg-slate-50 p-2 text-[11px] text-slate-700'>No artifact refs available.</div>"}
        </div>

        ${showDeepEvidence ? `
        <div class="mt-2 overflow-x-auto">
          <table class="w-full border-collapse text-[11px] text-slate-900">
            <thead>
              <tr>
                <th class="border border-slate-300 bg-slate-50 px-2 py-1 text-left">Artifact</th>
                <th class="border border-slate-300 bg-slate-50 px-2 py-1 text-left">Rows</th>
                <th class="border border-slate-300 bg-slate-50 px-2 py-1 text-left">Primary use</th>
              </tr>
            </thead>
            <tbody>
              <tr><td class="border border-slate-300 px-2 py-1">Legacy inventory</td><td class="border border-slate-300 px-2 py-1">${Number(rawLegacyInventory?.projects?.length || 0)}</td><td class="border border-slate-300 px-2 py-1">Project/file/system inventory</td></tr>
              <tr><td class="border border-slate-300 px-2 py-1">Dependency inventory</td><td class="border border-slate-300 px-2 py-1">${dependencies.length}</td><td class="border border-slate-300 px-2 py-1">ActiveX/COM/DLL risk and replacement planning</td></tr>
              <tr><td class="border border-slate-300 px-2 py-1">Event map</td><td class="border border-slate-300 px-2 py-1">${eventMap.length}</td><td class="border border-slate-300 px-2 py-1">Entrypoints, calls, side effects</td></tr>
              <tr><td class="border border-slate-300 px-2 py-1">SQL catalog</td><td class="border border-slate-300 px-2 py-1">${sqlCatalog.length}</td><td class="border border-slate-300 px-2 py-1">Query contract and data touchpoints</td></tr>
              <tr><td class="border border-slate-300 px-2 py-1">SQL map</td><td class="border border-slate-300 px-2 py-1">${sqlMapEntries.length}</td><td class="border border-slate-300 px-2 py-1">Form/Procedure to query/table risk mapping</td></tr>
              <tr><td class="border border-slate-300 px-2 py-1">Procedure summaries</td><td class="border border-slate-300 px-2 py-1">${procedureSummaries.length}</td><td class="border border-slate-300 px-2 py-1">Step-wise behavior decomposition</td></tr>
              <tr><td class="border border-slate-300 px-2 py-1">Business rules</td><td class="border border-slate-300 px-2 py-1">${businessRules.length}</td><td class="border border-slate-300 px-2 py-1">Rule extraction + BDD grounding</td></tr>
              <tr><td class="border border-slate-300 px-2 py-1">Detector findings</td><td class="border border-slate-300 px-2 py-1">${detectorFindings.length}</td><td class="border border-slate-300 px-2 py-1">Modernization risk hotspots</td></tr>
              <tr><td class="border border-slate-300 px-2 py-1">Delivery constitution</td><td class="border border-slate-300 px-2 py-1">${constitutionPrinciples.length}</td><td class="border border-slate-300 px-2 py-1">Project non-negotiables propagated across phases</td></tr>
            </tbody>
          </table>
        </div>
        <details class="mt-2"><summary class="cursor-pointer text-[11px] font-semibold text-slate-900">Event map rows (${eventMap.length})</summary><pre class="mono mt-1 max-h-52 overflow-auto whitespace-pre-wrap rounded border border-slate-300 bg-slate-50 p-2 text-[11px] text-slate-800">${escapeHtml(JSON.stringify(eventMap.slice(0, 200), null, 2))}</pre></details>
        <details class="mt-2"><summary class="cursor-pointer text-[11px] font-semibold text-slate-900">SQL catalog rows (${sqlCatalog.length})</summary><pre class="mono mt-1 max-h-52 overflow-auto whitespace-pre-wrap rounded border border-slate-300 bg-slate-50 p-2 text-[11px] text-slate-800">${escapeHtml(JSON.stringify(sqlCatalog.slice(0, 240), null, 2))}</pre></details>
        <details class="mt-2"><summary class="cursor-pointer text-[11px] font-semibold text-slate-900">Detector findings (${detectorFindings.length})</summary><pre class="mono mt-1 max-h-52 overflow-auto whitespace-pre-wrap rounded border border-slate-300 bg-slate-50 p-2 text-[11px] text-slate-800">${escapeHtml(JSON.stringify(detectorFindings.slice(0, 240), null, 2))}</pre></details>
        ` : `<div class="mt-2 rounded border border-slate-300 bg-slate-50 p-2 text-[11px] text-slate-700">Detailed raw evidence is available in Engineering/Security perspectives.</div>`}
      </section>

      <section data-analyst-view-panel="maps" class="mt-2 rounded border border-slate-300 bg-white p-2 hidden">
        <div class="text-xs font-semibold text-slate-900">Maps</div>
        <p class="mt-1 text-[11px] text-slate-700">Topology is derived from event flows, data touchpoints, and dependency inventory.</p>
        <div class="mt-2 grid gap-2 sm:grid-cols-3 text-[11px]">
          <div class="rounded border border-slate-300 bg-slate-50 p-2"><strong>Nodes:</strong> ${mapNodes.size}</div>
          <div class="rounded border border-slate-300 bg-slate-50 p-2"><strong>Edges:</strong> ${mapEdges.length}</div>
          <div class="rounded border border-slate-300 bg-slate-50 p-2"><strong>Artifact index:</strong> ${Number(artifactIndex?.artifacts?.length || 0)}</div>
        </div>
        <div class="mt-2 overflow-x-auto">
          <table class="w-full border-collapse text-[11px] text-slate-900">
            <thead><tr><th class="border border-slate-300 bg-slate-50 px-2 py-1 text-left">From</th><th class="border border-slate-300 bg-slate-50 px-2 py-1 text-left">To</th><th class="border border-slate-300 bg-slate-50 px-2 py-1 text-left">Type</th></tr></thead>
            <tbody>
              ${mapEdges.slice(0, 120).map((edge) => `<tr><td class="border border-slate-300 px-2 py-1">${escapeHtml(String(edge.from || ""))}</td><td class="border border-slate-300 px-2 py-1">${escapeHtml(String(edge.to || ""))}</td><td class="border border-slate-300 px-2 py-1">${escapeHtml(String(edge.type || ""))}</td></tr>`).join("") || "<tr><td class='border border-slate-300 px-2 py-1' colspan='3'>No map edges detected.</td></tr>"}
            </tbody>
          </table>
        </div>
      </section>

      <section data-analyst-view-panel="history" class="mt-2 rounded border border-slate-300 bg-white p-2 hidden">
        <div class="text-xs font-semibold text-slate-900">History</div>
        <div class="mt-1 text-[11px] text-slate-700"><strong>Conversation thread:</strong> ${escapeHtml(String(conversationAudit.thread_id || "n/a"))}</div>
        <div class="mt-1 text-[11px] text-slate-700"><strong>Markdown migration:</strong> ${escapeHtml(String(migrationState.ok === false ? "failed" : (migrationState.ok === true ? "applied" : "not applied")))}</div>
        ${Array.isArray(migrationState.warnings) && migrationState.warnings.length ? `<ul class="mt-1 list-disc pl-5 text-[11px] text-amber-800">${migrationState.warnings.map((w) => `<li>${escapeHtml(String(w || ""))}</li>`).join("")}</ul>` : ""}
        <div class="mt-2 text-[11px] font-semibold text-slate-900">Decision and redline history (${auditChanges.length})</div>
        <ul class="mt-1 list-disc pl-5 text-[11px] text-slate-900">
          ${auditChanges.slice(-80).reverse().map((change) => `<li><strong>${escapeHtml(String(change.change_id || "change"))}</strong> ${escapeHtml(String(change.summary || ""))} <span class="text-slate-700">(${escapeHtml(String(change.timestamp || ""))})</span></li>`).join("") || "<li>No historical changes captured.</li>"}
        </ul>
      </section>
    </div>
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
  const artifactRoot = String(output.artifact_root || "").trim();
  return `
    <div><strong>Total LOC:</strong> ${Number(output.total_loc || 0).toLocaleString()}</div>
    <div><strong>Components:</strong> ${Number(output.total_components || 0)}</div>
    <div><strong>Files:</strong> ${Number(output.total_files || 0)}</div>
    ${artifactRoot ? `<div><strong>Local Artifact Path:</strong> <span class="mono">${escapeHtml(artifactRoot)}</span></div>` : ""}
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
  const personaReqPack = Number(agent.stage) === 1 ? String(persona.requirements_pack_profile || "").trim() : "";

  el.currentAgentPanel.innerHTML = `
    <div class="flex flex-wrap items-start justify-between gap-3 ${status === "running" ? "running-glow" : ""}">
      <div>
        <p class="text-xs uppercase tracking-[0.16em] text-slate-600">Current Agent</p>
        <h3 class="mt-1 text-xl font-semibold text-ink-950">Stage ${agent.stage}: ${agent.icon} ${agent.name}</h3>
        <p class="mt-1 text-sm text-slate-700">${agent.desc}</p>
        <p class="mt-1 text-xs text-slate-700"><strong>Persona:</strong> ${escapeHtml(persona.display_name || "Default")}</p>
        ${personaReqPack ? `<p class="mt-1 text-xs text-sky-800"><strong>Requirements Pack Profile:</strong> ${escapeHtml(personaReqPack)}</p>` : ""}
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
      renderCollaborationPanel();
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
    ${stage === 1 && run?.run_id ? `
      <div class="mt-2 rounded-lg border border-slate-300 bg-white p-2">
        <div class="flex flex-wrap items-center gap-2">
          <button data-analyst-export-summary class="btn-light rounded-md px-2 py-1 text-[11px] font-semibold">Export Summary</button>
          <button data-analyst-export-full class="btn-light rounded-md px-2 py-1 text-[11px] font-semibold">Export Full Evidence</button>
          <button data-analyst-upload-trigger class="btn-dark rounded-md px-2 py-1 text-[11px] font-semibold">Upload Modified</button>
          <input data-analyst-upload-file type="file" class="hidden" accept=".md,.txt,.json" />
        </div>
        <p data-analyst-doc-status class="mt-1 text-[11px] text-slate-700">Export a summary or full evidence document, or upload an updated version.</p>
      </div>
    ` : ""}
    <div class="mt-2 rounded-lg border border-slate-300 bg-slate-50 p-2 text-xs text-slate-800">${renderReadableOutput(stage, result?.output || {}, runUseCase)}</div>
    <pre class="log-window mono mt-2 h-[120px] overflow-auto rounded-lg border border-slate-300 p-3 text-[11px]">${escapeHtml(logs)}</pre>
  `;
  const openBtn = el.agentTabPanel.querySelector("[data-open-stage]");
  if (openBtn) openBtn.addEventListener("click", () => openStageModal(Number(openBtn.getAttribute("data-open-stage"))));
  if (stage === 1 && run?.run_id) {
    wireAnalystDocActions(el.agentTabPanel, run);
    wireAnalystViewTabs(el.agentTabPanel, run);
  }
  setTimeout(() => renderMermaidBlocks(el.agentTabPanel), 0);
}

function collabCacheKey(runId, stage) {
  return `${String(runId || "")}::${Number(stage || 0)}`;
}

function collabDraftKey(stage) {
  const runId = String(state.currentRun?.run_id || state.currentRunId || "");
  return collabCacheKey(runId, stage);
}

function currentCollabStage() {
  return Number(state.selectedStage || determineCurrentStage(state.currentRun) || 1);
}

function collabRecord(stage) {
  const runId = String(state.currentRun?.run_id || state.currentRunId || "");
  if (!runId) return null;
  const key = collabCacheKey(runId, stage);
  return state.collaboration.cache[key] || null;
}

function syncRunFromApiResponse(run) {
  if (!run || !run.run_id) return;
  state.currentRun = run;
  state.currentRunId = String(run.run_id);
  state.dashboardRunDetails[state.currentRunId] = run;
}

function invalidateCollaborationCache(runId) {
  const prefix = `${String(runId || "")}::`;
  Object.keys(state.collaboration.cache || {}).forEach((key) => {
    if (key.startsWith(prefix)) delete state.collaboration.cache[key];
  });
  Object.keys(state.collaboration.errorByKey || {}).forEach((key) => {
    if (key.startsWith(prefix)) delete state.collaboration.errorByKey[key];
  });
}

function parseJsonLoose(rawText) {
  const text = String(rawText || "").trim();
  if (!text) return "";
  try {
    return JSON.parse(text);
  } catch (_err) {
    return text;
  }
}

async function ensureCollaborationLoaded(stage, force = false) {
  const runId = String(state.currentRun?.run_id || state.currentRunId || "");
  if (!runId) return;
  const key = collabCacheKey(runId, stage);
  if (!force && state.collaboration.cache[key]) return;
  if (state.collaboration.loadingKey === key) return;
  state.collaboration.loadingKey = key;
  try {
    const data = await api(`/api/runs/${encodeURIComponent(runId)}/stages/${stage}/collaboration`, null);
    state.collaboration.cache[key] = data.collaboration || null;
    if (data.run?.run_id) syncRunFromApiResponse(data.run);
    delete state.collaboration.errorByKey[key];
  } catch (err) {
    state.collaboration.errorByKey[key] = String(err?.message || err || "Failed to load collaboration state.");
  } finally {
    state.collaboration.loadingKey = "";
    renderCollaborationPanel();
  }
}

function collabStatusBadge(status) {
  const value = String(status || "pending").toLowerCase();
  if (value === "applied") return "border-emerald-300 bg-emerald-50 text-emerald-800";
  if (value === "rejected") return "border-rose-300 bg-rose-50 text-rose-800";
  return "border-slate-300 bg-slate-100 text-slate-800";
}

function formatLlmFallbackReason(rawReason) {
  const reason = String(rawReason || "").trim();
  if (!reason || reason === "stage_not_enabled") return "";
  if (reason === "no_api_key") return "LLM key not configured";
  if (reason === "disabled") return "LLM responses are disabled for this chat";
  if (reason === "empty_response") return "LLM returned an empty response";
  if (reason.startsWith("credential_resolution_failed")) return "LLM credentials could not be resolved";
  if (reason.startsWith("llm_invoke_failed:")) {
    const detail = reason.slice("llm_invoke_failed:".length);
    if (detail.includes("insufficient_quota") || detail.includes("Error code: 429") || detail.includes("429")) {
      return "LLM quota exceeded; using deterministic artifact-based response";
    }
    return "LLM call failed; using deterministic artifact-based response";
  }
  return "Deterministic artifact-based fallback is active";
}

function renderCollabChatTab(stage, record) {
  const draft = String(state.collaboration.drafts[collabDraftKey(stage)] || "");
  const chat = Array.isArray(record?.chat) ? record.chat : [];
  const llmMeta = (record?.llm_chat && typeof record.llm_chat === "object") ? record.llm_chat : {};
  const llmUsed = !!llmMeta.used;
  const llmLabel = llmUsed
    ? `LLM response enabled (${String(llmMeta.provider || "").toUpperCase()} · ${escapeHtml(String(llmMeta.model || ""))})`
    : "Deterministic artifact-based response mode";
  const llmReason = String(llmMeta.reason || "").trim();
  const llmReasonDisplay = formatLlmFallbackReason(llmReason);
  const thread = chat.length
    ? chat.slice(-16).map((row) => {
      const role = String(row.role || "assistant");
      const cls = role === "user" ? "border-sky-300 bg-sky-50" : "border-slate-300 bg-slate-50";
      return `
        <div class="rounded-md border ${cls} p-2">
          <div class="mb-1 flex items-center justify-between gap-2">
            <span class="text-[10px] font-semibold uppercase tracking-[0.12em] text-slate-700">${escapeHtml(role)}</span>
            <span class="mono text-[10px] text-slate-600">${escapeHtml(String(row.created_at || "").replace("T", " ").slice(0, 19))}</span>
          </div>
          <div class="whitespace-pre-wrap text-[11px] text-slate-800">${escapeHtml(row.message || "")}</div>
        </div>
      `;
    }).join("")
    : "<p class='text-slate-700'>No collaboration messages yet for this stage.</p>";
  return `
    <div class="grid gap-3 lg:grid-cols-[1.4fr_1fr]">
      <div>
        <div class="mb-1 text-[11px] ${llmUsed ? "text-emerald-800" : "text-slate-700"}">
          ${llmLabel}${!llmUsed && llmReasonDisplay ? ` (${escapeHtml(llmReasonDisplay)})` : ""}
        </div>
        <div class="max-h-[270px] space-y-2 overflow-auto rounded-lg border border-slate-300 bg-white p-2">${thread}</div>
        <textarea id="collab-chat-input" rows="3" class="mt-2 w-full rounded-lg border border-slate-300 bg-white px-2 py-2 text-xs text-slate-900" placeholder="Ask the agent to explain, compare options, or suggest a targeted change...">${escapeHtml(draft)}</textarea>
        <div class="mt-2 flex flex-wrap items-center gap-3">
          <label class="inline-flex items-center gap-2 text-[11px] text-slate-800"><input id="collab-save-directive" type="checkbox" class="h-4 w-4 rounded border-slate-400 text-slate-900" />Save as directive</label>
          <label class="inline-flex items-center gap-2 text-[11px] text-slate-800"><input id="collab-create-proposal" type="checkbox" class="h-4 w-4 rounded border-slate-400 text-slate-900" />Create proposal diff</label>
          <button id="collab-chat-send" class="btn-dark rounded-md px-3 py-1.5 text-xs font-semibold">Send</button>
        </div>
      </div>
      <div class="rounded-lg border border-slate-300 bg-slate-50 p-2">
        <p class="text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-700">Guidance</p>
        <ul class="mt-1 list-disc pl-4 text-[11px] text-slate-700">
          <li>Use chat for explain, options, and constraints.</li>
          <li>Use directives for persistent "must/should not" constraints.</li>
          <li>Approve proposals before artifact outputs are changed.</li>
        </ul>
      </div>
    </div>
  `;
}

function renderCollabProposalsTab(stage, record) {
  const proposals = Array.isArray(record?.proposals) ? record.proposals : [];
  const rows = proposals.length
    ? proposals.slice().reverse().map((proposal) => {
      const patch = Array.isArray(proposal.patch) ? proposal.patch : [];
      const patchPreview = patch.slice(0, 5).map((op) => `${op.op || "?"} ${op.path || ""}`).join("\n");
      const pending = String(proposal.status || "").toLowerCase() === "pending";
      return `
        <div class="rounded-lg border border-slate-300 bg-white p-2">
          <div class="flex flex-wrap items-center justify-between gap-2">
            <p class="text-xs font-semibold text-slate-900">${escapeHtml(proposal.title || "Proposal")}</p>
            <span class="rounded border px-2 py-0.5 text-[10px] font-semibold ${collabStatusBadge(proposal.status)}">${escapeHtml(String(proposal.status || "pending").toUpperCase())}</span>
          </div>
          <p class="mt-1 text-[11px] text-slate-700">${escapeHtml(proposal.summary || "")}</p>
          <p class="mono mt-1 text-[10px] text-slate-600">${escapeHtml(String(proposal.id || ""))}</p>
          <pre class="mono mt-1 max-h-28 overflow-auto rounded border border-slate-300 bg-slate-50 p-2 text-[10px] text-slate-800">${escapeHtml(patchPreview || "(no patch ops)")}</pre>
          ${pending ? `
            <div class="mt-2 flex gap-2">
              <button data-collab-approve="${escapeHtml(proposal.id || "")}" class="btn-success rounded-md px-2 py-1 text-[11px] font-semibold">Approve & Apply</button>
              <button data-collab-reject="${escapeHtml(proposal.id || "")}" class="btn-danger rounded-md px-2 py-1 text-[11px] font-semibold">Reject</button>
            </div>
          ` : ""}
        </div>
      `;
    }).join("")
    : "<p class='text-slate-700'>No proposals yet. Send a chat message with 'Create proposal diff' enabled.</p>";

  return `
    <div class="grid gap-3 lg:grid-cols-[1.3fr_1fr]">
      <div class="max-h-[330px] space-y-2 overflow-auto rounded-lg border border-slate-300 bg-slate-50 p-2">${rows}</div>
      <div class="rounded-lg border border-slate-300 bg-white p-2">
        <p class="text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-700">Manual proposal</p>
        <input id="collab-proposal-title" class="mt-1 w-full rounded border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900" placeholder="Proposal title" />
        <input id="collab-proposal-summary" class="mt-1 w-full rounded border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900" placeholder="Summary" />
        <div class="mt-1 grid gap-1 sm:grid-cols-[90px_minmax(0,1fr)]">
          <select id="collab-proposal-op" class="rounded border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900">
            <option value="add">add</option>
            <option value="replace">replace</option>
            <option value="remove">remove</option>
          </select>
          <input id="collab-proposal-path" class="rounded border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900" placeholder="/path/to/field" />
        </div>
        <textarea id="collab-proposal-value" rows="4" class="mono mt-1 w-full rounded border border-slate-300 bg-slate-50 px-2 py-1.5 text-xs text-slate-900" placeholder='JSON value or plain text (ignored for remove)'></textarea>
        <button id="collab-proposal-create" class="btn-dark mt-2 rounded-md px-3 py-1.5 text-xs font-semibold">Create proposal</button>
      </div>
    </div>
  `;
}

function renderCollabEvidenceTab(record) {
  const evidence = Array.isArray(record?.evidence) ? record.evidence : [];
  if (!evidence.length) {
    return "<p class='text-slate-700'>No evidence pointers available yet for this stage.</p>";
  }
  return `
    <div class="space-y-2">
      ${evidence.map((ev) => `
        <div class="rounded-lg border border-slate-300 bg-slate-50 p-2">
          <div class="flex flex-wrap items-center justify-between gap-2">
            <span class="text-[11px] font-semibold text-slate-900">${escapeHtml(ev.label || ev.kind || "evidence")}</span>
            <span class="mono text-[10px] text-slate-600">confidence=${escapeHtml(String(ev.confidence ?? ""))}</span>
          </div>
          <div class="mt-1 break-all text-[11px] text-slate-700">${escapeHtml(ev.ref || "")}</div>
        </div>
      `).join("")}
    </div>
  `;
}

function renderCollabDecisionsTab(record) {
  const directives = Array.isArray(record?.directives) ? record.directives : [];
  const decisions = Array.isArray(record?.decisions) ? record.decisions : [];
  return `
    <div class="grid gap-3 lg:grid-cols-2">
      <div class="rounded-lg border border-slate-300 bg-white p-2">
        <p class="text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-700">Directives</p>
        <div class="mt-1 max-h-[260px] space-y-2 overflow-auto">
          ${directives.length ? directives.slice().reverse().map((row) => `
            <div class="rounded border border-slate-300 bg-slate-50 p-2">
              <p class="text-[11px] font-semibold text-slate-900">${escapeHtml(row.priority || "medium")} priority</p>
              <p class="mt-1 text-[11px] text-slate-700">${escapeHtml(row.text || "")}</p>
            </div>
          `).join("") : "<p class='text-slate-700'>No directives saved yet.</p>"}
        </div>
      </div>
      <div class="rounded-lg border border-slate-300 bg-white p-2">
        <p class="text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-700">Decisions</p>
        <div class="mt-1 max-h-[260px] space-y-2 overflow-auto">
          ${decisions.length ? decisions.slice().reverse().map((row) => `
            <div class="rounded border border-slate-300 bg-slate-50 p-2">
              <div class="flex items-center justify-between gap-2">
                <p class="text-[11px] font-semibold text-slate-900">${escapeHtml(String(row.decision || "").toUpperCase())}</p>
                <p class="mono text-[10px] text-slate-600">${escapeHtml(String(row.proposal_id || ""))}</p>
              </div>
              <p class="mt-1 text-[11px] text-slate-700">${escapeHtml(row.rationale || "No rationale provided.")}</p>
              ${Array.isArray(row.changed_paths) && row.changed_paths.length ? `<p class="mono mt-1 text-[10px] text-slate-600">${escapeHtml(row.changed_paths.join(", "))}</p>` : ""}
            </div>
          `).join("") : "<p class='text-slate-700'>No decisions recorded yet.</p>"}
        </div>
      </div>
    </div>
  `;
}

async function submitCollabChat(stage) {
  const safeStage = Number(stage || currentCollabStage() || 1);
  const input = document.getElementById("collab-chat-input");
  const saveDirective = document.getElementById("collab-save-directive");
  const createProposal = document.getElementById("collab-create-proposal");
  const message = String(input?.value || "").trim();
  if (!message) {
    alert("Enter a message first.");
    return;
  }
  const runId = String(state.currentRun?.run_id || state.currentRunId || "");
  if (!runId) return;
  try {
    const data = await api(`/api/runs/${encodeURIComponent(runId)}/stages/${safeStage}/collaboration/chat`, {
      message,
      save_as_directive: !!saveDirective?.checked,
      propose_change: !!createProposal?.checked,
      llm: {
        enabled: true,
        provider: String(el.provider?.value || ""),
        model: String(el.model?.value || ""),
        temperature: Number(el.temperature?.value || 0.2),
      },
    });
    const key = collabCacheKey(runId, safeStage);
    const serverRecord = (data.collaboration && typeof data.collaboration === "object") ? data.collaboration : null;
    if (serverRecord && Array.isArray(serverRecord.chat) && serverRecord.chat.length) {
      state.collaboration.cache[key] = serverRecord;
    } else {
      const existing = state.collaboration.cache[key] && typeof state.collaboration.cache[key] === "object"
        ? { ...state.collaboration.cache[key] }
        : { stage: safeStage, agent_name: "", chat: [], directives: [], proposals: [], decisions: [], evidence: [] };
      const chat = Array.isArray(existing.chat) ? existing.chat.slice() : [];
      const nowIso = new Date().toISOString();
      chat.push({ id: `local_user_${Date.now()}`, role: "user", stage: safeStage, created_at: nowIso, message });
      chat.push({
        id: `local_assistant_${Date.now()}`,
        role: "assistant",
        stage: safeStage,
        created_at: nowIso,
        message: String(data.assistant_message || "Response received."),
      });
      existing.chat = chat.slice(-100);
      existing.updated_at = nowIso;
      state.collaboration.cache[key] = existing;
    }
    if (data.run?.run_id) syncRunFromApiResponse(data.run);
    state.collaboration.drafts[collabDraftKey(safeStage)] = "";
    if (input) input.value = "";
    renderRun();
  } catch (err) {
    alert(`Chat failed: ${err.message || err}`);
  }
}

async function createCollabProposal(stage) {
  const safeStage = Number(stage || currentCollabStage() || 1);
  const runId = String(state.currentRun?.run_id || state.currentRunId || "");
  if (!runId) return;
  const title = String(document.getElementById("collab-proposal-title")?.value || "").trim();
  const summary = String(document.getElementById("collab-proposal-summary")?.value || "").trim();
  const op = String(document.getElementById("collab-proposal-op")?.value || "add").trim().toLowerCase();
  const path = String(document.getElementById("collab-proposal-path")?.value || "").trim();
  const rawValue = String(document.getElementById("collab-proposal-value")?.value || "");
  if (!path) {
    alert("JSON pointer path is required.");
    return;
  }
  try {
    const data = await api(`/api/runs/${encodeURIComponent(runId)}/stages/${safeStage}/collaboration/proposals`, {
      title,
      summary,
      op,
      path,
      value: parseJsonLoose(rawValue),
    });
    const key = collabCacheKey(runId, safeStage);
    state.collaboration.cache[key] = data.collaboration || null;
    if (data.run?.run_id) syncRunFromApiResponse(data.run);
    renderRun();
  } catch (err) {
    alert(`Create proposal failed: ${err.message || err}`);
  }
}

async function decideCollabProposal(stage, proposalId, decision) {
  const safeStage = Number(stage || currentCollabStage() || 1);
  const runId = String(state.currentRun?.run_id || state.currentRunId || "");
  if (!runId || !proposalId) return;
  const rationale = prompt(`Provide rationale for ${decision}:`, "");
  try {
    const data = await api(`/api/runs/${encodeURIComponent(runId)}/stages/${safeStage}/collaboration/proposals/${encodeURIComponent(proposalId)}/decision`, {
      decision,
      rationale: String(rationale || "").trim(),
    });
    const key = collabCacheKey(runId, safeStage);
    state.collaboration.cache[key] = data.collaboration || null;
    if (data.run?.run_id) syncRunFromApiResponse(data.run);
    renderRun();
  } catch (err) {
    alert(`Decision failed: ${err.message || err}`);
  }
}

function bindCollaborationPanelEvents(stage) {
  const safeStage = Number(stage || currentCollabStage() || 1);
  const sendBtn = document.getElementById("collab-chat-send");
  if (sendBtn) sendBtn.addEventListener("click", () => submitCollabChat(safeStage));
  const chatInput = document.getElementById("collab-chat-input");
  if (chatInput) {
    chatInput.addEventListener("input", () => {
      state.collaboration.drafts[collabDraftKey(safeStage)] = String(chatInput.value || "");
    });
    chatInput.addEventListener("keydown", (event) => {
      if (event.key === "Enter" && !event.shiftKey) {
        event.preventDefault();
        submitCollabChat(safeStage);
      }
    });
  }

  const createBtn = document.getElementById("collab-proposal-create");
  if (createBtn) createBtn.addEventListener("click", () => createCollabProposal(safeStage));

  el.collabTabContent?.querySelectorAll("[data-collab-approve]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const proposalId = btn.getAttribute("data-collab-approve") || "";
      decideCollabProposal(safeStage, proposalId, "approve");
    });
  });
  el.collabTabContent?.querySelectorAll("[data-collab-reject]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const proposalId = btn.getAttribute("data-collab-reject") || "";
      decideCollabProposal(safeStage, proposalId, "reject");
    });
  });
}

function renderCollaborationPanel() {
  if (!el.collaborationPanel || !el.collabTabContent) return;
  const runId = String(state.currentRun?.run_id || state.currentRunId || "");
  if (!runId) {
    el.collaborationPanel.classList.add("hidden");
    return;
  }
  el.collaborationPanel.classList.remove("hidden");
  const stage = currentCollabStage();
  if (el.collabStageLabel) {
    const agent = AGENTS.find((a) => a.stage === stage);
    el.collabStageLabel.textContent = `Stage ${stage}${agent ? ` · ${agent.name}` : ""}`;
  }
  if (!["chat", "proposals", "evidence", "decisions"].includes(state.collaboration.selectedTab)) {
    state.collaboration.selectedTab = "chat";
  }
  el.collabTabButtons?.querySelectorAll("[data-collab-tab]").forEach((btn) => {
    const tab = String(btn.getAttribute("data-collab-tab") || "chat");
    const active = tab === state.collaboration.selectedTab;
    btn.classList.toggle("btn-dark", active);
    btn.classList.toggle("btn-light", !active);
  });

  const key = collabCacheKey(runId, stage);
  const loading = state.collaboration.loadingKey === key;
  const loadError = state.collaboration.errorByKey[key];
  const record = collabRecord(stage);
  if (loading && !record) {
    el.collabTabContent.innerHTML = "<p class='text-slate-700'>Loading collaboration state...</p>";
    return;
  }
  if (loadError && !record) {
    el.collabTabContent.innerHTML = `<p class='text-rose-700'>${escapeHtml(loadError)}</p>`;
    return;
  }
  if (!record) {
    el.collabTabContent.innerHTML = "<p class='text-slate-700'>No collaboration data yet.</p>";
    ensureCollaborationLoaded(stage);
    return;
  }

  if (state.collaboration.selectedTab === "chat") {
    el.collabTabContent.innerHTML = renderCollabChatTab(stage, record);
  } else if (state.collaboration.selectedTab === "proposals") {
    el.collabTabContent.innerHTML = renderCollabProposalsTab(stage, record);
  } else if (state.collaboration.selectedTab === "evidence") {
    el.collabTabContent.innerHTML = renderCollabEvidenceTab(record);
  } else {
    el.collabTabContent.innerHTML = renderCollabDecisionsTab(record);
  }
  bindCollaborationPanelEvents(stage);
  ensureCollaborationLoaded(stage);
}

function openStageModal(stage) {
  const agent = AGENTS.find((a) => a.stage === stage);
  const result = latestResultByStage(state.currentRun, stage);
  if (!result || !agent) return;
  const runUseCase = String(state.currentRun?.pipeline_state?.use_case || state.currentRun?.use_case || "business_objectives");

  el.modalTitle.textContent = `Stage ${stage}: ${agent.name}`;
  el.modalSummary.textContent = result.summary || "";
  if (stage === 1 && state.currentRun?.run_id) {
    el.modalReadable.innerHTML = `
      <div class="rounded-lg border border-slate-300 bg-white p-2">
        <div class="flex flex-wrap items-center gap-2">
          <button data-analyst-export-summary class="btn-light rounded-md px-2 py-1 text-[11px] font-semibold">Export Summary</button>
          <button data-analyst-export-full class="btn-light rounded-md px-2 py-1 text-[11px] font-semibold">Export Full Evidence</button>
          <button data-analyst-upload-trigger class="btn-dark rounded-md px-2 py-1 text-[11px] font-semibold">Upload Modified</button>
          <input data-analyst-upload-file type="file" class="hidden" accept=".md,.txt,.json" />
        </div>
        <p data-analyst-doc-status class="mt-1 text-[11px] text-slate-700">Export a summary or full evidence document, or upload an updated version.</p>
      </div>
      <div class="mt-2">${renderReadableOutput(stage, result.output || {}, runUseCase)}</div>
    `;
  } else {
    el.modalReadable.innerHTML = renderReadableOutput(stage, result.output || {}, runUseCase);
  }
  el.modalLogs.textContent = (result.logs || []).join("\n");
  if (stage === 7) {
    el.modalOutput.textContent = "Validation report is intentionally rendered as a human-readable summary in the Readable Output pane.";
  } else {
    el.modalOutput.textContent = JSON.stringify(result.output || {}, null, 2);
  }
  if (stage === 1 && state.currentRun?.run_id) {
    wireAnalystDocActions(el.modalReadable, state.currentRun);
    wireAnalystViewTabs(el.modalReadable, state.currentRun);
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

function renderRunControls() {
  const status = String(state.currentRun?.status || "").toLowerCase();
  const hasRun = Boolean(state.currentRunId);
  const canPause = hasRun && (status === "running" || status === "waiting_approval");
  const canResume = hasRun && status === "paused";
  const canRerun = hasRun && status && status !== "running";
  const canAbort = hasRun && !["completed", "failed", "aborted"].includes(status);
  const canIntervene = hasRun && (status === "running" || status === "waiting_approval");

  if (el.runPause) el.runPause.disabled = !canPause;
  if (el.runResume) el.runResume.disabled = !canResume;
  if (el.runRerunStage) el.runRerunStage.disabled = !canRerun;
  if (el.runAbort) el.runAbort.disabled = !canAbort;
  if (el.runIntervene) el.runIntervene.disabled = !canIntervene;
}

async function runControl(action, payload = {}) {
  const runId = state.currentRunId;
  if (!runId) {
    alert("Select a run first.");
    return;
  }
  try {
    await api(`/api/runs/${encodeURIComponent(runId)}/${action}`, payload);
    await syncRun(runId);
    await refreshRunHistory();
  } catch (err) {
    alert(`${action} failed: ${err.message}`);
  }
}

async function pauseRun() {
  await runControl("pause");
}

async function resumeRun() {
  await runControl("resume");
}

async function rerunSelectedStage() {
  const run = state.currentRun || {};
  const suggestedStage = Number(state.selectedStage || run.current_stage || 1);
  const stage = Number(window.prompt("Rerun from stage number (1-8):", String(Math.max(1, Math.min(8, suggestedStage)))) || "");
  if (!Number.isFinite(stage) || stage < 1 || stage > 8) {
    alert("Stage must be between 1 and 8.");
    return;
  }
  await runControl("rerun", { stage });
}

async function abortRun() {
  const reason = String(window.prompt("Reason for abort:", "Manual abort requested") || "").trim();
  if (!reason) {
    alert("Abort reason is required.");
    return;
  }
  await runControl("abort", { reason });
}

async function interveneRun() {
  await pauseRun();
  const run = state.currentRun || {};
  const p = run.pipeline_state || {};
  const branch = `synthetix/run-${run.run_id || "current"}`;
  const contextVersion = p?.context_vault_ref?.version_id || "latest";
  setGlobalSearchStatus(`Takeover session ready on ${branch} using context ${contextVersion}.`);
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
  renderCollaborationPanel();
  renderImpactDiff();
  renderLogs();
  renderFlowDiagram();
  renderRunControls();
  renderDiscoverInsights();
  renderVerifyPanels();
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
  invalidateCollaborationCache(data.run?.run_id || runId);
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
        invalidateCollaborationCache(payload.run?.run_id || runId);
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
        invalidateCollaborationCache(payload.run?.run_id || runId);
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
  renderVerifyPanels();
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
  const integrationContext = getIntegrationContext();
  if (String(integrationContext.domain_pack_error || "").trim()) {
    alert(`Domain Pack configuration error: ${integrationContext.domain_pack_error}`);
    setMode(MODES.DISCOVER);
    setWizardStep(1);
    setDiscoverStep(2);
    return;
  }

  if (!state.settings) {
    try {
      await loadSettings(false);
    } catch (_) {
      // keep existing UX below for missing credentials
    }
  }
  const selectedProvider = String(el.provider.value || "anthropic").toLowerCase();
  const llmProvider = state.settings?.llm?.providers?.[selectedProvider] || {};
  if (!llmProvider.has_secret) {
    alert(`No ${selectedProvider} API key is configured. Save it in Settings > LLM credentials.`);
    setMode(MODES.SETTINGS);
    return;
  }

  const useCase = currentUseCase();
  if (useCase === "code_modernization") {
    if (isModernizationRepoScanMode()) {
      const integration = integrationContext;
      const provider = String(integration?.brownfield?.repo_provider || "").toLowerCase();
      const repoUrl = String(integration?.brownfield?.repo_url || "").trim();
      if (provider !== "github" || !repoUrl) {
        alert("Code modernization repository scan mode requires a connected GitHub repository in Discover Connect.");
        return;
      }
    } else if (!(el.legacyCode.value || "").trim()) {
      alert("Legacy code is required for code modernization use case.");
      return;
    }
  }
  if (useCase === "database_conversion" && !(el.dbSchema.value || "").trim()) {
    alert("Legacy schema/SQL is required for database conversion use case.");
    return;
  }
  const discoverCompletion = discoverStepCompletion();
  const requiresConnectStep = useCase === "code_modernization" && isModernizationRepoScanMode();
  if (!discoverCompletion.scopeComplete || !discoverCompletion.scanComplete || (requiresConnectStep && !discoverCompletion.connectComplete)) {
    const blockers = [];
    if (requiresConnectStep && !discoverCompletion.connectComplete) blockers.push("Connect");
    if (!discoverCompletion.scopeComplete) blockers.push("Define scope");
    if (!discoverCompletion.scanComplete) blockers.push("Scan");
    alert(`Complete Discover step(s): ${blockers.join(", ")} before starting a run.`);
    setMode(MODES.DISCOVER);
    setWizardStep(1);
    if (requiresConnectStep && !discoverCompletion.connectComplete) setDiscoverStep(1);
    else if (!discoverCompletion.scopeComplete) setDiscoverStep(2);
    else if (!discoverCompletion.scanComplete) setDiscoverStep(3);
    return;
  }
  if (String(el.deploymentTarget.value || "local").toLowerCase() === "cloud" && !el.enableCloudPromotion?.checked) {
    alert("Cloud target is locked by local-first policy. Enable 'cloud promotion for this run' to continue.");
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
    temperature: Number(el.temperature.value || 0.3),
    parallel_agents: Number(el.parallelAgents.value || 5),
    max_retries: Number(el.maxRetries.value || 2),
    live_deploy: !!el.liveDeploy.checked,
    cluster_name: el.clusterName.value || "agent-pipeline",
    namespace: el.namespace.value || "agent-app",
    deploy_output_dir: el.deployOutputDir.value || "./deploy_output",
    integration_context: integrationContext,
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
  el.brandHomeBtn?.addEventListener("click", () => setMode(MODES.DASHBOARDS));
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
      el.contextDrawerToggle.textContent = hidden ? "Show Intelligence Drawer" : "Hide Intelligence Drawer";
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
  el.settingsLlmAnthropicConnect?.addEventListener("click", () => saveLlmProvider("anthropic").catch((err) => setSettingsMessage(err.message, true)));
  el.settingsLlmAnthropicTest?.addEventListener("click", () => testLlmProvider("anthropic").catch((err) => setSettingsMessage(err.message, true)));
  el.settingsLlmAnthropicDisconnect?.addEventListener("click", () => disconnectLlmProvider("anthropic").catch((err) => setSettingsMessage(err.message, true)));
  el.settingsLlmOpenaiConnect?.addEventListener("click", () => saveLlmProvider("openai").catch((err) => setSettingsMessage(err.message, true)));
  el.settingsLlmOpenaiTest?.addEventListener("click", () => testLlmProvider("openai").catch((err) => setSettingsMessage(err.message, true)));
  el.settingsLlmOpenaiDisconnect?.addEventListener("click", () => disconnectLlmProvider("openai").catch((err) => setSettingsMessage(err.message, true)));
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
  document.querySelectorAll("[data-city-overlay]").forEach((btn) => {
    btn.addEventListener("click", () => {
      state.cityOverlay = String(btn.getAttribute("data-city-overlay") || "none");
      document.querySelectorAll("[data-city-overlay]").forEach((b) => {
        if (!(b instanceof HTMLElement)) return;
        const active = String(b.getAttribute("data-city-overlay") || "") === state.cityOverlay;
        b.classList.toggle("btn-dark", active);
        b.classList.toggle("btn-light", !active);
      });
      renderDiscoverInsights();
    });
  });
  el.cityMapReset?.addEventListener("click", () => {
    state.graphView.city = { x: 0, y: 0, scale: 1 };
    state.graphSelected.city = "";
    renderDiscoverInsights();
  });
  el.systemMapSearch?.addEventListener("input", () => {
    state.systemSearch = String(el.systemMapSearch.value || "").trim().toLowerCase();
    renderDiscoverInsights();
  });
  el.systemMapClear?.addEventListener("click", () => {
    state.systemSearch = "";
    if (el.systemMapSearch) el.systemMapSearch.value = "";
    state.graphSelected.system = "";
    state.graphView.system = { x: 0, y: 0, scale: 1 };
    renderDiscoverInsights();
  });
  el.discoverExportBaseline?.addEventListener("click", () => {
    try {
      exportDiscoverBaselineReport();
      setGlobalSearchStatus("Baseline report exported from Discover.");
    } catch (err) {
      setGlobalSearchStatus(`Baseline export failed: ${err.message || err}`, true);
    }
  });
  el.wizardPrevDiscover?.addEventListener("click", () => {
    if (state.discoverStep > 1) setDiscoverStep(state.discoverStep - 1);
  });
  el.projectStateMode?.addEventListener("change", () => {
    state.projectState.sampleDatasetEnabled = false;
    const result = detectProjectStateHeuristic();
    applyProjectStateResult(result);
    autoTriggerDiscoverExternalViews({ force: true });
  });
  el.detectProjectState?.addEventListener("click", () => {
    state.projectState.sampleDatasetEnabled = false;
    applyProjectStateResult(detectProjectStateHeuristic());
    autoTriggerDiscoverExternalViews({ force: true });
  });
  el.discoverUseSample?.addEventListener("click", () => {
    applySampleDatasetPreset();
  });
  el.discoverRunAnalystBrief?.addEventListener("click", () => {
    loadDiscoverAnalystBrief({ force: true }).catch((err) => {
      state.discoverAnalystBrief = { loading: false, error: String(err?.message || err || "Failed to run analyst brief."), data: null, requestKey: "", threadId: "" };
      renderDiscoverAnalystBrief();
    });
  });
  el.bfLoadGithubTree?.addEventListener("click", () => {
    loadDiscoverGithubTree().catch((err) => {
      state.discoverGithubTree = { loading: false, error: String(err?.message || err || "Failed to load repo tree."), repo: null, tree: null };
      renderDiscoverIntegrationPreviews();
    });
  });
  el.bfLoadLinearIssues?.addEventListener("click", () => {
    loadDiscoverLinearIssues().catch((err) => {
      state.discoverLinearIssues = { loading: false, error: String(err?.message || err || "Failed to load issues."), team: null, issues: [], source: "" };
      renderDiscoverIntegrationPreviews();
    });
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
    el.domainJurisdiction,
    el.domainDataClassification,
    el.domainPackJson,
  ].forEach((node) => node?.addEventListener("input", () => {
    state.discoverGithubTree = { loading: false, error: "", repo: null, tree: null };
    state.discoverLinearIssues = { loading: false, error: "", team: null, issues: [], source: "" };
    state.discoverAutoFetch.githubKey = "";
    state.discoverAutoFetch.linearKey = "";
    state.discoverAnalystBrief = { loading: false, error: "", data: null, requestKey: "", threadId: "" };
    renderDomainPackControls();
    renderDiscoverStepper();
  }));
  [
    el.bfRepoProvider,
    el.bfIssueProvider,
    el.bfRuntimeTelemetry,
    el.gfRepoDestination,
    el.gfTrackerProvider,
    el.gfSaveGenerated,
    el.gfReadWriteTracker,
    el.modernizationSourceMode,
    el.analysisDepth,
    el.telemetryMode,
    el.domainPackSelect,
    el.domainJurisdiction,
  ].forEach((node) => node?.addEventListener("change", () => {
    state.discoverGithubTree = { loading: false, error: "", repo: null, tree: null };
    state.discoverLinearIssues = { loading: false, error: "", team: null, issues: [], source: "" };
    state.discoverAutoFetch.githubKey = "";
    state.discoverAutoFetch.linearKey = "";
    state.discoverAnalystBrief = { loading: false, error: "", data: null, requestKey: "", threadId: "" };
    toggleUseCasePanel();
    renderDomainPackControls();
    renderDiscoverStepper();
  }));

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
  el.enableCloudPromotion?.addEventListener("change", () => {
    toggleCloudConfig();
    renderTaskSummary();
  });
  el.taskType.addEventListener("change", () => {
    state.discoverAnalystBrief = { loading: false, error: "", data: null, requestKey: "", threadId: "" };
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
    state.discoverAnalystBrief = { loading: false, error: "", data: null, requestKey: "", threadId: "" };
    if (isCodeModernizationMode()) {
      el.objectives.dataset.autogen = "1";
      setAutogeneratedObjective();
    }
    renderDiscoverStepper();
    renderTaskSummary();
  });
  el.dbSource.addEventListener("change", () => {
    state.discoverAnalystBrief = { loading: false, error: "", data: null, requestKey: "", threadId: "" };
    if (isDatabaseConversionMode()) {
      el.objectives.dataset.autogen = "1";
      setAutogeneratedObjective();
    }
    renderDiscoverStepper();
    renderTaskSummary();
  });
  el.dbTarget.addEventListener("change", () => {
    state.discoverAnalystBrief = { loading: false, error: "", data: null, requestKey: "", threadId: "" };
    if (isDatabaseConversionMode()) {
      el.objectives.dataset.autogen = "1";
      setAutogeneratedObjective();
    }
    renderDiscoverStepper();
    renderTaskSummary();
  });
  el.objectives.addEventListener("input", () => {
    state.discoverAnalystBrief = { loading: false, error: "", data: null, requestKey: "", threadId: "" };
    el.objectives.dataset.autogen = "0";
    if (String(el.projectStateMode?.value || "auto") === "auto") {
      applyProjectStateResult(detectProjectStateHeuristic());
    } else {
      renderDiscoverStepper();
    }
    renderTaskSummary();
  });
  el.legacyCode.addEventListener("input", () => {
    state.discoverAnalystBrief = { loading: false, error: "", data: null, requestKey: "", threadId: "" };
    if (String(el.projectStateMode?.value || "auto") === "auto") {
      applyProjectStateResult(detectProjectStateHeuristic());
    }
    renderDiscoverStepper();
    renderTaskSummary();
  });
  el.dbSchema.addEventListener("input", () => {
    state.discoverAnalystBrief = { loading: false, error: "", data: null, requestKey: "", threadId: "" };
    if (String(el.projectStateMode?.value || "auto") === "auto") {
      applyProjectStateResult(detectProjectStateHeuristic());
    }
    renderDiscoverStepper();
    renderTaskSummary();
  });

  el.wizardContinue.addEventListener("click", async () => {
    if (state.discoverStep < 4) {
      const previousStep = state.discoverStep;
      if (!validateDiscoverStep(state.discoverStep)) return;
      setDiscoverStep(state.discoverStep + 1);
      if (previousStep === 2 && state.discoverStep === 3) {
        await loadDiscoverAnalystBrief({ force: true }).catch(() => {});
      }
      return;
    }
    const objectives = String(el.objectives.value || "").trim();
    if (!objectives) {
      alert("Please provide the business challenge first.");
      return;
    }
    if (isCodeModernizationMode()) {
      if (isModernizationRepoScanMode()) {
        const integration = getIntegrationContext();
        const provider = String(integration?.brownfield?.repo_provider || "").toLowerCase();
        const repoUrl = String(integration?.brownfield?.repo_url || "").trim();
        if (provider !== "github" || !repoUrl) {
          alert("For repository scan mode, connect a GitHub repository in Discover Connect.");
          return;
        }
      } else if (!(el.legacyCode.value || "").trim()) {
        alert("Please provide legacy code for code modernization, or switch source mode to repository scan.");
        return;
      }
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
  el.cloneBaseAgent?.addEventListener("change", refreshCloneRequirementsPackFields);
  el.cloneRequirementsPackProfile?.addEventListener("change", refreshCloneRequirementsPackFields);

  el.tasksRefresh.addEventListener("click", () => refreshTasks().catch((err) => alert(err.message)));
  el.verifyRefresh?.addEventListener("click", async () => {
    await refreshRunHistory().catch((err) => alert(err.message));
    await refreshTasks().catch((err) => alert(err.message));
    renderVerifyPanels();
  });
  el.verifyRunSelect?.addEventListener("change", () => {
    state.verify.selectedRunId = String(el.verifyRunSelect?.value || "").trim();
    renderVerifyPanels();
  });
  el.verifyTabButtons?.querySelectorAll("[data-verify-tab]").forEach((btn) => {
    btn.addEventListener("click", () => {
      state.verify.selectedTab = String(btn.getAttribute("data-verify-tab") || "summary");
      renderVerifyPanels();
    });
  });
  el.verifyExportJson?.addEventListener("click", async () => {
    const runId = String(state.verify.selectedRunId || state.currentRunId || "").trim();
    if (!runId) {
      alert("Select a run first.");
      return;
    }
    try {
      const run = runDetail(runId) || (await api(`/api/runs/${encodeURIComponent(runId)}`, null)).run;
      const baseline = _verifyBaselineRun(runId);
      const pack = buildEvidencePackFragment(run, baseline);
      downloadEvidencePackJson(pack, runId);
    } catch (err) {
      alert(`Failed to export JSON evidence pack: ${err.message}`);
    }
  });
  el.verifyExportPdf?.addEventListener("click", async () => {
    const runId = String(state.verify.selectedRunId || state.currentRunId || "").trim();
    if (!runId) {
      alert("Select a run first.");
      return;
    }
    try {
      const run = runDetail(runId) || (await api(`/api/runs/${encodeURIComponent(runId)}`, null)).run;
      const baseline = _verifyBaselineRun(runId);
      const pack = buildEvidencePackFragment(run, baseline);
      generateEvidencePackPdf(pack, runId);
    } catch (err) {
      alert(`Failed to generate PDF evidence pack: ${err.message}`);
    }
  });
  el.workItemsRefresh?.addEventListener("click", () => refreshWorkItems().catch((err) => alert(err.message)));
  el.workItemCreate?.addEventListener("click", () => createWorkItem().catch((err) => alert(err.message)));

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
    if (!file) return;
    const lower = String(file.name || "").toLowerCase();
    const isAccess = lower.endsWith(".mdb") || lower.endsWith(".accdb");
    if (isAccess) {
      setDbUploadStatus(`Parsing Access file ${file.name}...`);
      parseAccessDatabaseFile(file)
        .then((data) => {
          const schemaText = String(data.database_schema || "").trim();
          if (!schemaText) throw new Error("Access parser returned empty schema output.");
          el.dbSchema.value = schemaText;
          if (el.dbSource) el.dbSource.value = "Microsoft Access";
          toggleUseCasePanel();
          el.objectives.dataset.autogen = "1";
          setAutogeneratedObjective();
          const analysis = (data.analysis && typeof data.analysis === "object") ? data.analysis : {};
          const tableCount = Number(analysis.table_count || 0);
          const parser = String(analysis.parser || "mdbtools");
          setDbUploadStatus(`Access parsed via ${parser}: ${tableCount} table(s) extracted into migration-ready schema.`);
          if (String(el.projectStateMode?.value || "auto") === "auto") {
            applyProjectStateResult(detectProjectStateHeuristic());
          }
          renderDiscoverStepper();
          renderTaskSummary();
        })
        .catch((err) => {
          setDbUploadStatus(`Access parse failed: ${err.message || err}`, true);
          alert(`Access parse failed: ${err.message || err}`);
        });
      return;
    }

    readTextFile(file, (text) => {
      el.dbSchema.value = text;
      setDbUploadStatus(`Loaded text schema file: ${file.name}`);
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

  el.uploadDomainPack?.addEventListener("click", () => el.domainPackFile?.click());
  el.domainPackFile?.addEventListener("change", () => {
    const file = el.domainPackFile?.files?.[0];
    readTextFile(file, (text) => {
      if (el.domainPackJson) el.domainPackJson.value = text;
      if (el.domainPackSelect) el.domainPackSelect.value = "custom";
      renderDomainPackControls();
      renderDiscoverStepper();
      renderTaskSummary();
    });
  });

  el.runPipeline.addEventListener("click", startRun);
  el.runPause?.addEventListener("click", () => pauseRun().catch((err) => alert(err.message)));
  el.runResume?.addEventListener("click", () => resumeRun().catch((err) => alert(err.message)));
  el.runRerunStage?.addEventListener("click", () => rerunSelectedStage().catch((err) => alert(err.message)));
  el.runIntervene?.addEventListener("click", () => interveneRun().catch((err) => alert(err.message)));
  el.runAbort?.addEventListener("click", () => abortRun().catch((err) => alert(err.message)));
  el.approveStage.addEventListener("click", () => submitApproval("approve"));
  el.rejectStage.addEventListener("click", () => submitApproval("reject"));
  el.loadRun.addEventListener("click", () => loadRunFromHistory().catch((err) => alert(err.message)));
  el.refreshRunHistory.addEventListener("click", () => refreshRunHistory().catch((err) => alert(err.message)));
  el.refreshArtifacts.addEventListener("click", () => refreshArtifactsList().catch((err) => alert(err.message)));
  el.viewArtifact.addEventListener("click", () => openSelectedArtifact().catch((err) => alert(err.message)));
  el.runImpactForecast?.addEventListener("click", () => runImpactForecastNow().catch((err) => alert(err.message)));
  el.runDriftScan?.addEventListener("click", () => runDriftScanNow().catch((err) => alert(err.message)));
  document.querySelectorAll("[data-impact-tab]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const tab = btn.getAttribute("data-impact-tab") || "topology";
      state.impactDiffTab = tab;
      renderImpactDiff();
    });
  });
  el.collabTabButtons?.querySelectorAll("[data-collab-tab]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const tab = String(btn.getAttribute("data-collab-tab") || "chat");
      state.collaboration.selectedTab = tab;
      renderCollaborationPanel();
    });
  });
  el.closeModal.addEventListener("click", () => el.outputModal.close());
}

async function init() {
  [el.brandLogoSidebar, el.brandLogoHero].forEach((imgNode) => {
    if (!imgNode) return;
    imgNode.addEventListener("error", () => {
      imgNode.classList.add("hidden");
    });
  });
  bindEvents();
  await loadDomainPackCatalog().catch(() => {});
  setDefaultModelByProvider();
  toggleUseCasePanel();
  toggleCloudConfig();
  renderDomainPackControls();
  setWizardStep(1);
  applyProjectStateResult(detectProjectStateHeuristic());
  setDiscoverStep(1);

  await loadAgentsAndTeams();
  await refreshRunHistory();
  await refreshArtifactsList();
  await refreshTasks().catch(() => {});
  await loadSettings().catch(() => {});

  renderRun();
  renderVerifyPanels();
  setMode(MODES.DASHBOARDS);
}

init().catch((err) => {
  el.pipelineStatusText.textContent = `INIT ERROR: ${err.message}`;
});
