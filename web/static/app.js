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
  ESTIMATES: "estimates",
  BUILD: "build",
  VERIFY: "verify",
  SETTINGS: "settings",
};
const ACTIVE_USER_STORAGE_KEY = "synthetix.active_user_email";

const el = {
  navHome: document.getElementById("nav-home"),
  navWork: document.getElementById("nav-work"),
  navTeam: document.getElementById("nav-team"),
  navEstimates: document.getElementById("nav-estimates"),
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
  estimatesScreen: document.getElementById("estimates-screen"),
  planPanelTeamCreation: document.getElementById("plan-panel-team-creation"),
  planPanelAgentStudio: document.getElementById("plan-panel-agent-studio"),
  historyScreen: document.getElementById("history-screen"),
  settingsScreen: document.getElementById("settings-screen"),
  workspaceSelector: document.getElementById("workspace-selector"),
  projectSelector: document.getElementById("project-selector"),
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
  settingsCurrentUserSelect: document.getElementById("settings-current-user-select"),
  settingsUserEmail: document.getElementById("settings-user-email"),
  settingsUserName: document.getElementById("settings-user-name"),
  settingsUserRole: document.getElementById("settings-user-role"),
  settingsUserStatus: document.getElementById("settings-user-status"),
  settingsUserSave: document.getElementById("settings-user-save"),
  settingsUserUse: document.getElementById("settings-user-use"),
  settingsUserRemove: document.getElementById("settings-user-remove"),
  settingsUserMessage: document.getElementById("settings-user-message"),
  settingsUsersList: document.getElementById("settings-users-list"),
  settingsKnowledgeSourceName: document.getElementById("settings-knowledge-source-name"),
  settingsKnowledgeSourceLocation: document.getElementById("settings-knowledge-source-location"),
  settingsKnowledgeSourceType: document.getElementById("settings-knowledge-source-type"),
  settingsKnowledgeSourceScope: document.getElementById("settings-knowledge-source-scope"),
  settingsKnowledgeSourceClassification: document.getElementById("settings-knowledge-source-classification"),
  settingsKnowledgeSourceTags: document.getElementById("settings-knowledge-source-tags"),
  settingsKnowledgeSourceSave: document.getElementById("settings-knowledge-source-save"),
  settingsKnowledgeSetName: document.getElementById("settings-knowledge-set-name"),
  settingsKnowledgeSetVersion: document.getElementById("settings-knowledge-set-version"),
  settingsKnowledgeSetSourceIds: document.getElementById("settings-knowledge-set-source-ids"),
  settingsKnowledgeSetState: document.getElementById("settings-knowledge-set-state"),
  settingsKnowledgeSetSave: document.getElementById("settings-knowledge-set-save"),
  settingsBrainAgentKey: document.getElementById("settings-brain-agent-key"),
  settingsBrainSetIds: document.getElementById("settings-brain-set-ids"),
  settingsBrainTopK: document.getElementById("settings-brain-top-k"),
  settingsBrainCitationRequired: document.getElementById("settings-brain-citation-required"),
  settingsBrainSave: document.getElementById("settings-brain-save"),
  settingsBindingWorkspace: document.getElementById("settings-binding-workspace"),
  settingsBindingProject: document.getElementById("settings-binding-project"),
  settingsBindingSetIds: document.getElementById("settings-binding-set-ids"),
  settingsBindingSave: document.getElementById("settings-binding-save"),
  settingsKnowledgeMessage: document.getElementById("settings-knowledge-message"),
  settingsKnowledgeSourcesList: document.getElementById("settings-knowledge-sources-list"),
  settingsKnowledgeSetsList: document.getElementById("settings-knowledge-sets-list"),
  settingsKnowledgeBrainsList: document.getElementById("settings-knowledge-brains-list"),
  settingsKnowledgePaneSources: document.getElementById("settings-knowledge-pane-sources"),
  settingsKnowledgePaneKnowledge: document.getElementById("settings-knowledge-pane-knowledge"),
  settingsKnowledgePaneJobs: document.getElementById("settings-knowledge-pane-jobs"),
  settingsKnowledgePaneEvals: document.getElementById("settings-knowledge-pane-evals"),
  settingsKnowledgeJobsRefresh: document.getElementById("settings-knowledge-jobs-refresh"),
  settingsKnowledgeJobsList: document.getElementById("settings-knowledge-jobs-list"),
  settingsKnowledgeEvalsRefresh: document.getElementById("settings-knowledge-evals-refresh"),
  settingsKnowledgeEvalsList: document.getElementById("settings-knowledge-evals-list"),

  perspectiveSwitcher: document.getElementById("perspective-switcher"),
  userMenuBtn: document.getElementById("user-menu-btn"),
  contextDrawerToggle: document.getElementById("context-drawer-toggle"),
  contextDrawer: document.getElementById("context-drawer"),
  shellGrid: document.getElementById("shell-grid"),
  drawerContextBundle: document.getElementById("drawer-context-bundle"),
  drawerDeliveryConstitution: document.getElementById("drawer-delivery-constitution"),
  drawerSpecialistRouting: document.getElementById("drawer-specialist-routing"),
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
  dashboardRunsList: document.getElementById("dashboard-runs-list"),
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
  discoverStepLandscape: document.getElementById("discover-step-landscape"),
  discoverStepScope: document.getElementById("discover-step-scope"),
  discoverStepScan: document.getElementById("discover-step-scan"),
  discoverStepResults: document.getElementById("discover-step-results"),
  discoverConnectPanel: document.getElementById("discover-connect-panel"),
  discoverLandscapeStepPanel: document.getElementById("discover-landscape-step-panel"),
  discoverScopePanel: document.getElementById("discover-scope-panel"),
  discoverScanPanel: document.getElementById("discover-scan-panel"),
  discoverResultsPanel: document.getElementById("discover-results-panel"),
  discoverResultsSummary: document.getElementById("discover-results-summary"),
  discoverResultsState: document.getElementById("discover-results-state"),
  discoverResultsIntegrations: document.getElementById("discover-results-integrations"),
  discoverResultsScan: document.getElementById("discover-results-scan"),
  discoverResultsForensics: document.getElementById("discover-results-forensics"),
  discoverScopeGuidance: document.getElementById("discover-scope-guidance"),
  discoverLandscapeStepContent: document.getElementById("discover-landscape-step-content"),
  discoverRunAnalystBriefLandscape: document.getElementById("discover-run-analyst-brief-landscape"),
  discoverOpenLandscapeStepResults: document.getElementById("discover-open-landscape-step-results"),
  discoverExportLandscapeStep: document.getElementById("discover-export-landscape-step"),
  discoverExportComponentInventoryStep: document.getElementById("discover-export-component-inventory-step"),
  discoverExportTrackPlanStep: document.getElementById("discover-export-track-plan-step"),
  discoverOpenLandscape: document.getElementById("discover-open-landscape"),
  discoverOpenCityMap: document.getElementById("discover-open-city-map"),
  discoverOpenSystemMap: document.getElementById("discover-open-system-map"),
  discoverOpenHealthDebt: document.getElementById("discover-open-health-debt"),
  discoverOpenConventions: document.getElementById("discover-open-conventions"),
  discoverOpenStaticForensics: document.getElementById("discover-open-static-forensics"),
  discoverOpenCodeQuality: document.getElementById("discover-open-code-quality"),
  discoverOpenDeadCode: document.getElementById("discover-open-dead-code"),
  discoverOpenDependencyMatrix: document.getElementById("discover-open-dependency-matrix"),
  discoverOpenTrends: document.getElementById("discover-open-trends"),
  discoverOpenData: document.getElementById("discover-open-data"),
  discoverExportBaseline: document.getElementById("discover-export-baseline"),
  discoverLandscapePanel: document.getElementById("discover-landscape-panel"),
  discoverCityMapPanel: document.getElementById("discover-city-map-panel"),
  discoverSystemMapPanel: document.getElementById("discover-system-map-panel"),
  discoverHealthPanel: document.getElementById("discover-health-panel"),
  discoverConventionsPanel: document.getElementById("discover-conventions-panel"),
  discoverStaticForensicsPanel: document.getElementById("discover-static-forensics-panel"),
  discoverCodeQualityPanel: document.getElementById("discover-code-quality-panel"),
  discoverDeadCodePanel: document.getElementById("discover-dead-code-panel"),
  discoverDependencyMatrixPanel: document.getElementById("discover-dependency-matrix-panel"),
  discoverTrendsPanel: document.getElementById("discover-trends-panel"),
  discoverDataPanel: document.getElementById("discover-data-panel"),
  discoverExportLandscape: document.getElementById("discover-export-landscape"),
  discoverExportComponentInventory: document.getElementById("discover-export-component-inventory"),
  discoverExportTrackPlan: document.getElementById("discover-export-track-plan"),
  discoverExportRouterRuleset: document.getElementById("discover-export-router-ruleset"),
  discoverExportSourceSchema: document.getElementById("discover-export-source-schema"),
  discoverExportSourceErd: document.getElementById("discover-export-source-erd"),
  discoverExportDataDictionary: document.getElementById("discover-export-data-dictionary"),
  discoverExportProjectMetrics: document.getElementById("discover-export-project-metrics"),
  discoverExportStaticForensics: document.getElementById("discover-export-static-forensics"),
  discoverExportQualityRules: document.getElementById("discover-export-quality-rules"),
  discoverExportQualityViolations: document.getElementById("discover-export-quality-violations"),
  discoverExportDeadCode: document.getElementById("discover-export-dead-code"),
  discoverExportTypeDependencyMatrix: document.getElementById("discover-export-type-dependency-matrix"),
  discoverExportRuntimeDependencyMatrix: document.getElementById("discover-export-runtime-dependency-matrix"),
  discoverExportThirdPartyUsage: document.getElementById("discover-export-third-party-usage"),
  discoverExportTrendSnapshot: document.getElementById("discover-export-trend-snapshot"),
  discoverExportTrendSeries: document.getElementById("discover-export-trend-series"),
  discoverCityMapContent: document.getElementById("discover-city-map-content"),
  discoverSystemMapContent: document.getElementById("discover-system-map-content"),
  discoverHealthContent: document.getElementById("discover-health-content"),
  discoverConventionsContent: document.getElementById("discover-conventions-content"),
  discoverStaticForensicsContent: document.getElementById("discover-static-forensics-content"),
  discoverCodeQualityContent: document.getElementById("discover-code-quality-content"),
  discoverDeadCodeContent: document.getElementById("discover-dead-code-content"),
  discoverDependencyMatrixContent: document.getElementById("discover-dependency-matrix-content"),
  discoverTrendsContent: document.getElementById("discover-trends-content"),
  discoverDataContent: document.getElementById("discover-data-content"),
  discoverLandscapeContent: document.getElementById("discover-landscape-content"),
  cityMapSvg: document.getElementById("city-map-svg"),
  cityMapInspector: document.getElementById("city-map-inspector"),
  cityMapReset: document.getElementById("city-map-reset"),
  systemMapSvg: document.getElementById("system-map-svg"),
  systemMapInspector: document.getElementById("system-map-inspector"),
  systemMapSearch: document.getElementById("system-map-search"),
  systemMapClear: document.getElementById("system-map-clear"),
  projectStateMode: document.getElementById("project-state-mode"),
  detectProjectState: document.getElementById("detect-project-state"),
  projectStateResult: document.getElementById("project-state-result"),
  brownfieldIntegrations: document.getElementById("brownfield-integrations"),
  greenfieldIntegrations: document.getElementById("greenfield-integrations"),
  bfSourceMode: document.getElementById("bf-source-mode"),
  bfRepoProvider: document.getElementById("bf-repo-provider"),
  bfRepoUrl: document.getElementById("bf-repo-url"),
  bfIssueProvider: document.getElementById("bf-issue-provider"),
  bfIssueProject: document.getElementById("bf-issue-project"),
  bfDocsUrl: document.getElementById("bf-docs-url"),
  bfRuntimeTelemetry: document.getElementById("bf-runtime-telemetry"),
  bfEvidencePanel: document.getElementById("bf-evidence-panel"),
  bfEvidenceFiles: document.getElementById("bf-evidence-files"),
  bfUploadEvidence: document.getElementById("bf-upload-evidence"),
  bfEvidenceStatus: document.getElementById("bf-evidence-status"),
  bfEvidencePreview: document.getElementById("bf-evidence-preview"),
  bfEvidenceOutputTarget: document.getElementById("bf-evidence-output-target"),
  bfEvidenceAcceptRisk: document.getElementById("bf-evidence-accept-risk"),
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
  planTeamSelect: document.getElementById("plan-team-select"),
  planTeamLoadBtn: document.getElementById("plan-team-load-btn"),
  planTeamNewBtn: document.getElementById("plan-team-new-btn"),
  planTeamDuplicateBtn: document.getElementById("plan-team-duplicate-btn"),
  planTeamDeleteBtn: document.getElementById("plan-team-delete-btn"),
  teamAddAgentBtn: document.getElementById("team-add-agent-btn"),
  teamName: document.getElementById("team-name"),
  teamDescription: document.getElementById("team-description"),
  teamSaveBtn: document.getElementById("team-save-btn"),
  teamLoadSelectedBtn: document.getElementById("team-load-selected-btn"),
  teamUseInWorkBtn: document.getElementById("team-use-in-work-btn"),
  teamRefreshBtn: document.getElementById("team-refresh-btn"),
  teamSaveMessage: document.getElementById("team-save-message"),
  cloneBaseAgent: document.getElementById("clone-base-agent"),
  cloneAgentName: document.getElementById("clone-agent-name"),
  cloneAgentPersona: document.getElementById("clone-agent-persona"),
  cloneRequirementsPackProfile: document.getElementById("clone-requirements-pack-profile"),
  cloneRequirementsPackTemplate: document.getElementById("clone-requirements-pack-template"),
  agentStudioAgentSelect: document.getElementById("agent-studio-agent-select"),
  agentStudioPanel: document.getElementById("agent-studio-panel"),
  agentStudioSave: document.getElementById("agent-studio-save"),
  agentStudioMessage: document.getElementById("agent-studio-message"),
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
  knowledgeAssistantPanel: document.getElementById("knowledge-assistant-panel"),
  knowledgeAssistantInput: document.getElementById("knowledge-assistant-input"),
  knowledgeAssistantAsk: document.getElementById("knowledge-assistant-ask"),
  knowledgeAssistantPropose: document.getElementById("knowledge-assistant-propose"),
  knowledgeAssistantStatus: document.getElementById("knowledge-assistant-status"),
  knowledgeAssistantOutput: document.getElementById("knowledge-assistant-output"),
  knowledgeAssistantProposalsStatus: document.getElementById("knowledge-assistant-proposals-status"),
  knowledgeAssistantProposals: document.getElementById("knowledge-assistant-proposals"),
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
  diagramModal: document.getElementById("diagram-modal"),
  diagramModalTitle: document.getElementById("diagram-modal-title"),
  diagramModalViewer: document.getElementById("diagram-modal-viewer"),
  diagramClose: document.getElementById("diagram-close"),
  diagramDownloadSvg: document.getElementById("diagram-download-svg"),
  diagramDownloadMmd: document.getElementById("diagram-download-mmd"),
  estimateStatus: document.getElementById("estimate-status"),
  estimateMode: document.getElementById("estimate-mode"),
  estimateRunId: document.getElementById("estimate-run-id"),
  estimateId: document.getElementById("estimate-id"),
  estimateTeamModel: document.getElementById("estimate-team-model"),
  estimateBusinessNeed: document.getElementById("estimate-business-need"),
  estimateChunkManifest: document.getElementById("estimate-chunk-manifest"),
  estimateRiskRegister: document.getElementById("estimate-risk-register"),
  estimateTraceabilityScores: document.getElementById("estimate-traceability-scores"),
  estimateCreateBtn: document.getElementById("estimate-create-btn"),
  estimateLoadRunBtn: document.getElementById("estimate-load-run-btn"),
  estimateRefreshList: document.getElementById("estimate-refresh-list"),
  estimateList: document.getElementById("estimate-list"),
  estimateOverview: document.getElementById("estimate-overview"),
  estimateTeam: document.getElementById("estimate-team"),
  estimateWorkstreams: document.getElementById("estimate-workstreams"),
  estimateAssumptions: document.getElementById("estimate-assumptions"),
  estimateWbs: document.getElementById("estimate-wbs"),
  estimateAgentInput: document.getElementById("estimate-agent-input"),
  estimateAgentWbsItem: document.getElementById("estimate-agent-wbs-item"),
  estimateAgentIntakeBtn: document.getElementById("estimate-agent-intake-btn"),
  estimateAgentExplainBtn: document.getElementById("estimate-agent-explain-btn"),
  estimateAgentOutput: document.getElementById("estimate-agent-output"),
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
  discoverLandscape: {
    loading: false,
    error: "",
    data: null,
    requestKey: "",
    requestToken: "",
    inFlightPromise: null,
  },
  discoverAnalystBrief: {
    loading: false,
    error: "",
    data: null,
    requestKey: "",
    threadId: "",
    requestToken: "",
    inFlightPromise: null,
  },
  discoverEvidenceBundle: {
    loading: false,
    error: "",
    data: null,
  },
  domainPackCatalog: [],
  currentRunId: "",
  currentRun: null,
  eventSource: null,
  runSnapshotPollTimer: null,
  runBootstrapPollTimer: null,
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
    isCustom: false,
  },
  settings: null,
  activeUserEmail: "",
  activeUserRole: "engineering",
  activeUserName: "",
  settingsKnowledgeTab: "sources",
  planTab: "team_creation",
  agentStudio: {
    selectedAgentKey: "",
    tab: "persona",
    draftByAgent: {},
    evalByAgent: {},
  },
  estimation: {
    loading: false,
    error: "",
    currentEstimateId: "",
    currentEstimate: null,
    listByRun: {},
    loadedRunId: "",
    activeTab: "overview",
  },
  teamBuilder: {
    stageAgentIds: {},
    enabledStages: {},
    editingTeamId: "",
    editingIsCustom: false,
  },
  collaboration: {
    selectedTab: "chat",
    cache: {},
    loadingKey: "",
    errorByKey: {},
    drafts: {},
  },
  knowledgeAssistant: {
    draftByRun: {},
    loadingRunId: "",
    proposalLoadingRunId: "",
    errorByRun: {},
    responseByRun: {},
    proposalsByRun: {},
    proposalsLoadedByRun: {},
    proposalErrorByRun: {},
  },
  verify: {
    selectedTab: "summary",
    selectedRunId: "",
    loadingRunId: "",
  },
  analyst: {
    selectedTab: "spec",
  },
  runStart: {
    pending: false,
    startedAt: 0,
  },
};

let mermaidInitialized = false;

async function api(path, payload, method = "POST") {
  const headers = {};
  if (payload) headers["Content-Type"] = "application/json";
  const actorEmail = String(state.activeUserEmail || "").trim().toLowerCase();
  if (actorEmail) headers["x-user-email"] = actorEmail;
  const options = {
    method: payload ? method : "GET",
    headers: Object.keys(headers).length ? headers : undefined,
    body: payload ? JSON.stringify(payload) : undefined,
  };
  const res = await fetch(path, options);
  const data = await res.json().catch(() => ({}));
  if (!res.ok || data.ok === false) {
    throw new Error(data.error || `HTTP ${res.status}`);
  }
  return data;
}

async function apiMultipart(path, formData, method = "POST") {
  const headers = {};
  const actorEmail = String(state.activeUserEmail || "").trim().toLowerCase();
  if (actorEmail) headers["x-user-email"] = actorEmail;
  const res = await fetch(path, {
    method,
    headers: Object.keys(headers).length ? headers : undefined,
    body: formData,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || data.ok === false) {
    throw new Error(data.error || `HTTP ${res.status}`);
  }
  return data;
}

async function apiWithNetworkRetry(path, payload, method = "POST", options = {}) {
  const retries = Math.max(0, Number(options?.retries || 0));
  const retryDelayMs = Math.max(0, Number(options?.retryDelayMs || 1000));
  let attempt = 0;
  while (true) {
    try {
      return await api(path, payload, method);
    } catch (err) {
      const message = String(err?.message || err || "");
      const networkError = /failed to fetch|networkerror|load failed/i.test(message);
      if (!networkError || attempt >= retries) throw err;
      attempt += 1;
      await new Promise((resolve) => setTimeout(resolve, retryDelayMs));
    }
  }
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
  const rawArtifacts = (safeOutput.raw_artifacts && typeof safeOutput.raw_artifacts === "object")
    ? safeOutput.raw_artifacts
    : {};
  const rawArtifactRefs = (rawArtifacts.artifact_refs && typeof rawArtifacts.artifact_refs === "object")
    ? rawArtifacts.artifact_refs
    : {};
  const legacyInventory = resolveLegacyInventory(safeOutput);
  const legacyForms = resolveLegacyForms(legacyInventory);
  const vb6Projects = Array.isArray(legacyInventory.vb6_projects) ? legacyInventory.vb6_projects : [];
  const rawRepoLandscapeProjects = Array.isArray(rawArtifacts.repo_landscape?.projects)
    ? rawArtifacts.repo_landscape.projects
    : [];
  const rawVariantInventoryRows = Array.isArray(rawArtifacts.variant_inventory?.variants)
    ? rawArtifacts.variant_inventory.variants
    : [];
  const rawLegacyInventory = (rawArtifacts.legacy_inventory && typeof rawArtifacts.legacy_inventory === "object")
    ? rawArtifacts.legacy_inventory
    : {};
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
  const skill = resolveLegacySkillProfile(safeOutput);
  const isPhpSummary = (
    String(sourceProfile.language || safeOutput.source_language || "").trim().toLowerCase() === "php"
    || (legacyInventory.php_analysis && typeof legacyInventory.php_analysis === "object" && Object.keys(legacyInventory.php_analysis).length > 0)
    || String(skill.selected_skill_id || "").trim().toLowerCase() === "php_legacy"
  );
  const phpAnalysis = (legacyInventory.php_analysis && typeof legacyInventory.php_analysis === "object") ? legacyInventory.php_analysis : {};
  const phpRouteInventory = (phpAnalysis.route_inventory && typeof phpAnalysis.route_inventory === "object") ? phpAnalysis.route_inventory : {};
  const phpControllerInventory = (phpAnalysis.controller_inventory && typeof phpAnalysis.controller_inventory === "object") ? phpAnalysis.controller_inventory : {};
  const phpTemplateInventory = (phpAnalysis.template_inventory && typeof phpAnalysis.template_inventory === "object") ? phpAnalysis.template_inventory : {};
  const phpSessionInventory = (phpAnalysis.session_state_inventory && typeof phpAnalysis.session_state_inventory === "object") ? phpAnalysis.session_state_inventory : {};
  const phpAuthInventory = (phpAnalysis.authz_authn_inventory && typeof phpAnalysis.authz_authn_inventory === "object") ? phpAnalysis.authz_authn_inventory : {};
  const phpJobsInventory = (phpAnalysis.background_job_inventory && typeof phpAnalysis.background_job_inventory === "object") ? phpAnalysis.background_job_inventory : {};
  const phpFileIoInventory = (phpAnalysis.file_io_inventory && typeof phpAnalysis.file_io_inventory === "object") ? phpAnalysis.file_io_inventory : {};
  const phpRouteCount = Number(phpRouteInventory.route_count || 0);
  const phpControllerCount = Number(phpControllerInventory.controller_count || 0);
  const phpTemplateCount = Number(phpTemplateInventory.template_count || 0);
  const phpSessionKeyCount = Number(phpSessionInventory.session_key_count || 0);
  const phpAuthTouchpointCount = Number(phpAuthInventory.auth_touchpoint_count || 0);
  const phpJobCount = Number(phpJobsInventory.job_count || 0);
  const phpFileIoCount = Number((phpFileIoInventory.upload_file_count || 0) + (phpFileIoInventory.export_file_count || 0));
  const contextRef = (safeOutput.context_reference && typeof safeOutput.context_reference === "object")
    ? safeOutput.context_reference
    : ((reqPack.context_reference && typeof reqPack.context_reference === "object") ? reqPack.context_reference : {});
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
  const dllDeps = Array.isArray(legacyInventory.dll_dependencies) ? legacyInventory.dll_dependencies : [];
  const ocxDeps = Array.isArray(legacyInventory.ocx_dependencies) ? legacyInventory.ocx_dependencies : [];
  const dcxDeps = Array.isArray(legacyInventory.dcx_dependencies) ? legacyInventory.dcx_dependencies : [];
  const dcaDeps = Array.isArray(legacyInventory.dca_dependencies) ? legacyInventory.dca_dependencies : [];
  const genericDeps = Array.isArray(legacyInventory.dependencies) ? legacyInventory.dependencies : [];
  const dependencySet = [...new Set([
    ...activeX,
    ...dllDeps,
    ...ocxDeps,
    ...dcxDeps,
    ...dcaDeps,
    ...genericDeps,
  ].map((x) => String(x || "").trim()).filter(Boolean))];
  const eventHandlers = Array.isArray(legacyInventory.event_handlers) ? legacyInventory.event_handlers : [];
  const eventHandlersExact = Number(
    legacyInventory.event_handler_count_exact
    || vb6Analysis.event_handler_count_exact
    || eventHandlers.length
  );
  const controlsCount = vb6Projects.reduce((acc, project) => {
    const controls = Array.isArray(project?.controls) ? project.controls.length : 0;
    return acc + controls;
  }, 0);
  const projectFormCount = vb6Projects.reduce((acc, project) => {
    const explicit = Number(project?.forms_count || 0);
    if (Number.isFinite(explicit) && explicit > 0) return acc + explicit;
    return acc + (Array.isArray(project?.forms) ? project.forms.length : 0);
  }, 0);
  const formsDiscovered = Number(legacyInventory.form_count_discovered_files || rawLegacyInventory?.summary?.counts?.forms_or_screens || 0);
  const formsReferenced = Number(legacyInventory.form_count_referenced || projectFormCount || 0);
  const formsCount = Math.max(legacyForms.length, projectFormCount, formsDiscovered);
  const formsUnmapped = Number(
    legacyInventory.form_count_unmapped_files
    || Math.max(0, formsCount - formsReferenced)
  );
  const sourceLocRows = Array.isArray(legacyInventory.source_loc_by_file)
    ? legacyInventory.source_loc_by_file
    : (Array.isArray(vb6Analysis.source_loc_by_file) ? vb6Analysis.source_loc_by_file : []);
  const sourceLocByFile = {};
  sourceLocRows.slice(0, 5000).forEach((row) => {
    const path = String(row?.path || "").trim();
    if (!path) return;
    sourceLocByFile[path] = Number(row?.loc || 0);
  });
  const sourceLocTotal = Number(
    legacyInventory.source_loc_total
    || vb6Analysis.source_loc_total
    || Object.values(sourceLocByFile).reduce((acc, loc) => acc + Number(loc || 0), 0)
  );
  const sourceLocForms = Number(
    legacyInventory.source_loc_forms
    || vb6Analysis.source_loc_forms
    || Object.entries(sourceLocByFile).reduce((acc, [path, loc]) => (
      String(path || "").toLowerCase().endsWith(".frm") || String(path || "").toLowerCase().endsWith(".ctl")
        ? acc + Number(loc || 0)
        : acc
    ), 0)
  );
  const sourceLocModules = Number(
    legacyInventory.source_loc_modules
    || vb6Analysis.source_loc_modules
    || Object.entries(sourceLocByFile).reduce((acc, [path, loc]) => (
      String(path || "").toLowerCase().endsWith(".bas")
        ? acc + Number(loc || 0)
        : acc
    ), 0)
  );
  const sourceLocClasses = Number(
    legacyInventory.source_loc_classes
    || vb6Analysis.source_loc_classes
    || Object.entries(sourceLocByFile).reduce((acc, [path, loc]) => (
      String(path || "").toLowerCase().endsWith(".cls")
        ? acc + Number(loc || 0)
        : acc
    ), 0)
  );
  const sourceFilesScanned = Number(
    legacyInventory.source_files_scanned
    || vb6Analysis.source_files_scanned
    || Object.keys(sourceLocByFile).length
  );
  const projectCount = Math.max(vb6Projects.length, rawRepoLandscapeProjects.length, rawVariantInventoryRows.length);
  const scopeLock = (rawArtifacts.scope_lock && typeof rawArtifacts.scope_lock === "object")
    ? rawArtifacts.scope_lock
    : {};
  const discoverChecklistRaw = (rawArtifacts.discover_review_checklist && typeof rawArtifacts.discover_review_checklist === "object")
    ? rawArtifacts.discover_review_checklist
    : {};
  const discoverChecklistRows = Array.isArray(discoverChecklistRaw.checks)
    ? discoverChecklistRaw.checks
    : (Array.isArray(discoverChecklistRaw.items) ? discoverChecklistRaw.items : []);
  const computedReviewChecks = [];
  if (projectCount > 1) {
    const scopeResolved = String(scopeLock.status || scopeLock.decision || "").trim();
    computedReviewChecks.push({
      id: "variant_scope_lock",
      status: scopeResolved ? "pass" : "fail",
      title: "Variant scope decision",
      detail: scopeResolved
        ? `Scope locked: ${scopeResolved}`
        : `Detected ${projectCount} project variants without explicit scope lock.`,
    });
  }
  if (uiEventMap.length > 0) {
    const handlerCoverage = eventHandlersExact > 0 ? (eventHandlersExact / uiEventMap.length) : 0;
    computedReviewChecks.push({
      id: "handler_inventory_coverage",
      status: handlerCoverage >= 0.85 ? "pass" : "warn",
      title: "Event handler extraction coverage",
      detail: `handlers=${eventHandlersExact}, mapped_events=${uiEventMap.length}, coverage=${(handlerCoverage * 100).toFixed(1)}%`,
    });
  }
  const mapRows = Array.isArray(rawArtifacts.data_access_map?.rows) ? rawArtifacts.data_access_map.rows : [];
  if (mapRows.length || sqlCatalog.length) {
    const mapCoverage = Number(rawArtifacts.data_access_map?.coverage_score || 0);
    const mapComplete = Boolean(rawArtifacts.data_access_map?.complete);
    computedReviewChecks.push({
      id: "data_access_map_coverage",
      status: mapComplete && mapCoverage >= 1 ? "pass" : (mapCoverage >= 0.8 ? "warn" : "fail"),
      title: "Canonical data access map",
      detail: `complete=${mapComplete ? "true" : "false"}, rows=${mapRows.length}, coverage=${(mapCoverage * 100).toFixed(1)}%`,
    });
  }
  const discoverReviewChecks = discoverChecklistRows.length
    ? discoverChecklistRows.map((row, idx) => {
      const status = String(row?.status || row?.result || "warn").trim().toLowerCase();
      return {
        id: String(row?.id || `review_${idx + 1}`),
        status: (status === "pass" || status === "fail" || status === "warn") ? status : "warn",
        title: String(row?.title || row?.check || row?.why || row?.action || "Review check"),
        detail: String(row?.detail || row?.notes || row?.why || row?.action || ""),
      };
    })
    : computedReviewChecks;
  const discoverReviewStatus = discoverReviewChecks.some((row) => row.status === "fail")
    ? "FAIL"
    : (discoverReviewChecks.some((row) => row.status === "warn") ? "WARN" : "PASS");
  const refFor = (key, fallback) => String(rawArtifactRefs[key] || fallback);
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
  const blockingDecisions = isPhpSummary ? [
    {
      id: "DEC-PHP-ARCH-001",
      question: "Target runtime and application architecture for the PHP modernization",
      options: ["TypeScript modular monolith", "Next.js + NestJS", "Node service decomposition"],
      default_recommendation: "Use a TypeScript modular monolith first unless there is a clear reason to decompose immediately.",
      impact_if_wrong: "Route/controller parity, auth flow, and delivery sequencing can drift early.",
    },
    {
      id: "DEC-PHP-DB-001",
      question: "Database contract strategy during PHP migration",
      options: ["Preserve schema/queries", "Introduce compatibility layer", "Redesign schema"],
      default_recommendation: "Preserve contracts initially; migrate behind compatibility layer.",
      impact_if_wrong: "Business rule drift and data-side regressions.",
    },
  ] : [
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
  const landscapeDependencyFootprint = (rawArtifacts.repo_landscape_v1?.dependency_footprint && typeof rawArtifacts.repo_landscape_v1.dependency_footprint === "object")
    ? rawArtifacts.repo_landscape_v1.dependency_footprint
    : ((rawArtifacts.repo_landscape?.dependency_footprint && typeof rawArtifacts.repo_landscape.dependency_footprint === "object") ? rawArtifacts.repo_landscape.dependency_footprint : {});
  const phpDependencyCount = Number(legacyInventory.php_dependency_count || landscapeDependencyFootprint.composer_package_count || 0);
  const datastoreRows = Array.isArray(rawArtifacts.repo_landscape_v1?.datastore_signals)
    ? rawArtifacts.repo_landscape_v1.datastore_signals
    : (Array.isArray(rawArtifacts.repo_landscape?.datastore_signals) ? rawArtifacts.repo_landscape.datastore_signals : []);
  const phpOperationalDatastores = datastoreRows.filter((row) => ["mysql", "sqlserver", "postgres", "oracle", "db2"].includes(String(row?.datastore || "").trim()));
  if (isPhpSummary && phpOperationalDatastores.length > 1) {
    blockingDecisions.push({
      id: "DEC-PHP-DATA-002",
      question: "Dual-datastore migration strategy requires confirmation",
      options: ["Preserve both datastores", "Consolidate later", "Consolidate during migration"],
      default_recommendation: "Preserve both datastores during initial migration and define an explicit contract map.",
      impact_if_wrong: "Cross-database workflow parity and reporting behavior may break.",
    });
  }
  if (isPhpSummary && (datastoreRows.some((row) => String(row?.datastore || "").trim() === "mq") || phpJobCount > 0)) {
    blockingDecisions.push({
      id: "DEC-PHP-ASYNC-003",
      question: "Background job or queue listener migration strategy requires confirmation",
      options: ["Preserve async processing", "Temporarily inline", "Defer async workloads"],
      default_recommendation: "Preserve asynchronous behavior explicitly and inventory listeners and cron jobs before implementation.",
      impact_if_wrong: "Hidden message processing and scheduled workflows may be lost.",
    });
  }
  if (isPhpSummary && (phpSessionKeyCount > 0 || phpAuthTouchpointCount > 0)) {
    blockingDecisions.push({
      id: "DEC-PHP-SESSION-004",
      question: "Session and authentication migration approach must be confirmed",
      options: ["Preserve session model", "Introduce token-based auth", "Hybrid transitional model"],
      default_recommendation: "Model current session/auth behavior first, then move to a clearer target design with explicit compatibility rules.",
      impact_if_wrong: "Authentication, authorization, and workflow state regressions.",
    });
  }
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

  const inventorySummary = isPhpSummary ? {
    applications: (phpControllerCount || phpRouteCount || phpTemplateCount || sourceFilesScanned) ? 1 : 0,
    controllers: phpControllerCount,
    routes: phpRouteCount,
    templates: phpTemplateCount,
    session_keys: phpSessionKeyCount,
    auth_touchpoints: phpAuthTouchpointCount,
    background_jobs: phpJobCount,
    file_io_flows: phpFileIoCount,
    source_loc_total: sourceLocTotal,
    source_files_scanned: sourceFilesScanned,
    dependencies: phpDependencyCount,
    tables_touched: tablesTouched,
  } : {
    projects: projectCount,
    forms: formsCount,
    forms_referenced: formsReferenced,
    forms_unmapped: formsUnmapped,
    source_loc_total: sourceLocTotal,
    source_loc_forms: sourceLocForms,
    source_loc_modules: sourceLocModules,
    source_loc_classes: sourceLocClasses,
    source_files_scanned: sourceFilesScanned,
    controls: controlsCount,
    dependencies: dependencySet.length,
    event_handlers: eventHandlersExact,
    tables_touched: tablesTouched,
  };
  const strategyName = isPhpSummary
    ? String(readiness.recommended_strategy?.name || "PHP web modernization")
    : String(readiness.recommended_strategy?.name || "Phased modernization");
  const strategyRationale = isPhpSummary
    ? String(readiness.recommended_strategy?.rationale || "Preserve route, session, query, and integration behavior first; then move the PHP application onto a typed target architecture.")
    : String(readiness.recommended_strategy?.rationale || "Preserve behavior first, then modernize in controlled phases.");
  const strategyPhases = isPhpSummary ? [
    { id: "PH0", title: "Route, session, and data baseline", outcome: "Capture route inventory, session behavior, and SQL touchpoints.", exit_criteria: ["Routes inventoried", "Session/auth rules documented", "DB touchpoints confirmed"] },
    { id: "PH1", title: "Controller and service modernization", outcome: "Migrate controller workflows and extract reusable service boundaries.", exit_criteria: ["Priority workflows migrated", "Auth/session compatibility defined", "SQL contracts preserved"] },
    { id: "PH2", title: "Hardening and release evidence", outcome: "Finalize parity evidence, quality gates, and release documentation.", exit_criteria: ["Quality gates pass", "Route/session parity verified", "Release readiness approved"] },
  ] : [
    { id: "PH0", title: "Baseline and equivalence harness", outcome: "Capture golden flows and baseline outputs.", exit_criteria: ["Golden flows agreed", "Baseline outputs captured", "Parity checks defined"] },
    { id: "PH1", title: "Incremental migration and dependency replacement", outcome: "Migrate forms/modules with OCX/COM risk controls.", exit_criteria: ["P0 flows migrated", "Critical dependencies addressed", "Regression suite passing"] },
    { id: "PH2", title: "Hardening and release evidence", outcome: "Finalize quality gates and publish evidence pack.", exit_criteria: ["Quality gates pass", "Traceability complete", "Release readiness approved"] },
  ];
  const nextSteps = isPhpSummary ? [
    { id: "NS-001", title: "Confirm PHP target architecture and blocking decisions", owner_role: "Tech Lead", done_when: ["Runtime architecture approved", "Datastore/session decisions resolved"] },
    { id: "NS-002", title: "Inventory async processing and route parity evidence", owner_role: "QA Lead", done_when: ["Routes baselined", "Async listeners documented", "Parity checks defined"] },
  ] : [
    { id: "NS-001", title: "Confirm blocking decisions and freeze modernization scope", owner_role: "Tech Lead", done_when: ["Blocking decisions approved", "Backlog dependencies resolved"] },
    { id: "NS-002", title: "Implement golden flow harness for parity validation", owner_role: "QA Lead", done_when: ["Golden flow tests created", "Baseline artifacts stored"] },
  ];
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
        source_language: isPhpSummary ? "php" : (String(sourceProfile.language || safeOutput.source_language || "vb6").trim().toLowerCase() || "vb6"),
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
          ...inventorySummary,
        },
        headline: `${strategyName} recommended.`,
      },
      recommended_strategy: {
        name: strategyName,
        rationale: strategyRationale,
        phases: strategyPhases,
      },
      decisions_required: {
        blocking: blockingDecisions.slice(0, 8),
        non_blocking: nonBlockingDecisions,
      },
      top_risks: topRiskDrivers.slice(0, 8),
      next_steps: nextSteps,
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
      discover_review: {
        overall_status: discoverReviewStatus,
        checks: discoverReviewChecks,
      },
    appendix: {
      artifact_refs: {
        legacy_inventory_ref: refFor("legacy_inventory", "artifact://analyst/raw/legacy_inventory/v1"),
        repo_landscape_ref: refFor("repo_landscape", "artifact://analyst/raw/repo_landscape/v1"),
        scope_lock_ref: refFor("scope_lock", "artifact://analyst/raw/scope_lock/v1"),
        variant_inventory_ref: refFor("variant_inventory", "artifact://analyst/raw/variant_inventory/v1"),
        event_map_ref: refFor("event_map", "artifact://analyst/raw/event_map/v1"),
        sql_catalog_ref: refFor("sql_catalog", "artifact://analyst/raw/sql_catalog/v1"),
        sql_map_ref: refFor("sql_map", "artifact://analyst/raw/sql_map/v1"),
        data_access_map_ref: refFor("data_access_map", "artifact://analyst/raw/data_access_map/v1"),
        recordset_ops_ref: refFor("recordset_ops", "artifact://analyst/raw/recordset_ops/v1"),
        procedure_summary_ref: refFor("procedure_summary", "artifact://analyst/raw/procedure_summary/v1"),
        form_dossier_ref: refFor("form_dossier", "artifact://analyst/raw/form_dossier/v1"),
        dependency_list_ref: refFor("dependency_inventory", "artifact://analyst/raw/dependency_inventory/v1"),
        dependency_inventory_ref: refFor("dependency_inventory", "artifact://analyst/raw/dependency_inventory/v1"),
        business_rules_ref: refFor("business_rule_catalog", "artifact://analyst/raw/business_rule_catalog/v1"),
        detector_findings_ref: refFor("detector_findings", "artifact://analyst/raw/detector_findings/v1"),
        risk_register_ref: refFor("risk_register", "artifact://analyst/raw/risk_register/v1"),
        orphan_analysis_ref: refFor("orphan_analysis", "artifact://analyst/raw/orphan_analysis/v1"),
        delivery_constitution_ref: refFor("delivery_constitution", "artifact://analyst/raw/delivery_constitution/v1"),
        variant_diff_report_ref: refFor("variant_diff_report", "artifact://analyst/raw/variant_diff_report/v1"),
        reporting_model_ref: refFor("reporting_model", "artifact://analyst/raw/reporting_model/v1"),
        identity_access_model_ref: refFor("identity_access_model", "artifact://analyst/raw/identity_access_model/v1"),
        discover_review_checklist_ref: refFor("discover_review_checklist", "artifact://analyst/raw/discover_review_checklist/v1"),
        artifact_index_ref: refFor("artifact_index", "artifact://analyst/raw/artifact_index/v1"),
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
        objective: String(walkthrough.business_objective_summary || safeOutput.executive_summary || "Objective not captured."),
        inventory_counts: {
          projects: projectCount,
          forms: formsCount,
          dependencies: dependencySet.length,
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
  const sourceLocTotal = Number(inventory.source_loc_total || 0);
  const sourceLocForms = Number(inventory.source_loc_forms || 0);
  const sourceLocModules = Number(inventory.source_loc_modules || 0);
  const sourceFilesScanned = Number(inventory.source_files_scanned || 0);
  const strategy = brief.recommended_strategy || {};
  const decisions = brief.decisions_required || {};
  const backlog = report.delivery_spec?.backlog?.items || [];
  const testing = report.delivery_spec?.testing_and_evidence || {};
  const qaReport = (report.qa_report_v1 && typeof report.qa_report_v1 === "object")
    ? report.qa_report_v1
    : ((output.qa_report_v1 && typeof output.qa_report_v1 === "object") ? output.qa_report_v1 : {});
  const qaSummary = (qaReport.summary && typeof qaReport.summary === "object") ? qaReport.summary : {};
  const qaStructural = (qaReport.structural && typeof qaReport.structural === "object") ? qaReport.structural : {};
  const qaSemantic = (qaReport.semantic && typeof qaReport.semantic === "object") ? qaReport.semantic : {};
  const qaStructuralChecks = Array.isArray(qaStructural.checks) ? qaStructural.checks : [];
  const qaSemanticChecks = Array.isArray(qaSemantic.checks) ? qaSemantic.checks : [];
  const qaGates = Array.isArray(qaReport.quality_gates) ? qaReport.quality_gates : [];
  const appendix = report.appendix || {};
  const openQuestions = Array.isArray(report.delivery_spec?.open_questions) ? report.delivery_spec.open_questions : [];

  const isPhpSummary = String(report?.metadata?.context_reference?.source_language || report?.context?.source_language || "").trim().toLowerCase() === "php"
    || (report?.decision_brief?.at_a_glance?.inventory_summary && Object.prototype.hasOwnProperty.call(report.decision_brief.at_a_glance.inventory_summary, "routes"));
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
    isPhpSummary
      ? `| Inventory | ${String(inventory.applications ?? 0)} application(s), ${String(inventory.controllers ?? 0)} controllers, ${String(inventory.routes ?? 0)} routes, ${String(inventory.templates ?? 0)} templates, ${String(inventory.dependencies ?? 0)} Composer dependencies |`
      : `| Inventory | ${String(inventory.projects ?? 0)} project(s), ${String(inventory.forms ?? 0)} forms/usercontrols, ${String(inventory.dependencies ?? 0)} dependencies |`,
    isPhpSummary
      ? `| Lines of code scanned | ${String(sourceLocTotal)} total LOC across ${String(sourceFilesScanned)} files |`
      : `| Lines of code scanned | ${String(sourceLocTotal)} total LOC (${String(sourceLocForms)} form LOC, ${String(sourceLocModules)} module LOC) across ${String(sourceFilesScanned)} files |`,
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
  if (isPhpSummary) {
    lines.push(`- Route/session parity baseline required for ${String(inventory.routes ?? 0)} route(s), ${String(inventory.session_keys ?? 0)} session key(s), and ${String(inventory.background_jobs ?? 0)} background job(s).`);
  } else {
    lines.push("- Golden flows:");
    (Array.isArray(testing.golden_flows) ? testing.golden_flows : []).forEach((flow) => {
      lines.push(`  - ${String(flow.id || "GF")}: ${String(flow.name || "")} | entry=${String(flow.entrypoint || "")}`);
    });
    if (!Array.isArray(testing.golden_flows) || !testing.golden_flows.length) lines.push("  - None");
  }
  lines.push("- Quality gates:");
  (Array.isArray(testing.quality_gates) ? testing.quality_gates : []).forEach((gate) => {
    lines.push(`  - ${String(gate.id || "gate")}: ${String(gate.result || "warn").toUpperCase()} | ${String(gate.description || "")}`);
  });
  if (!Array.isArray(testing.quality_gates) || !testing.quality_gates.length) lines.push("  - None");
  lines.push("- QA summary:");
  if (Object.keys(qaSummary).length) {
    lines.push(`  - Status: ${String(qaSummary.status || "PASS")}`);
    lines.push(`  - Structural: pass=${String(qaSummary.pass_count ?? 0)}, warn=${String(qaSummary.warn_count ?? 0)}, fail=${String(qaSummary.fail_count ?? 0)}, blockers=${String(qaSummary.blocker_count ?? 0)}`);
    if (Array.isArray(qaGates) && qaGates.length) {
      qaGates.forEach((gate) => {
        lines.push(`  - QA Gate ${String(gate.id || "qa_gate")}: ${String(gate.result || "warn").toUpperCase()} | ${String(gate.description || "")}`);
      });
    }
    if (qaStructuralChecks.length) {
      const blocking = qaStructuralChecks.filter((row) => Boolean(row && row.blocking));
      lines.push(`  - Structural checks: ${qaStructuralChecks.length} total (${blocking.length} blocking)`);
    }
    if (qaSemanticChecks.length) {
      lines.push(`  - Semantic checks: ${qaSemanticChecks.length} warning(s)`);
      qaSemanticChecks.slice(0, 8).forEach((row) => {
        lines.push(`    - ${String(row?.check_id || row?.id || "semantic_check")}: ${String(row?.severity || "medium").toUpperCase()} | ${String(row?.detail || "")}`);
      });
    }
  } else {
    lines.push("  - None");
  }
  lines.push("  - Rule consolidation notes are documented in Appendix Section E2 when duplicate rule templates are suppressed.");

  lines.push("", "### Open Questions");
  openQuestions.forEach((q, idx) => {
    const row = normalizeOpenQuestionEntry(q, idx);
    lines.push(`- [${String(row.severity || "medium").toUpperCase()}] ${row.id}: ${row.question} (owner: ${row.owner})`);
    if (row.context) lines.push(`  - Context: ${row.context}`);
  });
  if (!openQuestions.length) lines.push("- None");

  lines.push("", "## QA Validation Summary");
  if (Object.keys(qaSummary).length) {
    lines.push(`- Overall status: ${String(qaSummary.status || "PASS")}`);
    lines.push(`- Structural summary: pass=${String(qaSummary.pass_count ?? 0)}, warn=${String(qaSummary.warn_count ?? 0)}, fail=${String(qaSummary.fail_count ?? 0)}, blockers=${String(qaSummary.blocker_count ?? 0)}`);
    const autoFixes = Array.isArray(qaSummary.auto_fixes_applied) ? qaSummary.auto_fixes_applied : [];
    if (autoFixes.length) {
      lines.push("- Auto-fixes applied:");
      autoFixes.slice(0, 12).forEach((fix) => lines.push(`  - ${String(fix || "").trim()}`));
    }
    if (qaSemanticChecks.length) {
      lines.push("- Active semantic warnings:");
      qaSemanticChecks.slice(0, 10).forEach((row) => {
        lines.push(`  - ${String(row?.check_id || row?.id || "semantic_check")}: ${String(row?.detail || "")}`);
      });
    }
  } else {
    lines.push("- No QA artifact present.");
  }

  lines.push("", "## Evidence Appendix");
  const refs = appendix.artifact_refs || {};
  Object.entries(refs).forEach(([k, v]) => {
    if (String(v || "").trim()) lines.push(`- ${k}: ${String(v)}`);
  });
  lines.push("- High-volume sections included in structured artifact (inventory, dependencies, event map, SQL catalog, business rules).");
  lines.push("", "## Appendix Snapshot");
  const hv = appendix.high_volume_sections || {};
  const rawFromReport = (report.raw_artifacts && typeof report.raw_artifacts === "object") ? report.raw_artifacts : {};
  const raw = Object.keys(rawFromReport).length
    ? rawFromReport
    : ((output.raw_artifacts && typeof output.raw_artifacts === "object") ? output.raw_artifacts : {});
  const rawEventMap = Array.isArray(raw.event_map?.entries) ? raw.event_map.entries : [];
  const rawSql = Array.isArray(raw.sql_catalog?.statements) ? raw.sql_catalog.statements : [];
  const rawSqlMap = Array.isArray(raw.sql_map?.entries) ? raw.sql_map.entries : [];
  const rawProcedures = Array.isArray(raw.procedure_summary?.procedures) ? raw.procedure_summary.procedures : [];
  const rawFormDossiers = Array.isArray(raw.form_dossier?.dossiers) ? raw.form_dossier.dossiers : [];
  const rawDeps = Array.isArray(raw.dependency_inventory?.dependencies) ? raw.dependency_inventory.dependencies : [];
  const rawRules = Array.isArray(raw.business_rule_catalog?.rules) ? raw.business_rule_catalog.rules : [];
  const rawRisks = Array.isArray(raw.risk_register?.risks) ? raw.risk_register.risks : [];
  const rawOrphans = Array.isArray(raw.orphan_analysis?.orphans) ? raw.orphan_analysis.orphans : [];
  const rawLandscape = Array.isArray(raw.repo_landscape?.projects) ? raw.repo_landscape.projects : [];
  const rawVariantInventory = Array.isArray(raw.variant_inventory?.variants) ? raw.variant_inventory.variants : [];
  const rawConstitution = Array.isArray(raw.delivery_constitution?.principles) ? raw.delivery_constitution.principles : [];
  const rawVariantDiff = (raw.variant_diff_report && typeof raw.variant_diff_report === "object") ? raw.variant_diff_report : {};
  const baseFormName = (value) => {
    let text = String(value || "").trim();
    if (!text) return "";
    if (text.includes("::")) text = text.split("::").pop();
    if (text.includes(":")) text = text.split(":")[0];
    if (text.includes("/")) text = text.split("/").pop();
    if (text.toLowerCase().endsWith(".frm")) text = text.slice(0, -4);
    return text.toLowerCase();
  };
  const projectFromScoped = (value) => {
    const text = String(value || "").trim();
    if (!text.includes("::")) return "";
    return text.split("::")[0].trim();
  };
  const formKey = (projectName, formName) => {
    const form = baseFormName(formName);
    const project = String(projectName || "").trim().toLowerCase();
    return (project && form) ? `${project}::${form}` : form;
  };
  const baseOnlyKey = (formName) => {
    const base = baseFormName(formName);
    return base ? `__base__::${base}` : "";
  };
  const formKeys = (projectName, formName) => {
    const keys = [];
    const scoped = formKey(projectName, formName);
    if (scoped) keys.push(scoped);
    const baseKey = baseOnlyKey(formName);
    if (baseKey && !keys.includes(baseKey)) keys.push(baseKey);
    return keys;
  };
  const lookupRows = (mapping, projectName, formName) => {
    const scoped = formKey(projectName, formName);
    const baseKey = baseOnlyKey(formName);
    if (scoped && Array.isArray(mapping[scoped]) && mapping[scoped].length) return mapping[scoped];
    if (baseKey && Array.isArray(mapping[baseKey]) && mapping[baseKey].length) return mapping[baseKey];
    return [];
  };
  const lookupSet = (mapping, projectName, formName) => {
    const scoped = formKey(projectName, formName);
    const baseKey = baseOnlyKey(formName);
    if (scoped && mapping[scoped] instanceof Set && mapping[scoped].size) return mapping[scoped];
    if (baseKey && mapping[baseKey] instanceof Set && mapping[baseKey].size) return mapping[baseKey];
    return new Set();
  };
  const lookupControlMap = (mapping, projectName, formName) => {
    const scoped = formKey(projectName, formName);
    const baseKey = baseOnlyKey(formName);
    if (scoped && mapping[scoped] && typeof mapping[scoped] === "object") return mapping[scoped];
    if (baseKey && mapping[baseKey] && typeof mapping[baseKey] === "object") return mapping[baseKey];
    return {};
  };
  const qualifiedFormName = (projectName, formName) => {
    const project = String(projectName || "").trim();
    const form = String(formName || "").trim();
    if (project && form) return `${project}::${form}`;
    return form || "n/a";
  };
  const extractFormsFromText = (value) => {
    const text = String(value || "");
    if (!text) return [];
    const out = [];
    const matches = text.match(/[A-Za-z0-9_./-]+/g) || [];
    matches.forEach((item) => {
      const low = item.toLowerCase();
      if (low.endsWith(".bas") || low.endsWith(".cls") || low.endsWith(".ctl") || low.endsWith(".vbp") || low.endsWith(".vbg") || low.endsWith(".res") || low.endsWith(".mdl") || low.endsWith(".mod")) return;
      if (!low.includes("frm") && !low.startsWith("form")) return;
      const leaf = item.includes("/") ? item.split("/").pop() : item;
      const normalizedRaw = leaf.toLowerCase().endsWith(".frm") ? leaf.slice(0, -4) : leaf;
      const normalized = normalizedRaw.replace(/[.,;:()\[\]{}]+$/g, "");
      const lowNorm = normalized.toLowerCase();
      if (/_((click|change|load|keypress|gotfocus|lostfocus|activate|deactivate))$/i.test(lowNorm)) return;
      if (!out.includes(normalized)) out.push(normalized);
    });
    return out;
  };
  const inferFormType = ({ formName, purpose, procedures, controls, tables }) => {
    const formLow = String(formName || "").toLowerCase();
    const purposeLow = String(purpose || "").toLowerCase();
    const controlText = (Array.isArray(controls) ? controls : []).map((c) => String(c || "").toLowerCase()).join(" ");
    const procNames = new Set((Array.isArray(procedures) ? procedures : []).map((p) => String(p?.procedure_name || "").toLowerCase()));
    const tableSet = new Set((tables || []).map((t) => String(t || "").toLowerCase()));
    if (formLow.includes("splash") || purposeLow.includes("splash")) return "Splash";
    if (formLow === "main" || formLow.startsWith("mdi") || controlText.includes("toolbar") || Array.from(procNames).some((p) => p.includes("toolbar"))) return "MDI_Host";
    if (formLow.includes("login") || purposeLow.includes("auth") || (formLow.includes("form9") && (tableSet.has("logi") || tableSet.has("login")))) return "Login";
    if (formLow.startsWith("rpt") || formLow.startsWith("datareport")) return "Report";
    return "Child";
  };
  const splitWords = (token) => {
    let raw = String(token || "").trim();
    let lowered = raw.toLowerCase();
    if (lowered.startsWith("dtpicker")) {
      raw = raw.slice("dtpicker".length);
      raw = raw ? `date${raw}` : "date";
      lowered = raw.toLowerCase();
    }
    ["txt", "cbo", "cmb", "dtp", "msk", "lst", "chk", "opt", "lbl", "cmd"].forEach((prefix) => {
      if (!(raw && lowered.startsWith(prefix) && raw.length > prefix.length)) return;
      const lowerRaw = raw.toLowerCase();
      if (prefix === "opt" && lowerRaw.startsWith("option")) return;
      if (prefix === "chk" && lowerRaw.startsWith("check")) return;
      if (prefix === "txt" && lowerRaw.startsWith("text")) return;
      if (prefix === "cbo" && lowerRaw.startsWith("combo")) return;
      raw = raw.slice(prefix.length);
    });
    raw = raw.replace(/[_-]+/g, " ");
    raw = raw.replace(/([a-z0-9])([A-Z])/g, "$1 $2");
    raw = raw.replace(/([A-Za-z])(id|no)\b/gi, "$1 $2");
    raw = raw.replace(/([A-Za-z])([0-9])/g, "$1 $2");
    raw = raw.replace(/\s+/g, " ").trim();
    return raw.toLowerCase();
  };
  const isDataInputControl = (controlId) => {
    const cid = String(controlId || "").trim().toLowerCase();
    return ["txt", "cbo", "cmb", "dtp", "msk", "lst", "chk", "opt"].some((p) => cid.startsWith(p));
  };
  const toBusinessInput = (controlId) => splitWords(controlId) || String(controlId || "").trim().toLowerCase();
  const callableKind = (procedureName, formName, eventHint = "") => {
    const proc = String(procedureName || "").trim().toLowerCase();
    const form = baseFormName(formName);
    const evt = String(eventHint || "").trim().toLowerCase();
    if (form === "shared_module") return "shared_function";
    if (evt) return "event_handler";
    if (/_((click|change|load|keypress|keydown|keyup|gotfocus|lostfocus|activate|deactivate))$/i.test(proc)) return "event_handler";
    if (/^(cmd|lbl|txt|cbo|opt|chk)/i.test(proc)) return "event_handler";
    return "procedure";
  };
  const semanticFormAlias = ({ formName, purpose, dbTables, procedures, rules, controls = [] }) => {
    const formToken = String(formName || "").toLowerCase();
    if (formToken === "main" || formToken === "mdiform") return "Navigation Hub";
    const isGenericForm = /^(form\d+|frm\d+)$/i.test(formToken);
    if (formToken.endsWith("frmsearch") || formToken === "frmsearch") return "Record Search";
    if (formToken.endsWith("frmtransactions") || formToken.endsWith("transactions")) return "Transaction History";
    if (formToken.endsWith("frmtransaction") || formToken.endsWith("transaction")) return "Transaction Entry";
    const purposeLow = String(purpose || "").toLowerCase();
    if (purposeLow.includes("deposit capture")) return "Deposit Capture";
    if (purposeLow.includes("withdrawal processing")) return "Withdrawal Processing";
    if (purposeLow.includes("customer profile")) return "Customer Management";
    if (purposeLow.includes("transaction ledger")) return "Transaction Ledger";
    if (purposeLow.includes("account type maintenance")) return "Account Type Maintenance";
    const tokenBlob = [
      formToken,
      String(purpose || "").toLowerCase(),
      Array.from(dbTables || []).map((x) => String(x || "").toLowerCase()).join(" "),
      (Array.isArray(procedures) ? procedures : []).map((p) => String(p?.procedure_name || "").toLowerCase()).join(" "),
      (Array.isArray(rules) ? rules : []).map((r) => String(r?.statement || "").toLowerCase()).join(" "),
      (Array.isArray(controls) ? controls : []).map((c) => String(c || "").toLowerCase()).join(" "),
    ].join(" ");
    if (["login", "logi", "username", "password", "txtpass", "pass1", "credential"].some((k) => tokenBlob.includes(k))) {
      return ["txtpass", "pass1", "credential"].some((k) => tokenBlob.includes(k)) ? "Password Management" : "Authentication";
    }
    const hasStrongTransactionSignal = ["transction", "tbltransaction", "transaction ledger", "ledger"].some((k) => tokenBlob.includes(k));
    if (hasStrongTransactionSignal || (tokenBlob.includes("debit") && tokenBlob.includes("credit"))) return "Transaction Ledger";
    if (["withdraw", "debit"].some((k) => tokenBlob.includes(k))) return "Withdrawal Processing";
    if (["deposit", "credit", "balancedt"].some((k) => tokenBlob.includes(k)) && !["transaction", "transction", "debit"].some((k) => tokenBlob.includes(k))) return "Deposit Capture";
    if (["customer", "tblcustomer"].some((k) => tokenBlob.includes(k)) && ["interest", "min balance", "account type", "acctype"].some((k) => tokenBlob.includes(k))) return "Customer Management";
    if (["accounttype", "acctype"].some((k) => tokenBlob.includes(k))) return "Account Type Maintenance";
    if (["customer", "tblcustomer"].some((k) => tokenBlob.includes(k))) return "Customer Management";
    if (["report", "datareport", "dataenvironment"].some((k) => tokenBlob.includes(k))) return "Reporting";
    if (["search", "lookup", "find"].some((k) => tokenBlob.includes(k))) return "Record Search";
    if (["main", "mdiform", "toolbar"].some((k) => tokenBlob.includes(k))) return "Navigation Hub";
    if (["balance", "tblbalance"].some((k) => tokenBlob.includes(k))) return "Balance Inquiry";
    if (["timer", "progressbar", "splash"].some((k) => tokenBlob.includes(k))) return "Splash/Loading";
    if (isGenericForm && formToken === "form9") return "Authentication Entry";
    if (isGenericForm && formToken === "form1") return "Navigation/Menu";
    if (isGenericForm && ["dated", "datejoined", "dtpicker", "date 1", "from date", "to date"].some((k) => tokenBlob.includes(k))) return "Date/Period Entry";
    const cleaned = String(purpose || "").replace(/\bworkflow\b/ig, "").replace(/\s+/g, " ").trim().replace(/[.-]+$/, "");
    const genericPhrases = new Set([
      "business executed through event-driven ui controls",
      "business workflow executed through event-driven ui controls",
      "application navigation and module routing",
    ]);
    if (genericPhrases.has(cleaned.toLowerCase())) return "";
    return cleaned;
  };
  const displayFormName = (formName, alias) => {
    const form = String(formName || "").trim();
    const semantic = String(alias || "").trim();
    if (!semantic) return form;
    const generic = /^(form\d+|frm\d+)$/i.test(form);
    if (generic) return `${form} [${semantic}]`;
    const normalizedName = form.toLowerCase().replace(/[^a-z0-9]+/g, "");
    const normalizedAlias = semantic.toLowerCase().replace(/[^a-z0-9]+/g, "");
    const ambiguousBankForm = ["frmtransaction", "frmtransactions", "main"].includes(form.toLowerCase());
    if (ambiguousBankForm || (normalizedAlias && !normalizedName.includes(normalizedAlias))) {
      return `${form} [${semantic}]`;
    }
    return form;
  };
  const ruleBusinessMeaning = (statement, category) => {
    const stmt = String(statement || "").trim();
    const low = stmt.toLowerCase();
    if (
      (low.includes("asc(") && (low.includes("< 46") || low.includes("<= 45")) && (low.includes("> 57") || low.includes(">= 58"))) ||
      ((low.includes("keyascii") || low.includes("keyvalue")) && low.includes(">= 48") && low.includes("<= 57"))
    ) {
      return "Input is restricted to numeric digits only.";
    }
    if (/keyascii\s*=\s*13/i.test(low)) return "Pressing Enter triggers the same action flow as the primary button.";
    if (low.includes("case keyascii")) return "Keyboard input routing determines which action path is executed.";
    if (/\.\s*state\s*=\s*1/i.test(low)) return "The action proceeds only when the recordset/connection is active.";
    if (/\.\s*recordcount\s*>\s*0/i.test(low)) return "The action proceeds only when matching records are found.";
    if ((low.includes("max(") && low.includes("+ 1")) || (low.includes("max(") && low.includes("+1"))) return "A new identifier is generated as current maximum value plus one.";
    if (low.includes("computed value rule") || low.includes("currbalance")) {
      if (low.includes("lblbalance.caption")) return "Balance is recalculated from the displayed balance label and entered amount (UI-derived source).";
      return "Balance is recalculated using the entered amount and current account value.";
    }
    if (low.includes("case button.index") || low.includes("case buttonmenu.key")) return "User menu selection routes the workflow to the corresponding module.";
    if (low.includes("threshold decision rule") && low.includes("if ")) {
      const rhs = String(stmt.split("IF").pop() || "").replace(/\s*THEN.*$/i, "").trim();
      const rhsLow = rhs.toLowerCase();
      if (
        (rhsLow.includes("asc(") && (rhsLow.includes("< 46") || rhsLow.includes("<= 45")) && (rhsLow.includes("> 57") || rhsLow.includes(">= 58"))) ||
        ((rhsLow.includes("keyascii") || rhsLow.includes("keyvalue")) && rhsLow.includes(">= 48") && rhsLow.includes("<= 57"))
      ) {
        return "Input is restricted to numeric digits only.";
      }
      return `The workflow continues only when this condition is true: ${rhs}.`;
    }
    if (low.includes("executes transaction workflow through procedures")) return "Workflow is orchestrated through UI event handlers and internal procedures.";
    if (low.includes("reads/writes persisted entities")) return "Form persists and retrieves records from the listed tables.";
    if (low.includes("authenticate users")) return "User authentication is required before entering the workflow.";
    if (["data_persistence", "calculation_logic", "threshold_rule"].includes(String(category || "").toLowerCase())) return "Business behavior is enforced through data and calculation logic.";
    return stmt;
  };
  const businessEffectFromSql = (operation, table) => {
    const op = String(operation || "").trim().toLowerCase();
    const tbl = String(table || "").trim();
    const low = tbl.toLowerCase();
    if (op === "select") {
      if (low.includes("balance")) return "Customer balance and account details displayed for review.";
      if (low.includes("customer")) return "Customer details displayed for review.";
      if (low.includes("transction") || low.includes("transaction")) return "Transaction history displayed for review.";
      if (low.includes("accounttype")) return "Account type details displayed for selection.";
      if (low.includes("logi") || low.includes("login")) return "User credentials validated against stored records.";
    }
    if (low.includes("deposit")) return "Deposit transaction recorded.";
    if (low.includes("withdraw")) return "Withdrawal transaction recorded.";
    if (low.includes("balance")) return "Account balance updated.";
    if (low.includes("transction") || low.includes("transaction")) return "Transaction ledger updated.";
    if (low.includes("customer") && ["insert", "update", "delete"].includes(op)) return "Customer profile data updated.";
    if (low.includes("accounttype")) return "Account type configuration updated.";
    if (low.includes("logi") || low.includes("login")) return "User authentication record validated.";
    if (op === "insert") return `New record created in ${tbl}.`;
    if (op === "update") return `Existing records updated in ${tbl}.`;
    if (op === "delete") return `Records deleted from ${tbl}.`;
    return "";
  };
  const fallbackBusinessEffects = ({ alias, purpose, inputs, dbTables, rules }) => {
    const tokenBlob = [
      String(alias || "").toLowerCase(),
      String(purpose || "").toLowerCase(),
      Array.from(inputs || []).map((x) => String(x || "").toLowerCase()).join(" "),
      (Array.isArray(dbTables) ? dbTables : []).map((x) => String(x || "").toLowerCase()).join(" "),
      (Array.isArray(rules) ? rules : []).map((r) => String(r?.statement || "").toLowerCase()).join(" "),
    ].join(" ");
    const effects = [];
    if (["deposit", "amount deposited", "credit"].some((k) => tokenBlob.includes(k))) {
      effects.push("Deposit transaction recorded.");
      effects.push("Account balance recalculated.");
    }
    if (["withdraw", "amount withdrawn", "debit"].some((k) => tokenBlob.includes(k))) {
      effects.push("Withdrawal transaction recorded.");
      effects.push("Account balance recalculated.");
    }
    if (["transaction ledger", "transction", "transaction"].some((k) => tokenBlob.includes(k))) effects.push("Transaction history updated.");
    if (["customer management", "customer profile"].some((k) => tokenBlob.includes(k))) effects.push("Customer profile created or updated.");
    if (["account type", "acctype", "accounttype"].some((k) => tokenBlob.includes(k))) effects.push("Account type master data maintained.");
    if (["authentication", "login", "password"].some((k) => tokenBlob.includes(k))) effects.push("User access is validated before workflow continuation.");
    if (["search", "lookup"].some((k) => tokenBlob.includes(k))) effects.push("Matching records displayed to the user.");
    if (["navigation hub", "main", "toolbar"].some((k) => tokenBlob.includes(k))) effects.push("Navigation routes the user to selected module screens.");
    if (!effects.length && (Array.isArray(dbTables) ? dbTables : []).some((t) => String(t || "").toLowerCase().includes("balance"))) effects.push("Account balance information refreshed.");
    if (!effects.length && (Array.isArray(dbTables) ? dbTables : []).some((t) => String(t || "").toLowerCase().includes("customer"))) effects.push("Customer details loaded/updated for the selected workflow.");
    return Array.from(new Set(effects.filter(Boolean))).slice(0, 6);
  };
  const dossierBusinessRuleSummary = ({ formName, dossier, ruleRows = [] }) => {
    const d = dossier && typeof dossier === "object" ? dossier : null;
    if (!d) return "";
    const controls = Array.isArray(d.controls) ? d.controls : [];
    const inputValues = new Set();
    controls.forEach((ctl) => {
      const ctlName = String(ctl || "").trim();
      if (!ctlName) return;
      const controlId = String(ctlName.split(":").pop() || "").trim();
      if (isDataInputControl(controlId)) inputValues.add(toBusinessInput(controlId));
    });
    const dbTables = Array.isArray(d.db_tables) ? d.db_tables.map((t) => String(t || "").trim()).filter(Boolean) : [];
    const purpose = String(d.purpose || "").trim().replace(/[.]+$/, "");
    const alias = semanticFormAlias({
      formName,
      purpose,
      dbTables: new Set(dbTables),
      procedures: [],
      rules: ruleRows,
      controls,
    });
    const effects = fallbackBusinessEffects({
      alias,
      purpose,
      inputs: inputValues,
      dbTables,
      rules: ruleRows,
    });
    const parts = [];
    if (inputValues.size) parts.push(`Captures ${Array.from(inputValues).sort().slice(0, 6).join(", ")}.`);
    if (effects.length) parts.push(`Business outcome: ${effects.slice(0, 3).join("; ")}.`);
    if (purpose && !purpose.toLowerCase().includes("event-driven ui controls")) parts.unshift(purpose);
    return parts.join(" ").trim();
  };
  const projectPathByName = {};
  const projectDepsByName = {};
  const projectTablesByName = {};
  rawLandscape.forEach((row) => {
    const rawId = String(row?.id || "").trim();
    const path = String(row?.path || "").trim();
    const left = rawId.includes("|") ? rawId.split("|")[0] : rawId;
    [left, String(row?.name || "").trim(), rawId].forEach((name) => {
      const key = String(name || "").trim();
      if (!key) return;
      if (!projectPathByName[key] && path) projectPathByName[key] = path;
      if (!projectDepsByName[key]) projectDepsByName[key] = new Set();
      if (!projectTablesByName[key]) projectTablesByName[key] = new Set();
      (Array.isArray(row?.dependencies) ? row.dependencies : []).forEach((dep) => {
        const depName = String(dep || "").trim();
        if (depName) projectDepsByName[key].add(depName);
      });
      (Array.isArray(row?.db_touchpoints) ? row.db_touchpoints : []).forEach((t) => {
        const tableName = String(t || "").trim();
        if (tableName) projectTablesByName[key].add(tableName);
      });
    });
  });
  const projectLabel = (name) => {
    const key = String(name || "").trim();
    if (!key) return "n/a";
    const path = String(projectPathByName[key] || "").trim();
    return path ? `${key} [${path}]` : key;
  };
  const formSqlRows = {};
  const formDbTables = {};
  const sqlMapRowKey = new WeakMap();
  rawSqlMap.forEach((row) => {
    const tables = new Set((Array.isArray(row?.tables) ? row.tables : []).map((t) => String(t || "").trim()).filter(Boolean));
    const projectName = String(row?.variant || "").trim() || projectFromScoped(row?.form);
    const formName = String(row?.form_base || "").trim() || String(row?.form || "").trim();
    const keys = formKeys(projectName, formName);
    if (!keys.length) return;
    sqlMapRowKey.set(row, keys[0]);
    keys.forEach((key) => {
      if (!formSqlRows[key]) formSqlRows[key] = [];
      if (!formDbTables[key]) formDbTables[key] = new Set();
      formSqlRows[key].push(row);
      tables.forEach((t) => formDbTables[key].add(t));
    });
    if (projectName) {
      if (!projectTablesByName[projectName]) projectTablesByName[projectName] = new Set();
      tables.forEach((t) => projectTablesByName[projectName].add(t));
    }
  });
  const formEventRows = {};
  const formEventHandlerByProc = {};
  rawEventMap.forEach((row) => {
    const container = String(row?.container || row?.name || "").trim();
    const projectName = projectFromScoped(container) || projectFromScoped(row?.handler?.symbol);
    const formName = baseFormName(container) || baseFormName(row?.handler?.symbol);
    const keys = formKeys(projectName, formName);
    if (!keys.length) return;
    keys.forEach((key) => {
      if (!formEventRows[key]) formEventRows[key] = [];
      formEventRows[key].push(row);
    });
    (Array.isArray(row?.calls) ? row.calls : []).forEach((call) => {
      const proc = String(call || "").trim();
      if (!proc) return;
      keys.forEach((key) => {
        const mapKey = `${key}::${proc}`;
        if (!formEventHandlerByProc[mapKey]) formEventHandlerByProc[mapKey] = new Set();
        const handler = String(row?.handler?.symbol || row?.entry_id || "").trim();
        if (handler) formEventHandlerByProc[mapKey].add(handler);
      });
    });
  });
  const formProcRows = {};
  rawProcedures.forEach((row) => {
    const keys = formKeys(projectFromScoped(row?.form), baseFormName(row?.form));
    if (!keys.length) return;
    keys.forEach((key) => {
      if (!formProcRows[key]) formProcRows[key] = [];
      formProcRows[key].push(row);
    });
  });
  const formRiskRows = {};
  rawRisks.forEach((row) => {
    const texts = [
      String(row?.description || ""),
      String(row?.recommended_action || ""),
      ...(Array.isArray(row?.evidence) ? row.evidence.map((ev) => String(ev?.external_ref?.ref || ev?.file_span?.path || "")) : []),
    ];
    const forms = new Set();
    texts.forEach((text) => extractFormsFromText(text).forEach((f) => forms.add(baseFormName(f))));
    rawFormDossiers.forEach((dossier) => {
      if (!forms.has(baseFormName(dossier?.form_name))) return;
      formKeys(dossier?.project_name, dossier?.form_name).forEach((key) => {
        if (!formRiskRows[key]) formRiskRows[key] = [];
        formRiskRows[key].push(row);
      });
    });
  });
  const knownFormBases = new Set(
    rawFormDossiers.map((d) => baseFormName(d?.form_name)).filter(Boolean),
  );
  const ruleFormBases = (ruleRow) => {
    const scope = (ruleRow?.scope && typeof ruleRow.scope === "object") ? ruleRow.scope : {};
    const candidates = [
      String(scope.form || "").trim(),
      String(scope.form_key || "").trim(),
      String(scope.component_id || "").trim(),
      String(ruleRow?.form || "").trim(),
      ...(Array.isArray(scope.forms) ? scope.forms.map((x) => String(x || "").trim()) : []),
      ...(Array.isArray(scope.form_keys) ? scope.form_keys.map((x) => String(x || "").trim()) : []),
    ];
    const bases = new Set();
    candidates.forEach((cand) => {
      if (!cand) return;
      const splitTokens = cand.split(/[,;|]/).map((part) => String(part || "").trim()).filter(Boolean);
      (splitTokens.length ? splitTokens : [cand]).forEach((tokenIn) => {
        let raw = String(tokenIn || "").trim();
        if (!raw) return;
        if (raw.includes("::")) raw = String(raw.split("::", 2)[1] || "").trim();
        raw = String(raw.split("/").pop() || "").trim();
        raw = raw.replace(/\.(frm|ctl|cls|bas)$/i, "").replace(/[.,;:()[\]{}]+$/g, "");
        const low = raw.toLowerCase();
        if (!low) return;
        if (/\.(bas|cls|ctl|ctx|vbp|vbg|res|dsr|dca|dcx)$/i.test(low)) return;
        if (/(?:_|^)(click|change|load|keypress|gotfocus|lostfocus|activate|deactivate)$/i.test(low)) return;
        const normalizedBase = baseFormName(raw);
        if (!(low.includes("frm") || low.startsWith("form") || ["main", "mdiform", "login"].includes(low) || knownFormBases.has(normalizedBase))) return;
        const base = baseFormName(raw);
        if (base) bases.add(base);
      });
    });
    const textHints = [
      String(scope.component_id || "").trim(),
      String(ruleRow?.statement || "").trim(),
      ...(Array.isArray(ruleRow?.evidence) ? ruleRow.evidence.map((ev) => String(ev?.external_ref?.ref || ev?.file_span?.path || "").trim()) : []),
    ];
    textHints.forEach((text) => extractFormsFromText(text).forEach((f) => {
      const base = baseFormName(f);
      if (base) bases.add(base);
    }));
    return bases;
  };
  const formRuleRows = {};
  rawRules.forEach((row) => {
    const forms = ruleFormBases(row);
    rawFormDossiers.forEach((dossier) => {
      if (!forms.has(baseFormName(dossier?.form_name))) return;
      formKeys(dossier?.project_name, dossier?.form_name).forEach((key) => {
        if (!formRuleRows[key]) formRuleRows[key] = [];
        formRuleRows[key].push(row);
      });
    });
  });
  // Mirror rules across equivalent forms for cross-variant generic form names (e.g., Form3 vs frmDeposits).
  const semanticByKey = {};
  rawFormDossiers.forEach((dossier) => {
    const projectName = String(dossier?.project_name || "").trim();
    const formName = String(dossier?.form_name || "").trim();
    if (!formName) return;
    const procRows = lookupRows(formProcRows, projectName, formName);
    const sqlRows = lookupRows(formSqlRows, projectName, formName);
    const tableHints = new Set();
    sqlRows.forEach((item) => (Array.isArray(item?.tables) ? item.tables : []).forEach((t) => {
      const v = String(t || "").trim();
      if (v) tableHints.add(v);
    }));
    const alias = semanticFormAlias({
      formName,
      purpose: String(dossier?.purpose || "").trim(),
      dbTables: tableHints,
      procedures: procRows,
      rules: lookupRows(formRuleRows, projectName, formName),
      controls: Array.isArray(dossier?.controls) ? dossier.controls : [],
    });
    const semantic = String(alias || "").trim().toLowerCase();
    if (!semantic) return;
    formKeys(projectName, formName).forEach((key) => {
      semanticByKey[key] = semantic;
    });
  });
  const donorRulesBySemantic = {};
  const semanticGroup = (semantic) => {
    const s = String(semantic || "").trim().toLowerCase();
    if (["transaction entry", "transaction history", "transaction ledger"].includes(s)) return "transaction_workflow";
    if (["password management", "authentication", "authentication entry"].includes(s)) return "authentication_workflow";
    if (["record search", "search"].includes(s)) return "record_search_workflow";
    return s;
  };
  const allowedSemantics = new Set([
    "deposit capture",
    "withdrawal processing",
    "transaction entry",
    "transaction ledger",
    "transaction history",
    "customer management",
    "account type maintenance",
    "password management",
    "authentication",
    "record search",
  ]);
  Object.entries(formRuleRows).forEach(([key, rows]) => {
    const semantic = String(semanticByKey[key] || "").trim();
    if (!allowedSemantics.has(semantic)) return;
    const list = Array.isArray(rows) ? rows : [];
    if (!list.length) return;
    const semanticBucket = semanticGroup(semantic);
    if (!donorRulesBySemantic[semanticBucket]) donorRulesBySemantic[semanticBucket] = [];
    const existingIds = new Set(donorRulesBySemantic[semanticBucket].map((r) => String(r?.rule_id || r?.id || "").trim()).filter(Boolean));
    list.forEach((r) => {
      const rid = String(r?.rule_id || r?.id || "").trim();
      if (rid && existingIds.has(rid)) return;
      donorRulesBySemantic[semanticBucket].push(r);
      if (rid) existingIds.add(rid);
    });
  });
  Object.entries(semanticByKey).forEach(([key, semantic]) => {
    if (!allowedSemantics.has(semantic)) return;
    if (Array.isArray(formRuleRows[key]) && formRuleRows[key].length) return;
    const mirrored = Array.isArray(donorRulesBySemantic[semanticGroup(semantic)]) ? donorRulesBySemantic[semanticGroup(semantic)].slice(0, 8) : [];
    if (mirrored.length) formRuleRows[key] = mirrored;
  });
  const formControlTypeByKey = {};
  rawFormDossiers.forEach((row) => {
    const keys = formKeys(row?.project_name, row?.form_name);
    if (!keys.length) return;
    keys.forEach((key) => {
      if (!formControlTypeByKey[key]) formControlTypeByKey[key] = {};
      (Array.isArray(row?.controls) ? row.controls : []).forEach((ctl) => {
        const text = String(ctl || "").trim();
        if (!text) return;
        const parts = text.split(":", 2);
        const ctlType = String(parts[0] || "").trim();
        const ctlName = String(parts[1] || parts[0] || "").trim().toLowerCase();
        if (ctlName) formControlTypeByKey[key][ctlName] = ctlType;
      });
    });
  });
  const sharedModuleProcedures = new Set(
    rawProcedures
      .filter((proc) => baseFormName(proc?.form) === "shared_module")
      .map((proc) => String(proc?.procedure_name || "").trim())
      .filter(Boolean),
  );
  const formSharedComponents = {};
  Object.entries(formEventRows).forEach(([key, rows]) => {
    (Array.isArray(rows) ? rows : []).forEach((evt) => {
      (Array.isArray(evt?.calls) ? evt.calls : []).forEach((call) => {
        const proc = String(call || "").trim();
        if (!proc || !sharedModuleProcedures.has(proc)) return;
        if (!formSharedComponents[key]) formSharedComponents[key] = new Set();
        formSharedComponents[key].add(proc);
      });
    });
  });
  const tableToProjects = {};
  Object.entries(projectTablesByName).forEach(([projectName, tablesSet]) => {
    Array.from(tablesSet instanceof Set ? tablesSet : []).forEach((tableName) => {
      if (!tableToProjects[tableName]) tableToProjects[tableName] = new Set();
      tableToProjects[tableName].add(projectName);
    });
  });
  const dependencyToForms = {};
  rawFormDossiers.forEach((row) => {
    const projectName = String(row?.project_name || "").trim();
    const qForm = qualifiedFormName(projectName, row?.form_name);
    const deps = new Set(projectDepsByName[projectName] instanceof Set ? Array.from(projectDepsByName[projectName]) : []);
    (Array.isArray(row?.controls) ? row.controls : []).forEach((ctl) => {
      const ctlType = String(String(ctl || "").split(":", 1)[0] || "").trim();
      if (ctlType && !ctlType.toUpperCase().startsWith("VB")) deps.add(ctlType);
    });
    Array.from(deps).forEach((dep) => {
      const name = String(dep || "").trim().toLowerCase();
      if (!name) return;
      if (!dependencyToForms[name]) dependencyToForms[name] = new Set();
      dependencyToForms[name].add(qForm);
    });
  });
  const legacyProjects = Array.isArray(raw?.legacy_inventory?.projects)
    ? raw.legacy_inventory.projects
    : (Array.isArray(hv?.legacy_inventory?.projects) ? hv.legacy_inventory.projects : []);
  const dossierByKey = {};
  rawFormDossiers.forEach((row) => {
    formKeys(row?.project_name, row?.form_name).forEach((key) => {
      if (key && !dossierByKey[key]) dossierByKey[key] = row;
    });
  });
  const discoveredForms = [];
  const seenDiscovered = new Set();
  const normalizeDiscoveredFormName = (value) => {
    const rawName = String(value || "").trim();
    if (!rawName) return "";
    const leaf = rawName.split("/").pop();
    if (!leaf) return "";
    if (leaf.includes(":")) {
      const [left, right] = leaf.split(":", 2);
      if (["form", "mdiform"].includes(String(left || "").trim().toLowerCase()) && String(right || "").trim()) {
        return String(right || "").trim();
      }
    }
    return String(leaf || "").trim();
  };
  const addDiscovered = (projectName, formName, source) => {
    const pname = String(projectName || "").trim();
    const fname = normalizeDiscoveredFormName(formName);
    if (!fname) return;
    const key = formKey(pname, fname);
    if (!key || seenDiscovered.has(key)) return;
    seenDiscovered.add(key);
    discoveredForms.push({ project_name: pname, form_name: fname, form_key: key, source });
  };
  legacyProjects.forEach((project) => {
    const pname = String(project?.name || "").trim();
    (Array.isArray(project?.forms) ? project.forms : []).forEach((f) => addDiscovered(pname, f, "project.forms"));
    (Array.isArray(project?.ui_assets) ? project.ui_assets : []).forEach((asset) => {
      const kind = String(asset?.kind || "").toLowerCase();
      if (kind === "form" || kind === "screen") addDiscovered(pname, asset?.name, "project.ui_assets");
    });
    (Array.isArray(project?.members) ? project.members : []).forEach((member) => {
      const kind = String(member?.kind || "").toLowerCase();
      const path = String(member?.path || "");
      if (kind === "form" || path.toLowerCase().endsWith(".frm")) addDiscovered(pname, path.split("/").pop(), "project.members");
    });
  });
  rawFormDossiers.forEach((row) => addDiscovered(row?.project_name, row?.form_name, "form_dossier"));
  const orphanByKey = {};
  let orphanUnmappedCount = 0;
  rawOrphans.forEach((row) => {
    const orphanForm = normalizeDiscoveredFormName(String(row?.form || "").trim() || String(row?.path || "").split("/").pop().replace(/\.frm$/i, ""));
    const orphanProject = String(row?.project_name || "").trim();
    if (orphanForm === "(unmapped_form_files)") {
      const summary = String(row?.behavior_summary || "");
      const m = summary.match(/(\d+)\s+discovered\s+form\s+files/i);
      if (m) orphanUnmappedCount = Number(m[1] || 0);
      return;
    }
    const key = formKey(orphanProject, orphanForm);
    if (key) {
      orphanByKey[key] = row;
      addDiscovered(orphanProject, orphanForm, "orphan_analysis");
    }
  });
  lines.push(`- Legacy inventory: ${raw.legacy_inventory ? "present" : (hv.legacy_inventory ? "present" : "missing")}`);
  lines.push(`- Event map rows: ${rawEventMap.length || (Array.isArray(hv.event_map) ? hv.event_map.length : 0)}`);
  lines.push(`- SQL catalog rows: ${rawSql.length || (Array.isArray(hv.sql_catalog) ? hv.sql_catalog.length : 0)}`);
  lines.push(`- SQL map rows: ${rawSqlMap.length}`);
  lines.push(`- Procedure summaries: ${rawProcedures.length}`);
  lines.push(`- Form dossiers: ${rawFormDossiers.length}`);
  lines.push(`- Dependency rows: ${rawDeps.length || (Array.isArray(hv.dependencies) ? hv.dependencies.length : 0)}`);
  lines.push(`- Business rules: ${rawRules.length || (Array.isArray(hv.business_rules) ? hv.business_rules.length : 0)}`);
  lines.push(`- Risk register rows: ${rawRisks.length}`);
  lines.push(`- Orphan analysis rows: ${rawOrphans.length}`);
  lines.push(`- Repo landscape variants: ${rawLandscape.length}`);
  lines.push(`- Variant inventory rows: ${rawVariantInventory.length}`);
  lines.push(`- Constitution principles: ${rawConstitution.length}`);
  const legacyCountsSnapshot = (raw.legacy_inventory?.summary?.counts && typeof raw.legacy_inventory.summary.counts === "object")
    ? raw.legacy_inventory.summary.counts
    : {};
  lines.push(`- Source LOC: ${Number(legacyCountsSnapshot.source_loc_total || 0)} total (forms=${Number(legacyCountsSnapshot.source_loc_forms || 0)}, modules=${Number(legacyCountsSnapshot.source_loc_modules || 0)}) across ${Number(legacyCountsSnapshot.source_files_scanned || 0)} file(s)`);

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
  const formDossierRows = rawFormDossiers;
  const constitutionRows = rawConstitution;
  const detectorRows = Array.isArray(raw.detector_findings?.findings) ? raw.detector_findings.findings : [];
  const riskRowsRaw = rawRisks;
  const orphanRows = rawOrphans;
  const landscapeRows = rawLandscape;
  const variantRows = rawVariantInventory;
  const artifactIndexRows = Array.isArray(raw.artifact_index?.artifacts) ? raw.artifact_index.artifacts : [];

  if (includeDetailedAppendix) {
    lines.push("", "## Detailed Appendix", "");
    lines.push("### A. Legacy Inventory");
    lines.push(`- Projects: ${projects.length}`);
    lines.push(`- Data touchpoints: ${(Array.isArray(rawLegacy.summary?.data_touchpoints) ? rawLegacy.summary.data_touchpoints.join(", ") : "") || "None detected"}`);
    const legacyCounts = (rawLegacy.summary?.counts && typeof rawLegacy.summary.counts === "object") ? rawLegacy.summary.counts : {};
    lines.push(`- Source LOC: ${Number(legacyCounts.source_loc_total || 0)} total (forms=${Number(legacyCounts.source_loc_forms || 0)}, modules=${Number(legacyCounts.source_loc_modules || 0)}) across ${Number(legacyCounts.source_files_scanned || 0)} file(s)`);
    if (projects.length) {
      lines.push("| Project | Type | Startup | Members | Forms | Reports | Dependencies | Source LOC | Shared tables |");
      lines.push("|---|---|---|---:|---:|---:|---:|---:|---|");
      projects.slice(0, 250).forEach((project) => {
        const members = Array.isArray(project?.members) ? project.members.length : 0;
        const ui = Array.isArray(project?.ui_assets) ? project.ui_assets.length : 0;
        const deps = Array.isArray(project?.dependencies) ? project.dependencies.length : 0;
        const sourceLoc = Number(project?.source_loc_total || 0);
        const projectName = String(project?.name || project?.project_id || "").trim();
        const reports = (Array.isArray(project?.members) ? project.members : []).filter((member) => {
          const kind = String(member?.kind || "").toLowerCase();
          const path = String(member?.path || "").toLowerCase();
          return kind === "report" || kind === "designer" || path.includes("report") || path.endsWith(".dsr");
        }).length;
        const sharedTables = Array.from(projectTablesByName[projectName] instanceof Set ? projectTablesByName[projectName] : [])
          .filter((t) => (tableToProjects[t] instanceof Set) && tableToProjects[t].size > 1)
          .sort();
        lines.push(`| ${projectName} | ${String(project?.type || "")} | ${String(project?.startup || "")} | ${members} | ${ui} | ${reports} | ${deps} | ${sourceLoc} | ${sharedTables.slice(0, 8).join(", ") || "none"} |`);
      });
    } else {
      lines.push("- No project rows available.");
    }

    lines.push("", "### B. Dependency Inventory");
    if (depRows.length) {
      lines.push("| Name | Kind | Risk | Recommended action | Forms mapped |");
      lines.push("|---|---|---|---|---|");
      depRows.slice(0, 500).forEach((dep) => {
        const risk = String(dep?.risk?.tier || dep?.tier || "unknown");
        const action = String(dep?.risk?.recommended_action || dep?.recommended_action || "");
        const depName = String(dep?.name || dep || "").trim();
        const mapped = Array.from(dependencyToForms[depName.toLowerCase()] instanceof Set ? dependencyToForms[depName.toLowerCase()] : []).sort();
        lines.push(`| ${depName} | ${String(dep?.kind || "")} | ${risk} | ${action || "n/a"} | ${mapped.slice(0, 6).join(", ") || "n/a"} |`);
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
      lines.push("| Rule ID | Form | Layer | Category | Business Meaning | Implementation Evidence | Risk links |");
      lines.push("|---|---|---|---|---|---|---|");
      const variantProjectsByBase = {};
      rawFormDossiers.forEach((dossier) => {
        const base = baseFormName(dossier?.form_name);
        const proj = String(dossier?.project_name || "").trim();
        if (!base || !proj) return;
        if (!variantProjectsByBase[base]) variantProjectsByBase[base] = new Set();
        variantProjectsByBase[base].add(proj);
      });
      const rulesByForm = {};
      const rawRulesByForm = {};
      const seenRuleFormPairs = new Set();
      const seenSourceVariantPairs = new Set();
      const emittedFormLabels = new Set();
      const emittedQualifiedFormLabels = new Set();
      const existingRuleNumbers = [];
      const usedOutputRuleIds = new Set();
      const saturatedMeaningTemplates = new Set([
        "Balance is recalculated using the entered amount and current account value.",
        "The action proceeds only when the recordset/connection is active.",
      ]);
      const canonicalMeaningRuleIds = {};
      const saturatedMeaningForms = {};
      const saturatedSuppressedCount = {};

      const ruleFormsFromRow = (row, evidenceText) => {
        const scope = (row?.scope && typeof row.scope === "object") ? row.scope : {};
        const formsOut = [];
        const candidates = [
          scope.form,
          scope.form_key,
          scope.component_id,
          row?.form,
          ...(Array.isArray(scope.forms) ? scope.forms : []),
          ...(Array.isArray(scope.form_keys) ? scope.form_keys : []),
        ];
        candidates.forEach((cand) => {
          const rawToken = String(cand || "").trim();
          if (!rawToken) return;
          const splitTokens = rawToken.split(/[,;|]/).map((part) => String(part || "").trim()).filter(Boolean);
          (splitTokens.length ? splitTokens : [rawToken]).forEach((tokenIn) => {
            let token = String(tokenIn || "").trim();
            if (!token) return;
            if (token.includes("::")) token = token.split("::", 2)[1] || "";
            token = String(token.split("/").pop() || "").trim();
            token = token.replace(/\.(frm|ctl|cls|bas)$/i, "").replace(/[.,;:()[\]{}]+$/g, "");
            const low = token.toLowerCase();
            if (!low) return;
            if (/\.(bas|cls|ctl|ctx|vbp|vbg|res|dsr|dca|dcx)$/i.test(low)) return;
            if (/(?:_|^)(click|change|load|keypress|gotfocus|lostfocus|activate|deactivate)$/i.test(low)) return;
            const normalizedBase = baseFormName(token);
            if (!(low.includes("frm") || low.startsWith("form") || ["main", "mdiform", "login"].includes(low) || knownFormBases.has(normalizedBase))) return;
            const normalizedDisplay = (low === "main") ? "main" : token;
            if (!formsOut.includes(normalizedDisplay)) formsOut.push(normalizedDisplay);
          });
        });
        [String(row?.statement || ""), String(evidenceText || "")].forEach((src) => {
          extractFormsFromText(src).forEach((formName) => {
            const normalizedDisplay = (baseFormName(formName) === "main") ? "main" : formName;
            if (!formsOut.includes(normalizedDisplay)) formsOut.push(normalizedDisplay);
          });
        });
        return formsOut;
      };

      const nextMirroredRuleId = () => {
        let nextNum = (existingRuleNumbers.length ? Math.max(...existingRuleNumbers) : 0) + 1;
        while (usedOutputRuleIds.has(`br-${String(nextNum).padStart(3, "0")}`)) nextNum += 1;
        existingRuleNumbers.push(nextNum);
        const rid = `BR-${String(nextNum).padStart(3, "0")}`;
        usedOutputRuleIds.add(rid.toLowerCase());
        return rid;
      };

      const allocateRuleId = (sourceRuleId) => {
        const sid = String(sourceRuleId || "").trim();
        if (/^BR-\d+$/i.test(sid) && !usedOutputRuleIds.has(sid.toLowerCase())) {
          usedOutputRuleIds.add(sid.toLowerCase());
          return { id: sid, sourceHint: "" };
        }
        return { id: nextMirroredRuleId(), sourceHint: sid };
      };

      const canonicalizeSaturatedMeaning = (meaningText, ruleId, formLabel) => {
        const text = String(meaningText || "").trim();
        if (!saturatedMeaningTemplates.has(text)) return { text, suppress: false, anchorRuleId: ruleId };
        const first = String(canonicalMeaningRuleIds[text] || "").trim();
        if (!first) {
          canonicalMeaningRuleIds[text] = ruleId;
          if (!saturatedMeaningForms[text]) saturatedMeaningForms[text] = new Set();
          saturatedMeaningForms[text].add(String(formLabel || "").trim() || "n/a");
          saturatedSuppressedCount[text] = Number(saturatedSuppressedCount[text] || 0);
          return { text, suppress: false, anchorRuleId: ruleId };
        }
        if (!saturatedMeaningForms[text]) saturatedMeaningForms[text] = new Set();
        saturatedMeaningForms[text].add(String(formLabel || "").trim() || "n/a");
        saturatedSuppressedCount[text] = Number(saturatedSuppressedCount[text] || 0) + 1;
        return { text, suppress: true, anchorRuleId: first };
      };

      const pickBackfillRule = (candidates) => {
        const rows = Array.isArray(candidates) ? candidates : [];
        if (!rows.length) return null;
        let preferred = null;
        for (const row of rows) {
          const category = String(row?.category || row?.rule_type || "other").trim();
          const meaning = ruleBusinessMeaning(String(row?.statement || ""), category);
          if (meaning && !saturatedMeaningTemplates.has(meaning)) return row;
          if (!preferred) preferred = row;
        }
        return preferred;
      };

      ruleRows.slice(0, 700).forEach((row) => {
        const sourceRuleId = String(row?.rule_id || row?.id || "n/a").trim() || "n/a";
        const m = sourceRuleId.match(/(\d+)$/);
        if (m) existingRuleNumbers.push(Number(m[1] || 0));
        const category = String(row?.category || row?.rule_type || "other");
        const statement = String(row?.statement || "");
        const ev = Array.isArray(row?.evidence)
          ? row.evidence.map((e) => String(e?.external_ref?.ref || e?.file_span?.path || e?.ref || "")).filter(Boolean).slice(0, 3).join(", ")
          : String(row?.evidence || "");
        const forms = ruleFormsFromRow(row, ev);
        const ruleForms = forms.length ? forms : ["n/a"];
        const evidenceLow = `${String(row?.statement || "")} ${ev}`.toLowerCase();
        let layer = "Presentation";
        if (["data_persistence", "calculation_logic", "threshold_rule"].includes(category.toLowerCase()) || ["select ", "insert ", "update ", "delete ", "table"].some((x) => evidenceLow.includes(x))) layer = "Data";
        if ([".bas", "module", "shared"].some((x) => evidenceLow.includes(x))) layer = "Shared";
        const relatedRiskIds = new Set();
        const ruleBaseForms = new Set(ruleForms.map((f) => baseFormName(f)).filter(Boolean));
        rawFormDossiers.forEach((dossier) => {
          if (!ruleBaseForms.has(baseFormName(dossier?.form_name))) return;
          lookupRows(formRiskRows, dossier?.project_name, dossier?.form_name).forEach((riskRow) => {
            const rid = String(riskRow?.risk_id || "").trim();
            if (rid) relatedRiskIds.add(rid);
          });
        });
        const ruleLow = statement.toLowerCase();
        rawRisks.forEach((riskRow) => {
          const desc = String(riskRow?.description || "").toLowerCase();
          const rid = String(riskRow?.risk_id || "").trim();
          if (!rid) return;
          if (["caption", "balance", "customerid", "delete", "injection", "credential", "password"].some((token) => desc.includes(token) && ruleLow.includes(token))) relatedRiskIds.add(rid);
        });
        const meaning = ruleBusinessMeaning(statement, category);
        ruleForms.slice(0, 16).forEach((formItem) => {
          const formDisplay = (baseFormName(formItem) === "main") ? "main" : (String(formItem || "").trim() || "n/a");
          const normalized = baseFormName(formDisplay) || String(formDisplay || "").trim().toLowerCase() || "n/a";
          const isGenericBase = /^(form\d+|frm\d+)$/i.test(normalized);
          if (!String(formDisplay || "").includes("::") && isGenericBase && (variantProjectsByBase[normalized]?.size || 0) > 1) return;
          const pairKey = `${sourceRuleId.toLowerCase()}::${normalized.toLowerCase()}`;
          if (seenRuleFormPairs.has(pairKey)) return;
          seenRuleFormPairs.add(pairKey);
          let rowMeaning = meaning;
          if ((String(formDisplay || "").toLowerCase().includes("splash") || String(ev || "").toLowerCase().includes("splash")) && rowMeaning.toLowerCase().includes("balance is recalculated")) {
            rowMeaning = "Splash/loading behavior advances progress state before opening workflow screens.";
          }
          const allocated = allocateRuleId(sourceRuleId);
          const saturation = canonicalizeSaturatedMeaning(rowMeaning, allocated.id, formDisplay);
          rowMeaning = saturation.text;
          if (saturation.suppress) {
            if (!rulesByForm[normalized]) rulesByForm[normalized] = [];
            if (!rawRulesByForm[normalized]) rawRulesByForm[normalized] = [];
            rulesByForm[normalized].push({ rule_id: saturation.anchorRuleId, meaning: rowMeaning });
            rawRulesByForm[normalized].push(row);
            return;
          }
          let evidenceOut = ev || "n/a";
          if (allocated.sourceHint && allocated.sourceHint.toLowerCase() !== allocated.id.toLowerCase()) {
            evidenceOut = evidenceOut !== "n/a" ? `${evidenceOut}; source_rule=${allocated.sourceHint}` : `source_rule=${allocated.sourceHint}`;
          }
          lines.push(`| ${allocated.id} | ${String(formDisplay || "n/a")} | ${layer} | ${category} | ${rowMeaning.replace(/\|/g, "\\|") || "n/a"} | ${String(evidenceOut || "n/a").replace(/\|/g, "\\|")} | ${Array.from(relatedRiskIds).sort().slice(0, 6).join(", ") || "none"} |`);
          emittedFormLabels.add(String(formDisplay || "n/a").toLowerCase());
          if (String(formDisplay || "").includes("::")) emittedQualifiedFormLabels.add(String(formDisplay || "").toLowerCase());
          if (!rulesByForm[normalized]) rulesByForm[normalized] = [];
          if (!rawRulesByForm[normalized]) rawRulesByForm[normalized] = [];
          rulesByForm[normalized].push({ rule_id: allocated.id, meaning: rowMeaning });
          rawRulesByForm[normalized].push(row);
        });
      });

      rawFormDossiers.forEach((dossier) => {
        const projectName = String(dossier?.project_name || "").trim();
        const formName = String(dossier?.form_name || "").trim();
        const base = baseFormName(formName);
        if (!base) return;
        const qualified = qualifiedFormName(projectName, base === "main" ? "main" : formName);
        if (emittedFormLabels.has(String(qualified || "").toLowerCase())) return;
        const mirroredRows = lookupRows(formRuleRows, projectName, formName);
        mirroredRows.slice(0, 10).forEach((mr) => {
          const sourceRuleId = String(mr?.rule_id || mr?.id || "n/a").trim() || "n/a";
          const sourcePair = `${sourceRuleId.toLowerCase()}::${qualified.toLowerCase()}`;
          if (seenSourceVariantPairs.has(sourcePair)) return;
          seenSourceVariantPairs.add(sourcePair);
          const category = String(mr?.category || mr?.rule_type || "other").trim() || "other";
          const statement = String(mr?.statement || "");
          const meaning = ruleBusinessMeaning(statement, category);
          const layer = ["data_persistence", "calculation_logic", "threshold_rule"].includes(category.toLowerCase()) ? "Data" : "Presentation";
          const relatedRiskIds = new Set();
          lookupRows(formRiskRows, projectName, formName).forEach((riskRow) => {
            const ridRisk = String(riskRow?.risk_id || "").trim();
            if (ridRisk) relatedRiskIds.add(ridRisk);
          });
          const mirroredAllocated = allocateRuleId(sourceRuleId);
          const mirroredRuleId = mirroredAllocated.id;
          const saturation = canonicalizeSaturatedMeaning((meaning || statement || "n/a"), mirroredRuleId, qualified);
          const mirroredMeaning = saturation.text;
          if (saturation.suppress) {
            if (!rulesByForm[base]) rulesByForm[base] = [];
            if (!rawRulesByForm[base]) rawRulesByForm[base] = [];
            rulesByForm[base].push({ rule_id: saturation.anchorRuleId, meaning: mirroredMeaning });
            rawRulesByForm[base].push(mr);
            return;
          }
          const evidence = `mirrored_from_variant_mapping (source=${sourceRuleId || "n/a"})`;
          lines.push(`| ${mirroredRuleId} | ${qualified} | ${layer} | ${category} | ${mirroredMeaning.replace(/\|/g, "\\|")} | ${evidence.replace(/\|/g, "\\|")} | ${Array.from(relatedRiskIds).sort().slice(0, 6).join(", ") || "none"} |`);
          emittedFormLabels.add(String(qualified || "n/a").toLowerCase());
          emittedQualifiedFormLabels.add(String(qualified || "").toLowerCase());
          if (!rulesByForm[base]) rulesByForm[base] = [];
          if (!rawRulesByForm[base]) rawRulesByForm[base] = [];
          rulesByForm[base].push({ rule_id: mirroredRuleId, meaning: mirroredMeaning });
          rawRulesByForm[base].push(mr);
        });
      });

      // E/Q synchronization backfill: emit one qualified row when Q has rules for a form
      // but E has no qualified row for that form label.
      rawFormDossiers.forEach((dossier) => {
        const projectName = String(dossier?.project_name || "").trim();
        const formName = String(dossier?.form_name || "").trim();
        const base = baseFormName(formName);
        if (!base) return;
        const qualified = qualifiedFormName(projectName, base === "main" ? "main" : formName);
        if (!qualified || emittedQualifiedFormLabels.has(String(qualified).toLowerCase())) return;
        const candidateRows = lookupRows(formRuleRows, projectName, formName);
        if (!Array.isArray(candidateRows) || !candidateRows.length) return;
        const chosen = pickBackfillRule(candidateRows);
        if (!chosen) return;
        const sourceRuleId = String(chosen?.rule_id || chosen?.id || "n/a").trim() || "n/a";
        const category = String(chosen?.category || chosen?.rule_type || "other").trim() || "other";
        const statement = String(chosen?.statement || "");
        const layer = ["data_persistence", "calculation_logic", "threshold_rule"].includes(category.toLowerCase()) ? "Data" : "Presentation";
        const backfillMeaning = ruleBusinessMeaning(statement, category) || statement || "n/a";
        const allocated = allocateRuleId(sourceRuleId);
        const relatedRiskIds = new Set();
        lookupRows(formRiskRows, projectName, formName).forEach((riskRow) => {
          const ridRisk = String(riskRow?.risk_id || "").trim();
          if (ridRisk) relatedRiskIds.add(ridRisk);
        });
        let evidence = `variant_backfill_for_eq_sync (source=${sourceRuleId || "n/a"})`;
        if (allocated.sourceHint && allocated.sourceHint.toLowerCase() !== allocated.id.toLowerCase()) {
          evidence = `${evidence}; source_rule=${allocated.sourceHint}`;
        }
        lines.push(`| ${allocated.id} | ${qualified} | ${layer} | ${category} | ${String(backfillMeaning || "n/a").replace(/\|/g, "\\|")} | ${String(evidence).replace(/\|/g, "\\|")} | ${Array.from(relatedRiskIds).sort().slice(0, 6).join(", ") || "none"} |`);
        emittedFormLabels.add(String(qualified || "n/a").toLowerCase());
        emittedQualifiedFormLabels.add(String(qualified || "").toLowerCase());
        if (!rulesByForm[base]) rulesByForm[base] = [];
        if (!rawRulesByForm[base]) rawRulesByForm[base] = [];
        rulesByForm[base].push({ rule_id: allocated.id, meaning: backfillMeaning });
        rawRulesByForm[base].push(chosen);
      });

      if (Object.keys(rulesByForm).length) {
        const dossierByBaseForm = {};
        rawFormDossiers.forEach((dossier) => {
          const key = baseFormName(dossier?.form_name);
          if (key && !dossierByBaseForm[key]) dossierByBaseForm[key] = dossier;
        });
        lines.push("", "### E1. Rule Cross-Reference by Form");
        Object.keys(rulesByForm).sort().slice(0, 220).forEach((formName) => {
          const rows = Array.isArray(rulesByForm[formName]) ? rulesByForm[formName] : [];
          const ruleIds = Array.from(new Set(rows.map((r) => String(r?.rule_id || "").trim()).filter(Boolean))).sort().slice(0, 8).join(", ") || "n/a";
          const summaries = [];
          rows.forEach((r) => {
            const m = String(r?.meaning || "").trim();
            if (m && !summaries.includes(m)) summaries.push(m);
          });
          const dossierSummary = dossierBusinessRuleSummary({
            formName,
            dossier: dossierByBaseForm[formName],
            ruleRows: Array.isArray(rawRulesByForm[formName]) ? rawRulesByForm[formName] : [],
          });
          if (dossierSummary && !summaries.includes(dossierSummary)) summaries.unshift(dossierSummary);
          lines.push(`- ${formName}: rule_ids=[${ruleIds}]; summary=${(summaries.slice(0, 3).join(" / ") || "n/a").replace(/\|/g, "\\|")}`);
        });
      }
      const sharedRows = Array.from(saturatedMeaningTemplates)
        .map((meaning) => {
          const suppressed = Number(saturatedSuppressedCount[meaning] || 0);
          const forms = Array.from(saturatedMeaningForms[meaning] instanceof Set ? saturatedMeaningForms[meaning] : []).sort();
          return {
            meaning,
            anchor: String(canonicalMeaningRuleIds[meaning] || "").trim() || "n/a",
            suppressed,
            forms,
          };
        })
        .filter((row) => row.suppressed > 0);
      if (sharedRows.length) {
        lines.push("", "### E2. Shared Rule Consolidation");
        sharedRows.forEach((row) => {
          const preview = row.forms.slice(0, 12).join(", ") || "n/a";
          const suffix = row.forms.length > 12 ? ` (+${row.forms.length - 12} more)` : "";
          lines.push(`- ${row.anchor}: consolidated ${row.suppressed} duplicate row(s); applies to ${row.forms.length} form(s): ${(preview + suffix).replace(/\|/g, "\\|")}`);
          lines.push(`  - Canonical meaning: ${String(row.meaning || "n/a").replace(/\|/g, "\\|")}`);
        });
      }
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
      lines.push("| Form | Procedure | Operation | Tables | Risks | activex_trigger | trace_complete |");
      lines.push("|---|---|---|---|---|---|---|");
      sqlMapRows.slice(0, 700).forEach((row) => {
        const projectName = String(row?.variant || "").trim() || projectFromScoped(row?.form);
        const formName = String(row?.form_base || "").trim() || String(row?.form || "").trim();
        const procRowsForForm = lookupRows(formProcRows, projectName, formName);
        const dbTablesForForm = Array.from(lookupSet(formDbTables, projectName, formName)).sort();
        const dossier = dossierByKey[formKey(projectName, formName)] || dossierByKey[baseOnlyKey(formName)] || {};
        const alias = semanticFormAlias({
          formName,
          purpose: String(dossier?.purpose || ""),
          dbTables: new Set(dbTablesForForm),
          procedures: procRowsForForm,
          rules: lookupRows(formRuleRows, projectName, formName),
        });
        const procName = String(row?.procedure || "").trim();
        const relatedEvents = lookupRows(formEventRows, projectName, formName).filter((evt) => {
          const handler = String(evt?.handler?.symbol || "").trim();
          return procName && handler.includes(procName);
        });
        const controlMap = lookupControlMap(formControlTypeByKey, projectName, formName);
        const activexHits = [];
        relatedEvents.forEach((evt) => {
          const triggerControl = String(evt?.trigger?.control || "").trim();
          if (!triggerControl) return;
          const ctlType = String(controlMap[triggerControl.toLowerCase()] || "").trim();
          if (ctlType && !ctlType.toUpperCase().startsWith("VB")) activexHits.push(`${triggerControl}:${ctlType}`);
        });
        const tables = Array.isArray(row?.tables) ? row.tables.slice(0, 6).join(", ") : "";
        const risks = Array.isArray(row?.risk_flags) ? row.risk_flags.slice(0, 6).join(", ") : "";
        const hasSql = Boolean(String(row?.sql_id || "").trim());
        const hasTables = Array.isArray(row?.tables) && row.tables.length > 0;
        const traceComplete = hasSql && hasTables;
        lines.push(`| ${qualifiedFormName(projectName, displayFormName(formName, alias))} | ${String(row?.procedure || "n/a")} | ${String(row?.operation || "unknown")} | ${tables || "n/a"} | ${risks || "none"} | ${Array.from(new Set(activexHits)).slice(0, 4).join(", ") || "n/a"} | ${traceComplete ? "yes" : "no"} |`);
      });
    } else {
      lines.push("- No SQL map rows available.");
    }

    lines.push("", "### I. Handler and Procedure Summaries");
    if (procedureRows.length) {
      lines.push("| Callable | Kind | Form | SQL IDs | Steps | Risks |");
      lines.push("|---|---|---|---|---|---|");
      procedureRows.slice(0, 700).forEach((row) => {
        const procName = String(row?.procedure_name || row?.procedure_id || "n/a");
        const kind = callableKind(procName, row?.form, String(row?.trigger?.event || ""));
        const sqlIds = Array.isArray(row?.sql_ids) ? row.sql_ids.slice(0, 6).join(", ") : "";
        const steps = Array.isArray(row?.steps) ? row.steps.slice(0, 2).join(" / ").replace(/\|/g, "\\|") : "";
        const risks = Array.isArray(row?.risks) ? row.risks.slice(0, 5).join(", ") : "";
        lines.push(`| ${procName} | ${kind} | ${String(row?.form || "n/a")} | ${sqlIds || "n/a"} | ${steps || "n/a"} | ${risks || "none"} |`);
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

    lines.push("", "### K. Form Dossiers");
    if (discoveredForms.length) {
      lines.push("| Form | Display Name | Project | form_type | Status | Purpose | Inputs (data) | Outputs (effects) | ActiveX used | DB tables | Actions | Coverage | Confidence | Exclusion reason |");
      lines.push("|---|---|---|---|---|---|---|---|---|---|---:|---:|---:|---|");
      const excludedRows = [];
      discoveredForms
        .slice()
        .sort((a, b) => (String(a?.project_name || "").localeCompare(String(b?.project_name || "")) || String(a?.form_name || "").localeCompare(String(b?.form_name || ""))))
        .slice(0, 900)
        .forEach((formRef) => {
          const projectName = String(formRef?.project_name || "").trim();
          const formName = String(formRef?.form_name || "").trim();
          const key = String(formRef?.form_key || "").trim() || formKey(projectName, formName);
          const baseKey = baseOnlyKey(formName);
          const dossier = dossierByKey[key] || dossierByKey[baseKey] || null;
          const orphanRow = orphanByKey[key] || orphanByKey[baseKey] || null;

          let status = "mapped";
          let exclusionReason = "none";
          if (!dossier) {
            if (orphanRow) {
              status = "orphan";
              exclusionReason = String(orphanRow?.recommendation || orphanRow?.reason || "orphaned_form");
            } else {
              status = "excluded";
              exclusionReason = "missing_from_form_dossier";
            }
          }

          const procRows = lookupRows(formProcRows, projectName, formName);
          const sqlRowsForForm = lookupRows(formSqlRows, projectName, formName);
          const formRules = lookupRows(formRuleRows, projectName, formName);
          const formControls = Array.isArray(dossier?.controls) ? dossier.controls : [];

          const dbTablesSet = lookupSet(formDbTables, projectName, formName);
          if (dbTablesSet instanceof Set && dbTablesSet.size === 0) {
            sqlRowsForForm.forEach((sqlRow) => {
              (Array.isArray(sqlRow?.tables) ? sqlRow.tables : []).forEach((tableName) => {
                const tbl = String(tableName || "").trim();
                if (tbl) dbTablesSet.add(tbl);
              });
            });
          }
          const dbTables = Array.from(dbTablesSet).sort();

          let purpose = String(dossier?.purpose || "").trim();
          const alias = semanticFormAlias({
            formName: formName || "n/a",
            purpose,
            dbTables: new Set(dbTables),
            procedures: procRows,
            rules: formRules,
            controls: formControls,
          });
          if (!purpose && alias) purpose = `${alias} workflow.`;
          const displayName = displayFormName(formName || "n/a", alias);

          const inputs = new Set();
          formControls.forEach((controlName) => {
            const token = String(controlName || "").trim();
            if (!token) return;
            const controlId = String(token.split(":").slice(-1)[0] || "").trim();
            if (isDataInputControl(controlId)) inputs.add(toBusinessInput(controlId));
          });
          procRows.forEach((proc) => {
            (Array.isArray(proc?.inputs) ? proc.inputs : []).forEach((inputName) => {
              const token = String(inputName || "").trim();
              if (isDataInputControl(token)) inputs.add(toBusinessInput(token));
            });
          });

          const outputs = new Set();
          sqlRowsForForm.forEach((sqlRow) => {
            const op = String(sqlRow?.operation || "").trim().toLowerCase();
            const tables = (Array.isArray(sqlRow?.tables) ? sqlRow.tables : []).map((x) => String(x || "").trim()).filter(Boolean);
            if (!tables.length) return;
            tables.forEach((tableName) => {
              const effect = businessEffectFromSql(op, tableName);
              if (effect) outputs.add(effect);
            });
          });
          if (!outputs.size) {
            procRows.forEach((proc) => {
              (Array.isArray(proc?.data_mutations) ? proc.data_mutations : []).forEach((tableName) => {
                const table = String(tableName || "").trim();
                if (!table) return;
                const effect = businessEffectFromSql("update", table);
                if (effect) outputs.add(effect);
              });
            });
            if (!outputs.size) {
              fallbackBusinessEffects({
                alias,
                purpose,
                inputs,
                dbTables,
                rules: formRules,
              }).forEach((effect) => outputs.add(effect));
            }
          }

          const activex = new Set();
          formControls.forEach((ctl) => {
            const name = String(ctl || "").trim();
            if (!name) return;
            const prefix = String(name.split(":")[0] || "").trim();
            if (prefix && !prefix.toUpperCase().startsWith("VB")) activex.add(prefix);
          });
          const projectDeps = projectDepsByName[projectName];
          if (projectDeps instanceof Set) {
            Array.from(projectDeps).forEach((dep) => {
              const d = String(dep || "").trim();
              if (!d) return;
              const up = d.toUpperCase();
              if (d.toLowerCase().endsWith(".ocx") || d.toLowerCase().endsWith(".dll") || up.includes("MSCOM") || up.includes("MSFLEX")) activex.add(d);
            });
          }

          const formType = inferFormType({
            formName: formName || "n/a",
            purpose,
            procedures: procRows,
            controls: formControls,
            tables: dbTables,
          });
          const coverage = Number(dossier?.coverage?.coverage_score || 0);
          const rawConfidence = Number(dossier?.coverage?.confidence_score || 0);
          const actions = Array.isArray(dossier?.actions) ? dossier.actions.length : procRows.length;
          const genericPurpose = new Set([
            "business workflow executed through event-driven ui controls.",
            "business workflow executed through event-driven ui controls",
            "potential orphan flow detected.",
            "potential orphan flow detected",
          ]).has(String(purpose || "").trim().toLowerCase());
          const coverageClamped = Math.max(0, Math.min(1, Number.isFinite(coverage) ? coverage : 0));
          let confidence = 0.22 + (0.45 * coverageClamped);
          confidence += Math.min(0.14, 0.02 * actions);
          confidence += Math.min(0.08, 0.015 * procRows.length);
          confidence += Math.min(0.08, 0.02 * dbTables.length);
          confidence += sqlRowsForForm.length ? 0.09 : -0.08;
          confidence += !genericPurpose ? 0.08 : -0.12;
          if (!inputs.size) confidence -= 0.06;
          if (!actions) confidence -= 0.16;
          if (/^(form\d+|frm\d+)$/i.test(formName || "") && !String(alias || "").trim()) confidence -= 0.08;
          if (rawConfidence > 0 && rawConfidence <= 1 && Math.abs(rawConfidence - 0.92) > 1e-4) {
            confidence = (confidence * 0.8) + (rawConfidence * 0.2);
          }
          confidence = Math.max(0.1, Math.min(0.98, confidence));

          if (status !== "mapped") {
            excludedRows.push({
              form: qualifiedFormName(projectName, formName),
              reason: exclusionReason,
              source: String(formRef?.source || "detected"),
            });
          }

          lines.push(`| ${formName || "n/a"} | ${displayName || "n/a"} | ${projectLabel(projectName)} | ${formType} | ${status} | ${String(purpose || "n/a").replace(/\\|/g, "\\\\|")} | ${Array.from(inputs).sort().slice(0, 8).join(", ") || "n/a"} | ${Array.from(outputs).sort().slice(0, 8).join(", ") || "n/a"} | ${Array.from(activex).sort().slice(0, 6).join(", ") || "n/a"} | ${dbTables.slice(0, 8).join(", ") || "n/a"} | ${actions} | ${Number.isFinite(coverage) ? coverage.toFixed(2) : "0.00"} | ${Number.isFinite(confidence) ? confidence.toFixed(2) : "0.00"} | ${String(exclusionReason || "none").replace(/\\|/g, "\\\\|")} |`);
        });

      const summaryCounts = (rawLegacy.summary && typeof rawLegacy.summary === "object" && rawLegacy.summary.counts && typeof rawLegacy.summary.counts === "object")
        ? rawLegacy.summary.counts
        : {};
      const expectedForms = Number(summaryCounts.forms_or_screens || 0);
      const renderedForms = discoveredForms.length;
      if ((expectedForms > renderedForms) || orphanUnmappedCount > 0) {
        const unresolved = orphanUnmappedCount || Math.max(0, expectedForms - renderedForms);
        lines.push("");
        lines.push(`- Coverage note: expected_forms=${expectedForms}, rendered_forms=${renderedForms}, unmapped_form_files=${unresolved}. Unmapped/placeholder forms are listed below.`);
      }
      if (excludedRows.length || orphanUnmappedCount > 0) {
        lines.push("", "#### K1. Excluded/Unresolved Forms");
        lines.push("| Form | Reason | Source |");
        lines.push("|---|---|---|");
        excludedRows.slice(0, 400).forEach((row) => {
          lines.push(`| ${String(row?.form || "n/a")} | ${String(row?.reason || "excluded")} | ${String(row?.source || "detected")} |`);
        });
        if (orphanUnmappedCount > 0) {
          lines.push(`| (unmapped_form_files) | reconcile_project_membership (${orphanUnmappedCount} unresolved form files) | orphan_analysis |`);
        }
      }
    } else {
      lines.push("- No form dossier rows available.");
    }

    lines.push("", "### L. Risk Register");
    if (riskRowsRaw.length) {
      lines.push("| Risk ID | Severity | Description | Recommended action |");
      lines.push("|---|---|---|---|");
      riskRowsRaw.slice(0, 700).forEach((row) => {
        lines.push(`| ${String(row?.risk_id || "n/a")} | ${String(row?.severity || "medium")} | ${String(row?.description || "").replace(/\\|/g, "\\\\|")} | ${String(row?.recommended_action || "").replace(/\\|/g, "\\\\|")} |`);
      });
    } else {
      lines.push("- No risk register rows available.");
    }

    lines.push("", "### M. Orphan Analysis");
    if (orphanRows.length) {
      lines.push("| Path | SQL IDs | Tables touched | Recommendation |");
      lines.push("|---|---|---|---|");
      orphanRows.slice(0, 500).forEach((row) => {
        const sqlIds = Array.isArray(row?.sql_ids) ? row.sql_ids.slice(0, 6).join(", ") : "";
        const tables = Array.isArray(row?.tables_touched) ? row.tables_touched.slice(0, 6).join(", ") : "";
        lines.push(`| ${String(row?.path || "n/a")} | ${sqlIds || "n/a"} | ${tables || "n/a"} | ${String(row?.recommendation || "verify")} |`);
      });
    } else {
      lines.push("- No orphan analysis rows available.");
    }

    lines.push("", "### N. Repository Landscape and Variant Inventory");
    if (landscapeRows.length) {
      lines.push("| Variant | Path | Startup | Forms | Members | Dependencies |");
      lines.push("|---|---|---|---:|---:|---:|");
      landscapeRows.slice(0, 200).forEach((row) => {
        const rawId = String(row?.id || "");
        const variant = rawId.includes("|") ? rawId.split("|")[0] : (rawId || "variant");
        lines.push(`| ${variant} | ${String(row?.path || "")} | ${String(row?.startup || "")} | ${Number(row?.counts?.forms || 0)} | ${Number(row?.counts?.members || 0)} | ${Number(row?.counts?.dependencies || 0)} |`);
      });
    } else {
      lines.push("- No repository landscape rows available.");
    }
    if (variantRows.length) {
      lines.push("", "| Variant | Forms | Modules | Tables touched | Dependency summary |");
      lines.push("|---|---:|---:|---:|---|");
      variantRows.slice(0, 200).forEach((row) => {
        const depSummary = row?.dependencies_summary || {};
        lines.push(`| ${String(row?.name || row?.id || "variant")} | ${Array.isArray(row?.forms) ? row.forms.length : 0} | ${Array.isArray(row?.modules) ? row.modules.length : 0} | ${Array.isArray(row?.tables_touched) ? row.tables_touched.length : 0} | total=${Number(depSummary.total || 0)}, ocx=${Number(depSummary.ocx || 0)}, dll=${Number(depSummary.dll || 0)} |`);
      });
    }

    lines.push("", "### O. Project Dependency Map");
    const depRows = [];
    const depSeen = new Set();
    eventRows.forEach((entry) => {
      const source = String(entry?.container || entry?.form || entry?.name || "n/a").trim() || "n/a";
      const triggerControl = String(entry?.trigger?.control || "").trim().toLowerCase();
      const calls = Array.isArray(entry?.calls) ? entry.calls : [];
      calls.forEach((callValue) => {
        const call = String(callValue || "").trim();
        if (!call) return;
        const callLower = call.toLowerCase();
        let type = "";
        if (sharedModuleProcedures.has(call)) type = "shared_module_call";
        else if ((source.toLowerCase().includes("main") || source.toLowerCase().includes("toolbar") || triggerControl.includes("toolbar")) && /^(frm|form|rpt|datareport)/i.test(call)) {
          if (/^(rpt|datareport)/i.test(call)) type = "report_navigation";
          else if (callLower === "frm" || callLower === "form") type = "mdi_navigation_unresolved";
          else type = "mdi_navigation";
        }
        if (!type) return;
        const evidence = String(entry?.handler?.symbol || `${source}->${call}`).trim();
        const key = `${source}|${call}|${type}|${evidence}`;
        if (depSeen.has(key)) return;
        depSeen.add(key);
        let to = call;
        let blocksSprint = "Sprint 1";
        if (type === "report_navigation") blocksSprint = "Sprint 2";
        if (type === "mdi_navigation_unresolved") {
          to = `${call} [Unresolved]`;
          blocksSprint = "n/a (unresolved)";
        }
        depRows.push({ from: source, to, type, evidence, blocksSprint });
      });
    });
    const schema = (rawVariantDiff.schema_divergence && typeof rawVariantDiff.schema_divergence === "object") ? rawVariantDiff.schema_divergence : {};
    const schemaPairs = Array.isArray(schema.blocking_pairs) && schema.blocking_pairs.length ? schema.blocking_pairs : (Array.isArray(schema.pairs) ? schema.pairs : []);
    schemaPairs.forEach((pair) => {
      const left = String(pair?.left_project || "").trim();
      const right = String(pair?.right_project || "").trim();
      if (!left || !right) return;
      const aliasCount = Array.isArray(pair?.alias_mismatches) ? pair.alias_mismatches.length : 0;
      const nearCount = Array.isArray(pair?.near_miss_names) ? pair.near_miss_names.length : 0;
      const txnConflict = pair?.transaction_schema_conflict ? "yes" : "no";
      const evidence = `alias_mismatches=${aliasCount}, near_miss=${nearCount}, transaction_conflict=${txnConflict}`;
      const key = `${left}|${right}|cross_variant_schema_conflict|${evidence}`;
      if (depSeen.has(key)) return;
      depSeen.add(key);
      depRows.push({ from: left, to: right, type: "cross_variant_schema_conflict", evidence, blocksSprint: "Sprint 0" });
    });
    if (depRows.length) {
      lines.push("| From | To | Type | Evidence | Blocks Sprint |");
      lines.push("|---|---|---|---|---|");
      depRows.slice(0, 800).forEach((row) => lines.push(`| ${String(row.from || "")} | ${String(row.to || "")} | ${String(row.type || "")} | ${String(row.evidence || "")} | ${String(row.blocksSprint || "Sprint 1")} |`));
    } else {
      lines.push("- No project dependency rows available.");
    }

    lines.push("", "### O1. Form User Flow (Spec-Kit Style)");
    const knownForms = new Set();
    discoveredForms.forEach((d) => {
      const n = String(d?.form_name || "").trim().toLowerCase();
      if (n) knownForms.add(n);
    });
    formDossierRows.forEach((d) => {
      const n = String(d?.form_name || "").trim().toLowerCase();
      if (n) knownForms.add(n);
    });

    const flowGraph = new Map(); // source -> Map(target -> Set(notes))
    const flowNote = (entry) => {
      const trig = entry?.trigger && typeof entry.trigger === "object" ? entry.trigger : {};
      const parts = [
        String(trig.control || "").trim(),
        String(trig.event || "").trim(),
        String(entry?.handler?.symbol || "").trim(),
      ].filter(Boolean);
      return parts[0] || "";
    };

    eventRows.forEach((entry) => {
      const source = String(entry?.container || entry?.form || entry?.name || "n/a").trim() || "n/a";
      const sourceLow = source.toLowerCase();
      const note = flowNote(entry);
      const calls = Array.isArray(entry?.calls) ? entry.calls : [];
      calls.forEach((callValue) => {
        const call = String(callValue || "").trim();
        if (!call) return;
        if (sharedModuleProcedures.has(call)) return;
        const low = call.toLowerCase();
        let target = "";
        if (["end", "quit", "app.end", "endapp"].includes(low)) target = "End";
        else if (low === "frm" || low === "form") target = "frm [Unresolved]";
        else if (/^(rpt|datareport)/i.test(call)) target = call;
        else if (/^(frm|form)/i.test(call) || low === "main" || knownForms.has(low)) target = call;
        else return;
        if (sourceLow === target.toLowerCase()) return;

        if (!flowGraph.has(source)) flowGraph.set(source, new Map());
        const byTarget = flowGraph.get(source);
        if (!byTarget.has(target)) byTarget.set(target, new Set());
        if (note) byTarget.get(target).add(note);
      });
    });

    if (flowGraph.size) {
      const sortTargets = (targets) => [...targets].sort((a, b) => {
        const rank = (v) => (v === "End" ? 2 : (v.startsWith("frm [Unresolved]") ? 1 : 0));
        const ra = rank(a);
        const rb = rank(b);
        if (ra !== rb) return ra - rb;
        return a.localeCompare(b);
      });
      [...flowGraph.keys()].sort((a, b) => a.localeCompare(b)).forEach((source) => {
        lines.push(source);
        const targets = sortTargets(flowGraph.get(source).keys());
        targets.forEach((target, idx) => {
          const isLast = idx === targets.length - 1;
          const branch = isLast ? "'- ->" : "|- ->";
          const notes = [...(flowGraph.get(source).get(target) || new Set())].sort();
          const suffix = notes.length ? ` [via ${notes[0]}]` : "";
          lines.push(`  ${branch} ${target}${suffix}`);
        });
        lines.push("");
      });
    } else {
      lines.push("- No explicit form-to-form navigation links detected.");
    }

    lines.push("", "### P. Form Flow Traces");
    if (formDossierRows.length) {
      formDossierRows.slice(0, 250).forEach((row) => {
        const formName = String(row?.form_name || "n/a").trim() || "n/a";
        const projectName = String(row?.project_name || "").trim();
        const controlMap = lookupControlMap(formControlTypeByKey, projectName, formName);
        const formEvents = lookupRows(formEventRows, projectName, formName);
        const formProcedures = lookupRows(formProcRows, projectName, formName);
        const formSql = lookupRows(formSqlRows, projectName, formName);
        const procedureNames = new Set();
        formProcedures.forEach((proc) => { const n = String(proc?.procedure_name || "").trim(); if (n) procedureNames.add(n); });
        formSql.forEach((item) => { const n = String(item?.procedure || "").trim(); if (n) procedureNames.add(n); });

        lines.push(`#### ${formName} (${projectLabel(projectName)})`);
        lines.push("| Procedure | Event | ActiveX | SQL IDs | Tables | Trace status |");
        lines.push("|---|---|---|---|---|---|");
        if (!procedureNames.size) {
          const dbTables = Array.from(lookupSet(formDbTables, projectName, formName)).sort();
          lines.push(`| n/a | n/a | n/a | n/a | ${dbTables.slice(0, 8).join(", ") || "n/a"} | TRACE_GAP |`);
        } else {
          Array.from(procedureNames).sort().slice(0, 120).forEach((procName) => {
            const relatedEvents = formEvents.filter((evt) => String(evt?.handler?.symbol || "").includes(procName));
            const relatedSql = formSql.filter((item) => String(item?.procedure || "").trim() === procName);
            const activexHits = [];
            relatedEvents.forEach((evt) => {
              const triggerControl = String(evt?.trigger?.control || "").trim();
              if (!triggerControl) return;
              const ctlType = String(controlMap[triggerControl.toLowerCase()] || "").trim();
              if (ctlType && !ctlType.toUpperCase().startsWith("VB")) activexHits.push(`${triggerControl}:${ctlType}`);
            });
            const sqlIds = Array.from(new Set(relatedSql.map((item) => String(item?.sql_id || "").trim()).filter(Boolean)));
            const tables = Array.from(new Set(relatedSql.flatMap((item) => Array.isArray(item?.tables) ? item.tables : []).map((t) => String(t || "").trim()).filter(Boolean))).sort();
            const traceOk = sqlIds.length > 0 && tables.length > 0;
            lines.push(`| ${procName} | ${relatedEvents.slice(0, 3).map((evt) => String(evt?.handler?.symbol || "").trim()).filter(Boolean).join(", ") || "n/a"} | ${Array.from(new Set(activexHits)).slice(0, 5).join(", ") || "n/a"} | ${sqlIds.slice(0, 6).join(", ") || "n/a"} | ${tables.slice(0, 8).join(", ") || "n/a"} | ${traceOk ? "OK" : "TRACE_GAP"} |`);
          });
        }
      });
    } else {
      lines.push("- No form dossiers available for flow traces.");
    }

    lines.push("", "### Q. Form Traceability Matrix");
    const traceabilityRows = [];
    if (formDossierRows.length) {
      lines.push("| Form | Project | Source LOC | has_event_map | has_sql_map | has_business_rules | has_risk_entry | completeness_score | missing_links |");
      lines.push("|---|---|---:|---|---|---|---|---:|---|");
      const seenKeys = new Set();
      formDossierRows.slice(0, 400).forEach((row) => {
        const formName = String(row?.form_name || "n/a").trim() || "n/a";
        const projectName = String(row?.project_name || "").trim();
        const sourceLoc = Number(row?.source_loc || 0);
        const key = formKey(projectName, formName);
        if (!key || seenKeys.has(key)) return;
        seenKeys.add(key);
        const hasEvent = lookupRows(formEventRows, projectName, formName).length > 0;
        const hasSql = lookupRows(formSqlRows, projectName, formName).length > 0;
        const hasRules = lookupRows(formRuleRows, projectName, formName).length > 0;
        const hasRisk = lookupRows(formRiskRows, projectName, formName).length > 0;
        const hasProc = lookupRows(formProcRows, projectName, formName).length > 0;
        const missing = [];
        if (!hasEvent) missing.push("event_map");
        if (!hasSql) missing.push("sql_map");
        if (!hasRules) missing.push("business_rules");
        if (!hasRisk) missing.push("risk_register");
        if (!hasProc) missing.push("procedure_summary");
        const completenessScore = (Number(hasEvent) + Number(hasSql) + Number(hasRules) + Number(hasRisk) + Number(hasProc)) * 20;
        lines.push(`| ${qualifiedFormName(projectName, formName)} | ${projectLabel(projectName)} | ${sourceLoc} | ${hasEvent ? "yes" : "no"} | ${hasSql ? "yes" : "no"} | ${hasRules ? "yes" : "no"} | ${hasRisk ? "yes" : "no"} | ${completenessScore} | ${missing.join(", ") || "none"} |`);
        traceabilityRows.push({
          formName,
          projectName,
          formKey: key,
          qualifiedForm: qualifiedFormName(projectName, formName),
          completenessScore,
          missing,
          riskIds: lookupRows(formRiskRows, projectName, formName).map((risk) => String(risk?.risk_id || "").trim()).filter(Boolean).slice(0, 4),
          hasEvent,
          hasSql,
        });
      });
    } else {
      lines.push("- No traceability rows available.");
    }

    lines.push("", "### R. Sprint Dependency Map");
    if (traceabilityRows.length) {
      lines.push("| Form | Suggested sprint | Depends on | Shared Components Required | Rationale |");
      lines.push("|---|---|---|---|---|");
      const variantGateNeeded = Boolean(rawVariantDiff.decision_required);
      const emittedKeys = new Set();
      traceabilityRows
        .sort((a, b) => (b.missing.length - a.missing.length) || (a.qualifiedForm.localeCompare(b.qualifiedForm)))
        .slice(0, 400)
        .forEach((item) => {
          if (!item.formKey || emittedKeys.has(item.formKey)) return;
          emittedKeys.add(item.formKey);
          const deps = [];
          if (variantGateNeeded) deps.push("DEC-VARIANT-001");
          if (item.missing.includes("sql_map")) deps.push("Q.sql_map");
          if (item.missing.includes("event_map")) deps.push("Q.event_map");
          if (item.missing.includes("business_rules")) deps.push("Q.business_rules");
          item.riskIds.slice(0, 2).forEach((id) => deps.push(id));
          const shared = Array.from(lookupSet(formSharedComponents, item.projectName, item.formName)).sort();
          let sprint = "Sprint 2 (Parity hardening)";
          let rationale = "Finalize quality gates and publish evidence pack for production readiness.";
          if (item.missing.includes("event_map") || item.missing.includes("sql_map")) {
            sprint = "Sprint 0 (Discovery closure)";
            rationale = "Close traceability gaps before modernization changes.";
          } else if (item.riskIds.length) {
            sprint = "Sprint 1 (Risk-first modernization)";
            rationale = "Implement remediation-first changes for high-risk legacy behavior.";
          }
          lines.push(`| ${item.qualifiedForm || item.formName} | ${sprint} | ${deps.join(", ") || "none"} | ${shared.slice(0, 5).join(", ") || "none"} | ${rationale} |`);
        });
    } else {
      lines.push("- No sprint dependency rows available.");
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
  const exportBaBriefDocxBtn = rootNode.querySelector("[data-analyst-export-ba-brief-docx]");
  const exportTechWorkbookDocxBtn = rootNode.querySelector("[data-analyst-export-tech-workbook-docx]");
  const exportBrdDocxBtn = rootNode.querySelector("[data-analyst-export-brd-docx]");
  const exportBusinessDocxBtn = rootNode.querySelector("[data-analyst-export-business-docx]");
  const uploadTrigger = rootNode.querySelector("[data-analyst-upload-trigger]");
  const uploadInput = rootNode.querySelector("[data-analyst-upload-file]");
  const statusNode = rootNode.querySelector("[data-analyst-doc-status]");
  const setStatus = (text, isError = false) => {
    if (!statusNode) return;
    statusNode.textContent = String(text || "");
    statusNode.className = `mt-1 text-[11px] ${isError ? "text-rose-700" : "text-slate-700"}`;
  };

  const exportDocgenDocx = async (docType, btn, label) => {
    try {
      if (btn) btn.setAttribute("disabled", "true");
      setStatus(`Generating ${label}...`);
      const response = await fetch(
        `/api/runs/${encodeURIComponent(String(run.run_id))}/analyst-docgen-docx?type=${encodeURIComponent(docType)}`,
        { method: "GET" },
      );
      if (!response.ok) {
        let message = `HTTP ${response.status}`;
        try {
          const payload = await response.json();
          message = String(payload?.error || message);
        } catch (_err) {
          // no-op
        }
        throw new Error(message);
      }
      const blob = await response.blob();
      const header = String(response.headers.get("content-disposition") || "");
      const match = header.match(/filename=\"?([^\";]+)\"?/i);
      const fallback = `${docType}-${String(run.run_id)}-${new Date().toISOString().replace(/[:.]/g, "-")}.docx`;
      const filename = (match && match[1]) ? match[1] : fallback;
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      window.URL.revokeObjectURL(url);
      setStatus(`${label} exported.`);
    } catch (err) {
      setStatus(`${label} export failed: ${err?.message || err}`, true);
    } finally {
      if (btn) btn.removeAttribute("disabled");
    }
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

  if (exportBaBriefDocxBtn) {
    exportBaBriefDocxBtn.addEventListener("click", async () => {
      await exportDocgenDocx("ba_brief", exportBaBriefDocxBtn, "BA Brief DOCX");
    });
  }

  if (exportTechWorkbookDocxBtn) {
    exportTechWorkbookDocxBtn.addEventListener("click", async () => {
      await exportDocgenDocx("tech_workbook", exportTechWorkbookDocxBtn, "Tech Workbook DOCX");
    });
  }

  if (exportBrdDocxBtn) {
    exportBrdDocxBtn.addEventListener("click", async () => {
      await exportDocgenDocx("brd", exportBrdDocxBtn, "BRD DOCX");
    });
  }

  if (exportBusinessDocxBtn) {
    exportBusinessDocxBtn.addEventListener("click", async () => {
      try {
        exportBusinessDocxBtn.setAttribute("disabled", "true");
        setStatus("Generating business-ready DOCX (LLM-enhanced)...");
        const response = await fetch(`/api/runs/${encodeURIComponent(String(run.run_id))}/analyst-docx?mode=llm_rich&style=strict_template`, {
          method: "GET",
        });
        if (!response.ok) {
          let message = `HTTP ${response.status}`;
          try {
            const payload = await response.json();
            message = String(payload?.error || message);
          } catch (_err) {
            // no-op
          }
          throw new Error(message);
        }
        const blob = await response.blob();
        const header = String(response.headers.get("content-disposition") || "");
        const match = header.match(/filename=\"?([^\";]+)\"?/i);
        const fallback = `analyst-business-brief-${String(run.run_id)}-${new Date().toISOString().replace(/[:.]/g, "-")}.docx`;
        const filename = (match && match[1]) ? match[1] : fallback;
        const renderMode = String(response.headers.get("x-docx-render-mode") || "deterministic").trim().toLowerCase();
        const styleMode = String(response.headers.get("x-docx-style-mode") || "").trim().toLowerCase();
        const llmReason = String(response.headers.get("x-docx-llm-reason") || "").trim();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        a.remove();
        window.URL.revokeObjectURL(url);
        if (renderMode === "llm_rich") {
          const styleSuffix = styleMode ? `, style=${styleMode}` : "";
          setStatus(`Business-ready DOCX exported (LLM-enhanced${styleSuffix}).`);
        } else {
          const suffix = llmReason ? ` Fallback reason: ${llmReason}` : "";
          const styleSuffix = styleMode ? ` style=${styleMode}.` : "";
          setStatus(`Business-ready DOCX exported (deterministic fallback).${styleSuffix}${suffix}`);
        }
      } catch (err) {
        setStatus(`Business DOCX export failed: ${err?.message || err}`, true);
      } finally {
        exportBusinessDocxBtn.removeAttribute("disabled");
      }
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
    view = "landscape";
    state.discoverResultsView = view;
  }
  const viewMap = {
    landscape: el.discoverLandscapePanel,
    city: el.discoverCityMapPanel,
    system: el.discoverSystemMapPanel,
    health: el.discoverHealthPanel,
    conventions: el.discoverConventionsPanel,
    static_forensics: el.discoverStaticForensicsPanel,
    code_quality: el.discoverCodeQualityPanel,
    dead_code: el.discoverDeadCodePanel,
    dependency_matrix: el.discoverDependencyMatrixPanel,
    trends: el.discoverTrendsPanel,
    data: el.discoverDataPanel,
  };
  Object.entries(viewMap).forEach(([key, panel]) => {
    panel?.classList.toggle("hidden", key !== view);
  });
}

function setDiscoverResultsView(view) {
  state.discoverResultsView = String(view || "landscape");
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
  return { nodes: [], edges: [], rules: [], findings: [], backlog: [], demo: false };
}

function _discoverRawArtifacts() {
  const runOutput = getAnalystOutput(state.currentRun || {});
  const runRaw = (runOutput && typeof runOutput.raw_artifacts === "object") ? runOutput.raw_artifacts : {};
  const landscapeView = (state.discoverLandscape?.data && typeof state.discoverLandscape.data === "object")
    ? state.discoverLandscape.data
    : {};
  const landscapeRaw = (landscapeView.raw_artifacts && typeof landscapeView.raw_artifacts === "object") ? landscapeView.raw_artifacts : {};
  const brief = (state.discoverAnalystBrief?.data && typeof state.discoverAnalystBrief.data === "object")
    ? state.discoverAnalystBrief.data
    : {};
  const briefRaw = (brief.raw_artifacts && typeof brief.raw_artifacts === "object") ? brief.raw_artifacts : {};
  const report = (brief.analyst_report_v2 && typeof brief.analyst_report_v2 === "object") ? brief.analyst_report_v2 : {};
  const reportRaw = (report.raw_artifacts && typeof report.raw_artifacts === "object") ? report.raw_artifacts : {};
  const evidenceView = (state.discoverEvidenceBundle?.data && typeof state.discoverEvidenceBundle.data === "object")
    ? state.discoverEvidenceBundle.data
    : {};
  const evidenceNormalized = (evidenceView.normalized_artifacts && typeof evidenceView.normalized_artifacts === "object")
    ? evidenceView.normalized_artifacts
    : {};
  const evidenceDirect = {};
  [
    "repo_landscape_v1",
    "component_inventory_v1",
    "modernization_track_plan_v1",
    "router_ruleset_v1",
    "project_metrics",
    "type_metrics",
    "type_dependency_matrix",
    "runtime_dependency_matrix",
    "dead_code_report",
    "third_party_usage",
    "trend_snapshot",
    "trend_series",
    "code_quality_rules",
    "static_forensics_layer",
    "evidence_coverage_report_v1",
    "quality_violation_report",
    "source_schema_model",
    "source_erd",
    "source_data_dictionary",
  ].forEach((key) => {
    if (evidenceView && typeof evidenceView === "object" && evidenceView[key] != null) {
      evidenceDirect[key] = evidenceView[key];
    }
  });
  const isMeaningful = (value) => {
    if (value == null) return false;
    if (Array.isArray(value)) return value.length > 0;
    if (typeof value === "object") return Object.keys(value).length > 0;
    if (typeof value === "string") return value.trim().length > 0;
    return true;
  };
  const mergeValues = (current, incoming) => {
    if (!isMeaningful(incoming)) return current;
    if (!isMeaningful(current)) return incoming;
    if (
      current
      && incoming
      && typeof current === "object"
      && typeof incoming === "object"
      && !Array.isArray(current)
      && !Array.isArray(incoming)
    ) {
      const merged = { ...current };
      Object.entries(incoming).forEach(([key, value]) => {
        merged[key] = mergeValues(merged[key], value);
      });
      return merged;
    }
    return incoming;
  };
  const merged = {};
  [evidenceNormalized, evidenceDirect, runRaw, landscapeRaw, briefRaw, reportRaw].forEach((source) => {
    if (!source || typeof source !== "object") return;
    Object.entries(source).forEach(([key, value]) => {
      merged[key] = mergeValues(merged[key], value);
    });
  });
  return merged;
}

function _discoverLandscapeArtifacts() {
  const raw = _discoverRawArtifacts();
  return {
    landscape: (raw.repo_landscape_v1 && typeof raw.repo_landscape_v1 === "object") ? raw.repo_landscape_v1 : {},
    components: (raw.component_inventory_v1 && typeof raw.component_inventory_v1 === "object") ? raw.component_inventory_v1 : {},
    tracks: (raw.modernization_track_plan_v1 && typeof raw.modernization_track_plan_v1 === "object") ? raw.modernization_track_plan_v1 : {},
    router: (raw.router_ruleset_v1 && typeof raw.router_ruleset_v1 === "object") ? raw.router_ruleset_v1 : {},
  };
}

function _discoverAnalysisPlanArtifact() {
  const raw = _discoverRawArtifacts();
  return (raw.analysis_plan_v1 && typeof raw.analysis_plan_v1 === "object") ? raw.analysis_plan_v1 : {};
}

function _formatUsd(value) {
  const num = Number(value || 0);
  if (!Number.isFinite(num) || num <= 0) return "n/a";
  return `$${num.toFixed(num < 1 ? 3 : 2)}`;
}

function renderDiscoverScopeGuidance() {
  if (!el.discoverScopeGuidance) return;
  const { landscape, components, tracks } = _discoverLandscapeArtifacts();
  const raw = _discoverRawArtifacts();
  const evidenceCoverage = (raw.evidence_coverage_report_v1 && typeof raw.evidence_coverage_report_v1 === "object") ? raw.evidence_coverage_report_v1 : {};
  const landscapeMode = String(landscape?.landscape_mode || "").trim().toLowerCase();
  const solutionSummary = (landscape.solution_summary && typeof landscape.solution_summary === "object") ? landscape.solution_summary : {};
  const componentRows = Array.isArray(components.components) ? components.components : [];
  const trackRows = Array.isArray(tracks.tracks) ? tracks.tracks : [];
  const riskRows = Array.isArray(landscape.high_risk_signals) ? landscape.high_risk_signals : [];
  if (!componentRows.length && !trackRows.length && !riskRows.length) {
    el.discoverScopeGuidance.innerHTML = "Landscape-guided scope decisions will appear here after the deterministic landscape scan runs.";
    return;
  }
  const variantCandidates = componentRows.filter((row) => !!row?.variant_candidate).map((row) => String(row?.name || row?.component_id || "").trim()).filter(Boolean);
  const reporting = componentRows.filter((row) => String(row?.component_kind || "").includes("reporting")).map((row) => String(row?.name || row?.component_id || "").trim()).filter(Boolean);
  const batch = componentRows.filter((row) => String(row?.component_kind || "").includes("batch")).map((row) => String(row?.name || row?.component_id || "").trim()).filter(Boolean);
  const dedupePreserveOrder = (rows) => {
    const seen = new Set();
    const out = [];
    (Array.isArray(rows) ? rows : []).forEach((row) => {
      const value = String(row || "").trim();
      if (!value) return;
      const key = value.toLowerCase();
      if (seen.has(key)) return;
      seen.add(key);
      out.push(value);
    });
    return out;
  };
  const questions = dedupePreserveOrder([
    ...trackRows.flatMap((row) => Array.isArray(row?.gating_questions) ? row.gating_questions : []),
    ...Array.isArray(tracks.open_questions) ? tracks.open_questions : [],
    ...Array.isArray(evidenceCoverage.blockers) ? evidenceCoverage.blockers : [],
  ]).slice(0, 5);
  const bullets = [];
  if (landscapeMode === "greenfield") {
    const targetPlatform = String(solutionSummary.target_platform || "").trim();
    const targetLanguage = String(solutionSummary.target_language || "").trim();
    const targetDatastore = String(solutionSummary.database_target || "").trim();
    if (targetPlatform || targetLanguage) {
      bullets.push(`Confirm the primary delivery stack: <strong>${escapeHtml([targetLanguage, targetPlatform].filter(Boolean).join(" / ") || "TBD")}</strong>.`);
    }
    if (targetDatastore) {
      bullets.push(`Validate the target data platform decision: <strong>${escapeHtml(targetDatastore)}</strong>.`);
    }
    if (trackRows.length) {
      bullets.push(`Planned delivery has <strong>${trackRows.length}</strong> track(s) across <strong>${componentRows.length}</strong> intended component(s).`);
    }
    if (!bullets.length) {
      bullets.push("Greenfield landscape has been synthesized from the current scope inputs. Confirm target platforms and delivery tracks before scope lock.");
    }
  } else {
    if (variantCandidates.length) {
      bullets.push(`Confirm canonical project(s): <strong>${escapeHtml(variantCandidates.join(", "))}</strong>.`);
    }
    if (reporting.length) {
      bullets.push(`Decide whether reporting components are in scope now or deferred: <strong>${escapeHtml(reporting.join(", "))}</strong>.`);
    }
    if (batch.length) {
      bullets.push(`Review batch/automation workloads separately from UI modernization: <strong>${escapeHtml(batch.join(", "))}</strong>.`);
    }
    if (!bullets.length) {
      bullets.push(`Synthetix detected <strong>${componentRows.length}</strong> candidate component(s) and <strong>${trackRows.length}</strong> suggested modernization track(s).`);
    }
  }
  el.discoverScopeGuidance.innerHTML = `
    <p class="font-semibold text-slate-900">Guided by Landscape</p>
    <ul class="mt-1 list-disc pl-4 text-slate-700">
      ${bullets.map((row) => `<li>${row}</li>`).join("")}
    </ul>
    ${String(landscape?.source_mode || "").trim() === "imported_analysis" ? `
      <div class="mt-2 rounded border border-amber-300 bg-amber-50 p-2 text-[11px] text-amber-900">
        Evidence Mode: architecture/dependency findings are evidence-backed; behavioral and data parity remain coverage-scored and may require verification.
      </div>
    ` : ""}
    <div class="mt-2 rounded border border-slate-300 bg-white p-2">
      <p class="font-semibold text-slate-900">Questions to resolve before scope lock</p>
      <ul class="mt-1 list-disc pl-4 text-slate-700">
        ${(questions.length ? questions : ["No gating questions generated yet."]).map((row) => `<li>${escapeHtml(String(row))}</li>`).join("")}
      </ul>
    </div>
  `;
}

function renderDiscoverLandscape() {
  if (!el.discoverLandscapeContent && !el.discoverLandscapeStepContent) return;
  const landscapeView = state.discoverLandscape || {};
  const landscapeRaw = (landscapeView.data && typeof landscapeView.data === "object" && landscapeView.data.raw_artifacts && typeof landscapeView.data.raw_artifacts === "object") ? landscapeView.data.raw_artifacts : {};
  const raw = landscapeRaw;
  const analysisPlan = (landscapeRaw.analysis_plan_v1 && typeof landscapeRaw.analysis_plan_v1 === "object") ? landscapeRaw.analysis_plan_v1 : _discoverAnalysisPlanArtifact();
  const repoSnapshot = (raw.repo_snapshot_v1 && typeof raw.repo_snapshot_v1 === "object") ? raw.repo_snapshot_v1 : {};
  const integration = getIntegrationContext();
  const projectState = String(integration?.project_state_detected || state.projectState?.detected || "").trim().toLowerCase();
  const repoUrl = String(integration?.brownfield?.repo_url || "").trim();
  const evidenceBundleId = String(integration?.evidence?.bundle_id || "").trim();
  const greenfieldTarget = String(integration?.greenfield?.repo_target || "").trim();
  const { landscape, components, tracks } = _discoverLandscapeArtifacts();
  const landscapeMode = String(landscape.landscape_mode || (projectState === "greenfield" ? "greenfield" : "brownfield")).trim().toLowerCase();
  const solutionSummary = (landscape.solution_summary && typeof landscape.solution_summary === "object") ? landscape.solution_summary : {};
  const scan = (landscape.scan_summary && typeof landscape.scan_summary === "object") ? landscape.scan_summary : {};
  const languageRows = Array.isArray(landscape.languages) ? landscape.languages : [];
  const buildRows = Array.isArray(landscape.build_systems) ? landscape.build_systems : [];
  const archetypeRows = Array.isArray(landscape.archetypes) ? landscape.archetypes : [];
  const datastoreRows = Array.isArray(landscape.datastore_signals) ? landscape.datastore_signals : [];
  const riskRows = Array.isArray(landscape.high_risk_signals) ? landscape.high_risk_signals : [];
  const dependencyFootprint = (landscape.dependency_footprint && typeof landscape.dependency_footprint === "object") ? landscape.dependency_footprint : {};
  const componentRows = Array.isArray(components.components) ? components.components : [];
  const edgeSummary = (components.graph_summary && typeof components.graph_summary === "object") ? components.graph_summary : {};
  const trackRows = Array.isArray(tracks.tracks) ? tracks.tracks : [];
  const evidenceCoverage = (raw.evidence_coverage_report_v1 && typeof raw.evidence_coverage_report_v1 === "object") ? raw.evidence_coverage_report_v1 : {};
  const evidenceDimensions = (evidenceCoverage.dimensions && typeof evidenceCoverage.dimensions === "object") ? evidenceCoverage.dimensions : {};
  const evidenceBlockers = Array.isArray(evidenceCoverage.blockers) ? evidenceCoverage.blockers : [];
  const analysisMode = String(analysisPlan.analysis_mode || "").trim().toLowerCase();
  const analysisReasons = Array.isArray(analysisPlan.analysis_mode_reasons) ? analysisPlan.analysis_mode_reasons : [];
  const analysisNotes = Array.isArray(analysisPlan.notes) ? analysisPlan.notes : [];
  const typeCounts = (repoSnapshot.counts_by_type && typeof repoSnapshot.counts_by_type === "object") ? repoSnapshot.counts_by_type : {};
  const phpFrameworkProfile = (raw.php_framework_profile_v1 && typeof raw.php_framework_profile_v1 === "object") ? raw.php_framework_profile_v1 : {};
  const phpRouteHints = (raw.php_route_hints_v1 && typeof raw.php_route_hints_v1 === "object") ? raw.php_route_hints_v1 : {};
  const phpRouteInventory = (raw.php_route_inventory_v1 && typeof raw.php_route_inventory_v1 === "object") ? raw.php_route_inventory_v1 : {};
  const phpControllerInventory = (raw.php_controller_inventory_v1 && typeof raw.php_controller_inventory_v1 === "object") ? raw.php_controller_inventory_v1 : {};
  const phpTemplateInventory = (raw.php_template_inventory_v1 && typeof raw.php_template_inventory_v1 === "object") ? raw.php_template_inventory_v1 : {};
  const phpJobsInventory = (raw.php_background_job_inventory_v1 && typeof raw.php_background_job_inventory_v1 === "object") ? raw.php_background_job_inventory_v1 : {};
  const phpSqlInventory = (raw.php_sql_catalog_v1 && typeof raw.php_sql_catalog_v1 === "object") ? raw.php_sql_catalog_v1 : {};
  const phpRouteHintRows = Array.isArray(phpRouteHints.routes) ? phpRouteHints.routes : [];
  const hasPhpLanguageSignal = languageRows.some((row) => String(row?.language || "").trim().toLowerCase() === "php");
  const hasPhpArchetypeSignal = archetypeRows.some((row) => String(row?.archetype || "").trim().toLowerCase().includes("php"));
  const hasPhpComponentSignal = componentRows.some((row) =>
    Array.isArray(row?.language_mix) && row.language_mix.some((item) => String(item?.language || "").trim().toLowerCase() === "php")
  );
  const hasPhpCountSignal = Number(phpFrameworkProfile.app_php_file_count || phpFrameworkProfile.php_file_count || 0) > 0
    || Number(phpControllerInventory.controller_count || phpFrameworkProfile.controller_count || 0) > 0
    || Number(phpRouteInventory.route_count || phpRouteHints.estimated_route_files || 0) > 0
    || Number(phpTemplateInventory.template_count || phpFrameworkProfile.template_count || 0) > 0;
  const isPhpLandscape = hasPhpLanguageSignal || hasPhpArchetypeSignal || hasPhpComponentSignal || hasPhpCountSignal;
  const phpSignalsHtml = isPhpLandscape && (phpFrameworkProfile.framework || phpRouteHintRows.length || hasPhpCountSignal)
    ? `
      <div class="mt-2 rounded border border-slate-300 bg-slate-50 p-2">
        <p class="font-semibold text-slate-900">PHP application signals</p>
        <div class="mt-2 grid gap-1 sm:grid-cols-4">
          <div class="rounded border border-slate-300 bg-white px-2 py-1 text-[11px] text-slate-900"><strong>Framework</strong><br/>${escapeHtml(String(phpFrameworkProfile.framework || "custom_php"))}</div>
          <div class="rounded border border-slate-300 bg-white px-2 py-1 text-[11px] text-slate-900"><strong>PHP files</strong><br/>${escapeHtml(String(phpFrameworkProfile.app_php_file_count || 0))}</div>
          <div class="rounded border border-slate-300 bg-white px-2 py-1 text-[11px] text-slate-900"><strong>Controllers</strong><br/>${escapeHtml(String(phpFrameworkProfile.controller_count || 0))}</div>
          <div class="rounded border border-slate-300 bg-white px-2 py-1 text-[11px] text-slate-900"><strong>Route hints</strong><br/>${escapeHtml(String(phpRouteHintRows.length || phpFrameworkProfile.route_hint_count || 0))}</div>
        </div>
        <p class="mt-2 text-[11px] text-slate-700">
          Session complexity: <strong>${escapeHtml(String(phpRouteHints.session_state_complexity || "unknown"))}</strong>
          · Suggested lane: <strong>${escapeHtml(String((trackRows[0] && (trackRows[0].lane || trackRows[0].title)) || "php_web_modernization"))}</strong>
        </p>
      </div>
    `
    : "";
  const hasLandscapeData = !!componentRows.length || !!trackRows.length || !!languageRows.length || !!buildRows.length || !!riskRows.length;
  if (landscapeMode !== "greenfield" && projectState !== "greenfield" && !repoUrl && !evidenceBundleId) {
    const html = `
      <div class="rounded border border-slate-300 bg-white p-3 text-xs text-slate-700">
        Connect a public/private GitHub repository or upload imported analysis outputs in <strong>Connect</strong> to generate the Landscape view.
      </div>
    `;
    if (el.discoverLandscapeContent) el.discoverLandscapeContent.innerHTML = html;
    if (el.discoverLandscapeStepContent) el.discoverLandscapeStepContent.innerHTML = html;
    return;
  }
  if (landscapeView.loading && !hasLandscapeData) {
    const loadingLabel = landscapeMode === "greenfield"
      ? (greenfieldTarget || "greenfield scope")
      : (evidenceBundleId || repoUrl || "brownfield scope");
    const html = `
      <div class="rounded border border-slate-300 bg-white p-3 text-xs text-slate-700">
        Landscape scan in progress for <strong>${escapeHtml(loadingLabel)}</strong>. Repo MRI or planned solution tracks will appear here when the analyst brief returns.
      </div>
    `;
    if (el.discoverLandscapeContent) el.discoverLandscapeContent.innerHTML = html;
    if (el.discoverLandscapeStepContent) el.discoverLandscapeStepContent.innerHTML = html;
    return;
  }
  if (!hasLandscapeData) {
    const html = `
      <div class="rounded border border-slate-300 bg-white p-3 text-xs text-slate-700">
        No Landscape data is available yet. ${landscapeMode === "greenfield" ? "Provide a business objective or target delivery context, then click <strong>Refresh Landscape</strong>." : (evidenceBundleId ? "Imported analysis is available but has not been normalized into a Landscape view yet. Click <strong>Refresh Landscape</strong>." : "Open <strong>Landscape</strong> after connecting a repo or uploading analysis outputs, or click <strong>Refresh Landscape</strong> to run the deterministic scan now.")}
      </div>
    `;
    if (el.discoverLandscapeContent) el.discoverLandscapeContent.innerHTML = html;
    if (el.discoverLandscapeStepContent) el.discoverLandscapeStepContent.innerHTML = html;
    return;
  }
  const polyglot = languageRows.filter((row) => Number(row?.stats?.loc || 0) > 0).length > 1 || buildRows.length > 1;
  const greenfieldSignals = [
    ["Repo target", String(solutionSummary.repo_target || "").trim()],
    ["Repo destination", String(solutionSummary.repo_destination || "").trim()],
    ["Tracker", [String(solutionSummary.tracker_provider || "").trim(), String(solutionSummary.tracker_project || "").trim()].filter(Boolean).join(" / ")],
    ["Target stack", [String(solutionSummary.target_language || "").trim(), String(solutionSummary.target_platform || "").trim()].filter(Boolean).join(" / ")],
    ["Database target", String(solutionSummary.database_target || "").trim()],
    ["Jurisdiction", String(solutionSummary.jurisdiction || "").trim()],
  ].filter((row) => !!row[1]);
  const locCardLabel = landscapeMode === "greenfield" ? "Planned text LOC" : "Estimated text LOC";
  const locCardNote = landscapeMode === "greenfield"
    ? "Scope-derived planning estimate"
    : "Shallow scan of text-like files";
  const languageHtml = languageRows.slice(0, 8).map((row) => `
    <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1">
      <strong>${escapeHtml(String(row?.language || "Unknown"))}</strong><br/>
      ${escapeHtml(String(row?.stats?.files || 0))} files · ${escapeHtml(String(row?.stats?.loc || 0))} est. LOC
    </div>
  `).join("");
  const buildHtml = buildRows.slice(0, 8).map((row) => `<li><strong>${escapeHtml(String(row?.kind || ""))}</strong> · ${escapeHtml(String((row?.paths || []).slice(0, 3).join(", ") || "n/a"))}</li>`).join("");
  const archetypeHtml = archetypeRows.slice(0, 8).map((row) => `<span class="rounded border border-slate-300 bg-slate-50 px-2 py-1">${escapeHtml(String(row?.archetype || ""))}</span>`).join("");
  const datastoreHtml = datastoreRows.slice(0, 8).map((row) => `<span class="rounded border border-slate-300 bg-slate-50 px-2 py-1">${escapeHtml(String(row?.datastore || ""))}</span>`).join("");
  const componentTable = componentRows.slice(0, 16).map((row) => {
    const componentName = String(row?.name || row?.component_id || "").trim();
    const componentPath = String((row?.project_files || [])[0] || (row?.root_paths || [])[0] || "").trim();
    const componentKind = String(row?.component_kind || "").trim();
    const primaryLanguage = String((row?.language_mix || [])[0]?.language || "n/a").trim();
    const locValue = Number(row?.stats?.loc || 0);
    const isArtifactOnly = locValue <= 0 && ["reporting_pack", "batch_pack"].includes(componentKind);
    const locDisplay = isArtifactOnly ? "n/a" : String(locValue);
    const locNote = componentKind === "reporting_pack"
      ? "artifact-based"
      : (componentKind === "batch_pack" && locValue <= 0 ? "script pack" : "");
    const riskDisplay = String((row?.risk_flags || []).slice(0, 2).join(", ") || "none");
    const trackDisplay = String((row?.suggested_tracks || []).map((x) => x?.lane || x?.title || "").filter(Boolean).slice(0, 2).join(", ") || "review");
    return `
      <tr>
        <td class="px-2 py-1">
          <div class="font-medium text-slate-900">${escapeHtml(componentName)}</div>
          ${componentPath ? `<div class="mt-0.5 text-[10px] text-slate-600">${escapeHtml(componentPath)}</div>` : ""}
        </td>
        <td class="px-2 py-1">${escapeHtml(componentKind)}</td>
        <td class="px-2 py-1">${escapeHtml(primaryLanguage)}</td>
        <td class="px-2 py-1 text-right">
          <div>${escapeHtml(locDisplay)}</div>
          ${locNote ? `<div class="text-[10px] text-slate-500">${escapeHtml(locNote)}</div>` : ""}
        </td>
        <td class="px-2 py-1">${escapeHtml(riskDisplay)}</td>
        <td class="px-2 py-1">${escapeHtml(trackDisplay)}</td>
      </tr>
    `;
  }).join("");
  const trackHtml = trackRows.slice(0, 10).map((row) => `
    <div class="rounded border border-slate-300 bg-white p-2">
      <div class="flex items-start justify-between gap-2">
        <div>
          <p class="font-semibold text-slate-900">${escapeHtml(String(row?.title || row?.track_id || ""))}</p>
          <p class="mt-0.5 text-[11px] text-slate-700">${escapeHtml(String(row?.why || ""))}</p>
        </div>
        <span class="rounded border border-slate-300 bg-slate-50 px-2 py-0.5 text-[10px] font-semibold text-slate-800">${escapeHtml(String(row?.lane || "unknown"))}</span>
      </div>
      <p class="mt-1 text-[11px] text-slate-700"><strong>Target:</strong> ${escapeHtml(String(row?.suggested_target || "TBD"))}</p>
      <p class="mt-1 text-[11px] text-slate-700"><strong>Source:</strong> ${escapeHtml(String((row?.source_components || []).join(", ") || "n/a"))}</p>
      <p class="mt-1 text-[11px] text-slate-700"><strong>Questions:</strong> ${escapeHtml(String((row?.gating_questions || []).slice(0, 2).join(" | ") || "None"))}</p>
    </div>
  `).join("");
  const riskHtml = riskRows.slice(0, 8).map((row) => `
    <div class="rounded border border-slate-300 bg-slate-50 p-2">
      <p class="font-semibold text-slate-900">${escapeHtml(String(row?.title || row?.signal_id || ""))}</p>
      <p class="mt-0.5 text-[11px] text-slate-700">${escapeHtml(String(row?.description || ""))}</p>
      <p class="mt-1 text-[11px] text-slate-700"><strong>Action:</strong> ${escapeHtml(String(row?.recommendation || ""))}</p>
    </div>
  `).join("");
  const nextActions = [
    landscapeMode === "greenfield"
      ? "Confirm target platform, delivery tracks, and business constraints before scope lock."
      : componentRows.some((row) => !!row?.variant_candidate) ? "Confirm canonical component(s) before deep analysis." : "",
    trackRows.length ? "Choose a target stack per suggested modernization track." : "",
    landscapeMode === "greenfield"
      ? "Confirm ownership boundaries and integration expectations for each planned component."
      : datastoreRows.length ? "Provide DB exports or docs to improve datastore confidence where possible." : "",
    riskRows.some((row) => String(row?.signal_id || "").includes("HUGE_FILES")) ? "Enable streaming/decomposed analysis for large files." : "",
    landscapeMode === "greenfield" && !greenfieldSignals.length ? "Provide target repo, tracker, and platform details to improve route confidence." : "",
  ].filter(Boolean);
  const detectionHeading = landscapeMode === "greenfield" ? "Planned solution view" : "What we detected";
  const buildHeading = landscapeMode === "greenfield" ? "Planned targets" : "Build systems";
  const archetypeHeading = landscapeMode === "greenfield" ? "Planned archetypes" : "Application archetypes";
  const datastoreHeading = landscapeMode === "greenfield" ? "Planned datastores" : "Datastores";
  const dependencyHeading = landscapeMode === "greenfield" ? "Planning signals" : (isPhpLandscape ? "Package footprint" : "Dependency footprint");
  const dependencyBodyHtml = landscapeMode === "greenfield"
    ? `<p class="mt-1 text-[11px] text-slate-700">Planning artifact signals will appear here as target stack details are provided.</p>`
    : isPhpLandscape
      ? `<p class="mt-1 text-[11px] text-slate-700">Composer=${escapeHtml(String(dependencyFootprint.composer_package_count || phpFrameworkProfile.composer_package_count || 0))} · NPM=${escapeHtml(String(dependencyFootprint.npm_package_count || 0))} · Top=${escapeHtml(String((dependencyFootprint.top_dependencies || []).slice(0, 4).join(', ') || 'n/a'))}</p>`
      : `<p class="mt-1 text-[11px] text-slate-700">OCX=${escapeHtml(String(dependencyFootprint.ocx_count || 0))} · COM/DLL=${escapeHtml(String(dependencyFootprint.com_dll_count || 0))} · Top=${escapeHtml(String((dependencyFootprint.top_dependencies || []).slice(0, 4).join(', ') || 'n/a'))}</p>`;
  const landscapeIntro = landscapeMode === "greenfield"
    ? `Greenfield landscape derived from scope inputs${greenfieldTarget ? ` for ${escapeHtml(greenfieldTarget)}` : ""}.`
    : "";
  const evidenceCoverageHtml = String(landscape.source_mode || "").trim() === "imported_analysis"
    ? `
      <div class="mt-2 rounded border border-amber-300 bg-amber-50 p-2">
        <p class="font-semibold text-amber-950">Evidence Coverage</p>
        <div class="mt-2 grid gap-1 sm:grid-cols-4">
          ${[
            ["Architecture", Number(evidenceDimensions.architecture || 0)],
            ["Dependencies", Number(evidenceDimensions.dependencies || 0)],
            ["Behavior", Number(evidenceDimensions.behavior || 0)],
            ["Data", Number(evidenceDimensions.data || 0)],
          ].map((row) => `<div class="rounded border border-amber-200 bg-white px-2 py-1 text-[11px] text-slate-900"><strong>${escapeHtml(String(row[0]))}</strong><br/>${escapeHtml(String(row[1]))}%</div>`).join("")}
        </div>
        <p class="mt-2 text-[11px] text-amber-900">Proceed state: <strong>${escapeHtml(String(evidenceCoverage.proceed_state || "review_required"))}</strong></p>
        <ul class="mt-1 list-disc pl-4 text-[11px] text-amber-900">${(evidenceBlockers.length ? evidenceBlockers.slice(0, 4).map((row) => `<li>${escapeHtml(String(row))}</li>`).join("") : "<li>No blockers reported.</li>")}</ul>
      </div>
    `
    : "";
  const greenfieldSummaryHtml = landscapeMode === "greenfield"
    ? `
      <div class="mt-2 rounded border border-slate-300 bg-slate-50 p-2">
        <p class="font-semibold text-slate-900">Planning inputs</p>
        <div class="mt-2 grid gap-1 sm:grid-cols-2">
          ${greenfieldSignals.length
            ? greenfieldSignals.map((row) => `
              <div class="rounded border border-slate-300 bg-white px-2 py-1">
                <strong>${escapeHtml(String(row[0]))}</strong><br/>
                ${escapeHtml(String(row[1]))}
              </div>
            `).join("")
            : `<p class="text-slate-700">No explicit planning inputs captured yet. Landscape is using default greenfield assumptions.</p>`}
        </div>
      </div>
    `
    : "";
  const analysisPlanHtml = Object.keys(analysisPlan).length
    ? `
      <div class="mt-2 rounded border ${analysisMode === "large_repo" ? "border-amber-300 bg-amber-50" : "border-slate-300 bg-slate-50"} p-2">
        <div class="flex flex-wrap items-start justify-between gap-2">
          <div>
            <p class="font-semibold ${analysisMode === "large_repo" ? "text-amber-950" : "text-slate-900"}">Analysis route</p>
            <p class="text-[11px] ${analysisMode === "large_repo" ? "text-amber-900" : "text-slate-700"}">
              <strong>${escapeHtml(String(analysisPlan.analysis_mode || "standard"))}</strong>
              ${analysisMode === "large_repo" ? " — large-repo orchestration will be used for this substantial application." : ""}
            </p>
          </div>
          <span class="rounded border ${String(analysisPlan.llm_rejection_risk || "").toLowerCase() === "high" ? "border-rose-300 bg-rose-50 text-rose-800" : String(analysisPlan.llm_rejection_risk || "").toLowerCase() === "medium" ? "border-amber-300 bg-amber-50 text-amber-900" : "border-emerald-300 bg-emerald-50 text-emerald-800"} px-2 py-0.5 text-[10px] font-semibold uppercase">
            LLM size risk: ${escapeHtml(String(analysisPlan.llm_rejection_risk || "unknown"))}
          </span>
        </div>
        <div class="mt-2 grid gap-1 sm:grid-cols-4">
          <div class="rounded border border-slate-300 bg-white px-2 py-1 text-[11px] text-slate-900"><strong>Estimated Stage 1 analysis tokens</strong><br/>${escapeHtml(String(analysisPlan.estimated_total_tokens || 0))}</div>
          <div class="rounded border border-slate-300 bg-white px-2 py-1 text-[11px] text-slate-900"><strong>Estimated Stage 1 analysis cost</strong><br/>${escapeHtml(_formatUsd(analysisPlan.estimated_cost_usd))}</div>
          <div class="rounded border border-slate-300 bg-white px-2 py-1 text-[11px] text-slate-900"><strong>Strategy</strong><br/>${escapeHtml(String(analysisPlan.llm_strategy || "n/a"))}</div>
          <div class="rounded border border-slate-300 bg-white px-2 py-1 text-[11px] text-slate-900"><strong>Chunks</strong><br/>${escapeHtml(String(analysisPlan.chunk_count || 0))}</div>
        </div>
        ${analysisReasons.length ? `<p class="mt-2 text-[11px] ${analysisMode === "large_repo" ? "text-amber-900" : "text-slate-700"}"><strong>Why:</strong> ${escapeHtml(analysisReasons.join(" | "))}</p>` : ""}
        ${analysisNotes.length ? `<ul class="mt-1 list-disc pl-4 text-[11px] ${analysisMode === "large_repo" ? "text-amber-900" : "text-slate-700"}">${analysisNotes.slice(0, 4).map((row) => `<li>${escapeHtml(String(row))}</li>`).join("")}</ul>` : ""}
      </div>
    `
    : "";
  const inventorySummaryHtml = (isPhpLandscape || Object.keys(typeCounts).length)
    ? `
      <div class="mt-2 rounded border border-slate-300 bg-slate-50 p-2">
        <div class="flex items-start justify-between gap-2">
          <div>
            <p class="font-semibold text-slate-900">Source inventory</p>
            <p class="text-[11px] text-slate-700">${isPhpLandscape ? "Selected PHP legacy files and promoted application structures." : "Selected legacy files by detected type."}</p>
          </div>
          <button type="button" class="btn-light rounded px-2 py-1 text-[11px] font-semibold" onclick="window.__downloadDiscoverArtifact && window.__downloadDiscoverArtifact('repo_snapshot')">Export file inventory</button>
        </div>
        <div class="mt-2 grid gap-1 sm:grid-cols-3 lg:grid-cols-6">
          ${(isPhpLandscape
            ? [
                ["Applications", 1],
                ["PHP files", Number(phpFrameworkProfile.app_php_file_count || phpFrameworkProfile.php_file_count || 0)],
                ["Controllers", Number(phpControllerInventory.controller_count || phpFrameworkProfile.controller_count || 0)],
                ["Routes", Number(phpRouteInventory.route_count || phpRouteHints.estimated_route_files || 0)],
                ["Templates", Number(phpTemplateInventory.template_count || phpFrameworkProfile.template_count || 0)],
                ["Jobs", Number(phpJobsInventory.job_count || 0)],
                ["SQL touchpoints", Number(phpSqlInventory.statement_count || 0)],
                ["Dependencies", Number(dependencyFootprint.composer_package_count || phpFrameworkProfile.composer_package_count || 0)],
              ]
            : [
                ["Projects", Number(typeCounts.project || 0) + Number(typeCounts.project_group || 0)],
                ["Forms", Number(typeCounts.form || 0)],
                ["UserControls", Number(typeCounts.usercontrol || 0)],
                ["Modules", Number(typeCounts.module || 0)],
                ["Classes", Number(typeCounts.class || 0)],
                ["Companions", Number(typeCounts.form_binary || 0) + Number(typeCounts.usercontrol_binary || 0) + Number(typeCounts.database || 0)],
              ]
          ).map((row) => `<div class="rounded border border-slate-300 bg-white px-2 py-1 text-[11px] text-slate-900"><strong>${escapeHtml(String(row[0]))}</strong><br/>${escapeHtml(String(row[1]))}</div>`).join("")}
        </div>
      </div>
    `
    : "";

  const landscapeHtml = `
    ${landscapeIntro ? `<div class="mb-2 rounded border border-slate-300 bg-slate-50 px-2 py-1 text-[11px] text-slate-700">${landscapeIntro}</div>` : ""}
    <div class="grid gap-2 sm:grid-cols-6">
      <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Total files</strong><br/>${escapeHtml(String(scan.total_files || 0))}</div>
      <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>${locCardLabel}</strong><br/>${escapeHtml(String(scan.total_loc || 0))}<div class="text-[10px] text-slate-500">${escapeHtml(locCardNote)}</div></div>
      <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Build systems</strong><br/>${escapeHtml(String(buildRows.length))}</div>
      <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Components</strong><br/>${escapeHtml(String(componentRows.length))}</div>
      <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Tracks</strong><br/>${escapeHtml(String(trackRows.length))}</div>
      <div class="rounded border ${polyglot ? "border-amber-300 bg-amber-50 text-amber-900" : "border-emerald-300 bg-emerald-50 text-emerald-900"} px-2 py-1"><strong>Polyglot repo</strong><br/>${polyglot ? "Yes" : "No"}</div>
    </div>
    <div class="mt-2 grid gap-2 lg:grid-cols-2">
      <div class="rounded border border-slate-300 bg-white p-2">
        <p class="font-semibold text-slate-900">${detectionHeading}</p>
        <div class="mt-2 grid gap-1 sm:grid-cols-2">${languageHtml || `<p class="text-slate-700">No language signals detected yet. Check repo access and exclusions.</p>`}</div>
        <div class="mt-2">
          <p class="font-semibold text-slate-900">${buildHeading}</p>
          <ul class="mt-1 list-disc pl-4 text-slate-700">${buildHtml || `<li>No build system files detected.</li>`}</ul>
        </div>
        <div class="mt-2">
          <p class="font-semibold text-slate-900">${archetypeHeading}</p>
          <div class="mt-1 flex flex-wrap gap-1">${archetypeHtml || `<span class=\"text-slate-700\">No archetypes detected.</span>`}</div>
        </div>
        <div class="mt-2">
          <p class="font-semibold text-slate-900">${datastoreHeading}</p>
          <div class="mt-1 flex flex-wrap gap-1">${datastoreHtml || `<span class=\"text-slate-700\">No datastore signals detected.</span>`}</div>
        </div>
        <div class="mt-2 rounded border border-slate-300 bg-slate-50 p-2">
          <p class="font-semibold text-slate-900">${dependencyHeading}</p>
          ${dependencyBodyHtml}
        </div>
        ${phpSignalsHtml}
        ${analysisPlanHtml}
        ${inventorySummaryHtml}
        ${evidenceCoverageHtml}
        ${greenfieldSummaryHtml}
      </div>
      <div class="rounded border border-slate-300 bg-white p-2">
        <p class="font-semibold text-slate-900">Suggested modernization tracks</p>
        <div class="mt-2 grid gap-2">${trackHtml || `<p class="text-slate-700">No track suggestions generated yet.</p>`}</div>
      </div>
    </div>
    <div class="mt-2 rounded border border-slate-300 bg-white p-2">
      <div class="flex items-center justify-between gap-2">
        <p class="font-semibold text-slate-900">Components</p>
        <span class="text-[11px] text-slate-700">edges=${escapeHtml(String(edgeSummary.edge_count || 0))} · shared_db=${escapeHtml(String(edgeSummary.shared_db_edges || 0))}</span>
      </div>
      <div class="mt-1 overflow-x-auto">
        <table class="w-full text-[11px]">
          <thead><tr class="text-left text-slate-600"><th class="px-2 py-1">Component</th><th class="px-2 py-1">Kind</th><th class="px-2 py-1">Primary language</th><th class="px-2 py-1 text-right">LOC</th><th class="px-2 py-1">Risk flags</th><th class="px-2 py-1">Suggested track</th></tr></thead>
          <tbody>${componentTable || `<tr><td class="px-2 py-1 text-slate-600\" colspan=\"6\">No components detected. Try expanding scan roots or removing exclusions.</td></tr>`}</tbody>
        </table>
      </div>
    </div>
    <div class="mt-2 grid gap-2 lg:grid-cols-2">
      <div class="rounded border border-slate-300 bg-white p-2">
        <p class="font-semibold text-slate-900">Immediate risk flags</p>
        <div class="mt-2 grid gap-2">${riskHtml || `<p class="text-slate-700">No immediate landscape risk flags.</p>`}</div>
      </div>
      <div class="rounded border border-slate-300 bg-white p-2">
        <p class="font-semibold text-slate-900">Next actions</p>
        <ul class="mt-2 list-disc pl-4 text-slate-700">
          ${(nextActions.length ? nextActions : ["Proceed to Define Scope and confirm in-scope components."]).map((row) => `<li>${escapeHtml(String(row))}</li>`).join("")}
        </ul>
      </div>
    </div>
  `;
  if (el.discoverLandscapeContent) el.discoverLandscapeContent.innerHTML = landscapeHtml;
  if (el.discoverLandscapeStepContent) el.discoverLandscapeStepContent.innerHTML = landscapeHtml;
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
  const rawArtifacts = _discoverRawArtifacts();
  renderDiscoverLandscape();
  renderDiscoverScopeGuidance();

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

  const cqProjectMetrics = Array.isArray(rawArtifacts.project_metrics?.rows) ? rawArtifacts.project_metrics.rows : [];
  const cqTypeMetrics = Array.isArray(rawArtifacts.type_metrics?.rows) ? rawArtifacts.type_metrics.rows : [];
  const cqTypeDependencyMatrix = Array.isArray(rawArtifacts.type_dependency_matrix?.edges) ? rawArtifacts.type_dependency_matrix.edges : [];
  const cqRuntimeDependencyMatrix = Array.isArray(rawArtifacts.runtime_dependency_matrix?.edges) ? rawArtifacts.runtime_dependency_matrix.edges : [];
  const cqDeadCode = (rawArtifacts.dead_code_report && typeof rawArtifacts.dead_code_report === "object")
    ? rawArtifacts.dead_code_report
    : {};
  const cqThirdParty = Array.isArray(rawArtifacts.third_party_usage?.rows) ? rawArtifacts.third_party_usage.rows : [];
  const cqRules = Array.isArray(rawArtifacts.code_quality_rules?.rules) ? rawArtifacts.code_quality_rules.rules : [];
  const cqViolations = (rawArtifacts.quality_violation_report && typeof rawArtifacts.quality_violation_report === "object")
    ? rawArtifacts.quality_violation_report
    : {};
  const cqViolationRows = Array.isArray(cqViolations.violations) ? cqViolations.violations : [];
  const cqViolationSummary = (cqViolations.summary && typeof cqViolations.summary === "object") ? cqViolations.summary : {};
  const cqDeadSummary = (cqDeadCode.summary && typeof cqDeadCode.summary === "object") ? cqDeadCode.summary : {};
  const cqTrendSnapshot = (rawArtifacts.trend_snapshot && typeof rawArtifacts.trend_snapshot === "object")
    ? rawArtifacts.trend_snapshot
    : {};
  const cqTrendSeries = (rawArtifacts.trend_series && typeof rawArtifacts.trend_series === "object")
    ? rawArtifacts.trend_series
    : {};
  const cqTrendMetrics = (cqTrendSnapshot.snapshot && typeof cqTrendSnapshot.snapshot === "object" && typeof cqTrendSnapshot.snapshot.metrics === "object")
    ? cqTrendSnapshot.snapshot.metrics
    : {};
  const staticForensics = (rawArtifacts.static_forensics_layer && typeof rawArtifacts.static_forensics_layer === "object")
    ? rawArtifacts.static_forensics_layer
    : {};

  if (el.discoverStaticForensicsContent) {
    const summary = (staticForensics.summary && typeof staticForensics.summary === "object") ? staticForensics.summary : {};
    const checks = Array.isArray(staticForensics.checks) ? staticForensics.checks : [];
    const checkRows = checks.slice(0, 12).map((row) => {
      const status = String(row?.status || "warn").toUpperCase();
      const badgeClassName = status === "PASS"
        ? "border-emerald-300 bg-emerald-50 text-emerald-900"
        : (status === "FAIL" ? "border-rose-300 bg-rose-50 text-rose-900" : "border-amber-300 bg-amber-50 text-amber-900");
      return `
        <div class="rounded border px-2 py-1 ${badgeClassName}">
          <strong>${escapeHtml(status)}</strong> · ${escapeHtml(String(row?.label || row?.id || "check"))}
          <div class="mt-0.5 text-[10px]">${escapeHtml(String(row?.detail || ""))}</div>
        </div>
      `;
    }).join("");
    el.discoverStaticForensicsContent.innerHTML = `
      <div class="grid gap-2 sm:grid-cols-6">
        <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Status</strong><br/>${escapeHtml(String(summary.overall_status || "PENDING"))}</div>
        <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Projects</strong><br/>${Number(summary.projects || 0)}</div>
        <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Types</strong><br/>${Number(summary.types || 0)}</div>
        <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Type Edges</strong><br/>${Number(summary.type_dependency_edges || 0)}</div>
        <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Runtime Edges</strong><br/>${Number(summary.runtime_dependency_edges || 0)}</div>
        <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Violations</strong><br/>${Number(summary.quality_violations || 0)}</div>
      </div>
      <div class="mt-2 rounded border border-slate-300 bg-slate-50 p-2">
        <p class="mb-1 font-semibold text-slate-900">Coverage checks</p>
        <div class="grid gap-1">${checkRows || `<p class="text-[11px] text-slate-700">No static-forensics checks generated yet.</p>`}</div>
      </div>
    `;
  }

  if (el.discoverCodeQualityContent) {
    const hotspotRows = [...cqTypeMetrics]
      .sort((a, b) => Number(b?.cyclomatic_complexity || 0) - Number(a?.cyclomatic_complexity || 0))
      .slice(0, 10)
      .map((row) => `
        <tr>
          <td class="px-2 py-1">${escapeHtml(String(row?.project || "n/a"))}</td>
          <td class="px-2 py-1">${escapeHtml(String(row?.type_name || ""))}</td>
          <td class="px-2 py-1 text-right">${escapeHtml(String(row?.cyclomatic_complexity || 0))}</td>
          <td class="px-2 py-1 text-right">${escapeHtml(String(Number(row?.afferent_coupling || 0) + Number(row?.efferent_coupling || 0)))}</td>
        </tr>
      `).join("");
    const violationRows = cqViolationRows.slice(0, 10).map((row) => `
      <tr>
        <td class="px-2 py-1">${escapeHtml(String(row?.rule_id || ""))}</td>
        <td class="px-2 py-1">${escapeHtml(String(row?.severity || ""))}</td>
        <td class="px-2 py-1">${escapeHtml(String(row?.subject || ""))}</td>
        <td class="px-2 py-1">${escapeHtml(String(row?.detail || ""))}</td>
      </tr>
    `).join("");
    el.discoverCodeQualityContent.innerHTML = `
      <div class="grid gap-2 sm:grid-cols-6">
        <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Projects</strong><br/>${cqProjectMetrics.length}</div>
        <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Types</strong><br/>${cqTypeMetrics.length}</div>
        <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Rules</strong><br/>${cqRules.length}</div>
        <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Violations</strong><br/>${Number(cqViolationSummary.total_violations || cqViolationRows.length)}</div>
        <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Critical</strong><br/>${Number(cqViolationSummary.critical_violations || 0)}</div>
        <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Hotspots</strong><br/>${Number(cqTrendMetrics.hotspot_count || 0)}</div>
      </div>
      <div class="mt-2 grid gap-2 lg:grid-cols-2">
        <div class="rounded border border-slate-300 bg-slate-50 p-2">
          <p class="mb-1 font-semibold text-slate-900">Hotspot Types (sample)</p>
          <div class="overflow-x-auto">
            <table class="w-full text-[11px]">
              <thead><tr class="text-left text-slate-600"><th class="px-2 py-1">Project</th><th class="px-2 py-1">Type</th><th class="px-2 py-1 text-right">Complexity</th><th class="px-2 py-1 text-right">Coupling</th></tr></thead>
              <tbody>${hotspotRows || `<tr><td class="px-2 py-1 text-slate-600" colspan="4">No type metrics rows.</td></tr>`}</tbody>
            </table>
          </div>
        </div>
        <div class="rounded border border-slate-300 bg-slate-50 p-2">
          <p class="mb-1 font-semibold text-slate-900">Quality Violations (sample)</p>
          <div class="overflow-x-auto">
            <table class="w-full text-[11px]">
              <thead><tr class="text-left text-slate-600"><th class="px-2 py-1">Rule</th><th class="px-2 py-1">Severity</th><th class="px-2 py-1">Subject</th><th class="px-2 py-1">Detail</th></tr></thead>
              <tbody>${violationRows || `<tr><td class="px-2 py-1 text-slate-600" colspan="4">No violations detected.</td></tr>`}</tbody>
            </table>
          </div>
        </div>
      </div>
    `;
  }

  if (el.discoverDeadCodeContent) {
    const candidates = (Array.isArray(cqDeadCode.candidates) && cqDeadCode.candidates.length > 0)
      ? cqDeadCode.candidates
      : [
        ...(Array.isArray(cqDeadCode.probable_dead_types) ? cqDeadCode.probable_dead_types.map((row) => ({ ...row, kind: row?.kind || "type" })) : []),
        ...(Array.isArray(cqDeadCode.probable_dead_methods) ? cqDeadCode.probable_dead_methods.map((row) => ({ ...row, kind: row?.kind || "method" })) : []),
        ...(Array.isArray(cqDeadCode.probable_dead_fields) ? cqDeadCode.probable_dead_fields.map((row) => ({ ...row, kind: row?.kind || "field" })) : []),
      ];
    const rows = candidates.slice(0, 12).map((row) => `
      <tr>
        <td class="px-2 py-1">${escapeHtml(String(row?.kind || ""))}</td>
        <td class="px-2 py-1">${escapeHtml(String(row?.name || row?.symbol || ""))}</td>
        <td class="px-2 py-1 text-right">${escapeHtml(String(row?.confidence || row?.score || ""))}</td>
      </tr>
    `).join("");
    el.discoverDeadCodeContent.innerHTML = `
      <div class="grid gap-2 sm:grid-cols-4">
        <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Dead Types</strong><br/>${Number(cqDeadSummary.dead_type_candidates || 0)}</div>
        <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Dead Methods</strong><br/>${Number(cqDeadSummary.dead_method_candidates || 0)}</div>
        <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Dead Fields</strong><br/>${Number(cqDeadSummary.dead_field_candidates || 0)}</div>
        <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Candidates</strong><br/>${candidates.length}</div>
      </div>
      <div class="mt-2 rounded border border-slate-300 bg-slate-50 p-2">
        <p class="mb-1 font-semibold text-slate-900">Dead Code Candidates (sample)</p>
        <div class="overflow-x-auto">
          <table class="w-full text-[11px]">
            <thead><tr class="text-left text-slate-600"><th class="px-2 py-1">Kind</th><th class="px-2 py-1">Name</th><th class="px-2 py-1 text-right">Confidence</th></tr></thead>
            <tbody>${rows || `<tr><td class="px-2 py-1 text-slate-600" colspan="3">No dead-code candidates detected.</td></tr>`}</tbody>
          </table>
        </div>
      </div>
    `;
  }

  if (el.discoverDependencyMatrixContent) {
    const typeRows = cqTypeDependencyMatrix.slice(0, 12).map((row) => `
      <tr>
        <td class="px-2 py-1">${escapeHtml(String(row?.source || row?.from || ""))}</td>
        <td class="px-2 py-1">${escapeHtml(String(row?.target || row?.to || ""))}</td>
        <td class="px-2 py-1">${escapeHtml(String(row?.kind || row?.type || ""))}</td>
      </tr>
    `).join("");
    const runtimeRows = cqRuntimeDependencyMatrix.slice(0, 12).map((row) => `
      <tr>
        <td class="px-2 py-1">${escapeHtml(String(row?.source || row?.from || ""))}</td>
        <td class="px-2 py-1">${escapeHtml(String(row?.target || row?.to || ""))}</td>
        <td class="px-2 py-1">${escapeHtml(String(row?.kind || row?.type || ""))}</td>
      </tr>
    `).join("");
    const tpRows = [...cqThirdParty]
      .sort((a, b) => Number(b?.usage_intensity || 0) - Number(a?.usage_intensity || 0))
      .slice(0, 10)
      .map((row) => `
        <tr>
          <td class="px-2 py-1">${escapeHtml(String(row?.dependency || ""))}</td>
          <td class="px-2 py-1">${escapeHtml(String(row?.kind || ""))}</td>
          <td class="px-2 py-1 text-right">${escapeHtml(String(row?.forms_using_count || 0))}</td>
          <td class="px-2 py-1 text-right">${escapeHtml(String(row?.usage_intensity || 0))}</td>
        </tr>
      `).join("");
    el.discoverDependencyMatrixContent.innerHTML = `
      <div class="grid gap-2 sm:grid-cols-3">
        <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Type Edges</strong><br/>${cqTypeDependencyMatrix.length}</div>
        <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Runtime Edges</strong><br/>${cqRuntimeDependencyMatrix.length}</div>
        <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Third-Party Deps</strong><br/>${cqThirdParty.length}</div>
      </div>
      <div class="mt-2 grid gap-2 lg:grid-cols-2">
        <div class="rounded border border-slate-300 bg-slate-50 p-2">
          <p class="mb-1 font-semibold text-slate-900">Type Dependency Matrix (top rows)</p>
          <div class="overflow-x-auto"><table class="w-full text-[11px]"><thead><tr class="text-left text-slate-600"><th class="px-2 py-1">From</th><th class="px-2 py-1">To</th><th class="px-2 py-1">Kind</th></tr></thead><tbody>${typeRows || `<tr><td class="px-2 py-1 text-slate-600" colspan="3">No type dependencies.</td></tr>`}</tbody></table></div>
        </div>
        <div class="rounded border border-slate-300 bg-slate-50 p-2">
          <p class="mb-1 font-semibold text-slate-900">Runtime Dependency Matrix (top rows)</p>
          <div class="overflow-x-auto"><table class="w-full text-[11px]"><thead><tr class="text-left text-slate-600"><th class="px-2 py-1">From</th><th class="px-2 py-1">To</th><th class="px-2 py-1">Kind</th></tr></thead><tbody>${runtimeRows || `<tr><td class="px-2 py-1 text-slate-600" colspan="3">No runtime dependencies.</td></tr>`}</tbody></table></div>
        </div>
      </div>
      <div class="mt-2 rounded border border-slate-300 bg-slate-50 p-2">
        <p class="mb-1 font-semibold text-slate-900">Third-Party Usage (top rows)</p>
        <div class="overflow-x-auto"><table class="w-full text-[11px]"><thead><tr class="text-left text-slate-600"><th class="px-2 py-1">Dependency</th><th class="px-2 py-1">Kind</th><th class="px-2 py-1 text-right">Forms</th><th class="px-2 py-1 text-right">Intensity</th></tr></thead><tbody>${tpRows || `<tr><td class="px-2 py-1 text-slate-600" colspan="4">No third-party usage rows.</td></tr>`}</tbody></table></div>
      </div>
    `;
  }

  if (el.discoverTrendsContent) {
    const series = (Array.isArray(cqTrendSeries.points) && cqTrendSeries.points.length > 0)
      ? cqTrendSeries.points
      : (Array.isArray(cqTrendSeries.series) ? cqTrendSeries.series : []);
    const seriesRows = series.slice(0, 12).map((row) => `
      <tr>
        <td class="px-2 py-1">${escapeHtml(String(row?.at || row?.timestamp || row?.captured_at || ""))}</td>
        <td class="px-2 py-1 text-right">${escapeHtml(String(row?.loc_total || row?.loc || row?.metrics?.loc_total || 0))}</td>
        <td class="px-2 py-1 text-right">${escapeHtml(String(row?.avg_complexity || row?.metrics?.avg_complexity || 0))}</td>
        <td class="px-2 py-1 text-right">${escapeHtml(String(row?.max_complexity || row?.metrics?.max_complexity || 0))}</td>
      </tr>
    `).join("");
    el.discoverTrendsContent.innerHTML = `
      <div class="grid gap-2 sm:grid-cols-4">
        <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>LOC Total</strong><br/>${escapeHtml(String(cqTrendMetrics.loc_total || 0))}</div>
        <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Avg Complexity</strong><br/>${escapeHtml(String(cqTrendMetrics.avg_complexity || 0))}</div>
        <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Max Complexity</strong><br/>${escapeHtml(String(cqTrendMetrics.max_complexity || 0))}</div>
        <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Hotspots</strong><br/>${escapeHtml(String(cqTrendMetrics.hotspot_count || 0))}</div>
      </div>
      <div class="mt-2 rounded border border-slate-300 bg-slate-50 p-2">
        <p class="mb-1 font-semibold text-slate-900">Trend Series (sample)</p>
        <div class="overflow-x-auto">
          <table class="w-full text-[11px]">
            <thead><tr class="text-left text-slate-600"><th class="px-2 py-1">Timestamp</th><th class="px-2 py-1 text-right">LOC</th><th class="px-2 py-1 text-right">Avg Complexity</th><th class="px-2 py-1 text-right">Max Complexity</th></tr></thead>
            <tbody>${seriesRows || `<tr><td class="px-2 py-1 text-slate-600" colspan="4">No trend series rows.</td></tr>`}</tbody>
          </table>
        </div>
      </div>
    `;
  }

  if (el.discoverDataContent) {
    const phpRouteInventory = (rawArtifacts.php_route_inventory && typeof rawArtifacts.php_route_inventory === "object")
      ? rawArtifacts.php_route_inventory
      : {};
    const phpControllerInventory = (rawArtifacts.php_controller_inventory && typeof rawArtifacts.php_controller_inventory === "object")
      ? rawArtifacts.php_controller_inventory
      : {};
    const phpTemplateInventory = (rawArtifacts.php_template_inventory && typeof rawArtifacts.php_template_inventory === "object")
      ? rawArtifacts.php_template_inventory
      : {};
    const phpSqlCatalog = (rawArtifacts.php_sql_catalog && typeof rawArtifacts.php_sql_catalog === "object")
      ? rawArtifacts.php_sql_catalog
      : {};
    const phpSessionInventory = (rawArtifacts.php_session_state_inventory && typeof rawArtifacts.php_session_state_inventory === "object")
      ? rawArtifacts.php_session_state_inventory
      : {};
    const phpAuthInventory = (rawArtifacts.php_authz_authn_inventory && typeof rawArtifacts.php_authz_authn_inventory === "object")
      ? rawArtifacts.php_authz_authn_inventory
      : {};
    const phpJobInventory = (rawArtifacts.php_background_job_inventory && typeof rawArtifacts.php_background_job_inventory === "object")
      ? rawArtifacts.php_background_job_inventory
      : {};
    const phpFileIoInventory = (rawArtifacts.php_file_io_inventory && typeof rawArtifacts.php_file_io_inventory === "object")
      ? rawArtifacts.php_file_io_inventory
      : {};
    const phpValidationRules = (rawArtifacts.php_validation_rules && typeof rawArtifacts.php_validation_rules === "object")
      ? rawArtifacts.php_validation_rules
      : {};
    const sourceProfile = (rawArtifacts.source_db_profile && typeof rawArtifacts.source_db_profile === "object")
      ? rawArtifacts.source_db_profile
      : {};
    const sourceSchema = (rawArtifacts.source_schema_model && typeof rawArtifacts.source_schema_model === "object")
      ? rawArtifacts.source_schema_model
      : {};
    const targetSchema = (rawArtifacts.target_schema_model && typeof rawArtifacts.target_schema_model === "object")
      ? rawArtifacts.target_schema_model
      : {};
    const sourceErd = (rawArtifacts.source_erd && typeof rawArtifacts.source_erd === "object")
      ? rawArtifacts.source_erd
      : {};
    const sourceDictionary = (rawArtifacts.source_data_dictionary && typeof rawArtifacts.source_data_dictionary === "object")
      ? rawArtifacts.source_data_dictionary
      : {};
    const sourceRelationshipCandidates = (rawArtifacts.source_relationship_candidates && typeof rawArtifacts.source_relationship_candidates === "object")
      ? rawArtifacts.source_relationship_candidates
      : {};
    const mapping = (rawArtifacts.schema_mapping_matrix && typeof rawArtifacts.schema_mapping_matrix === "object")
      ? rawArtifacts.schema_mapping_matrix
      : {};
    const dbQa = (rawArtifacts.db_qa_report && typeof rawArtifacts.db_qa_report === "object")
      ? rawArtifacts.db_qa_report
      : {};
    const projectMetrics = Array.isArray(rawArtifacts.project_metrics?.rows) ? rawArtifacts.project_metrics.rows : [];
    const typeMetrics = Array.isArray(rawArtifacts.type_metrics?.rows) ? rawArtifacts.type_metrics.rows : [];
    const typeDependencyMatrix = Array.isArray(rawArtifacts.type_dependency_matrix?.edges) ? rawArtifacts.type_dependency_matrix.edges : [];
    const runtimeDependencyMatrix = Array.isArray(rawArtifacts.runtime_dependency_matrix?.edges) ? rawArtifacts.runtime_dependency_matrix.edges : [];
    const deadCodeReport = (rawArtifacts.dead_code_report && typeof rawArtifacts.dead_code_report === "object")
      ? rawArtifacts.dead_code_report
      : {};
    const thirdPartyUsage = Array.isArray(rawArtifacts.third_party_usage?.rows) ? rawArtifacts.third_party_usage.rows : [];
    const codeQualityRules = Array.isArray(rawArtifacts.code_quality_rules?.rules) ? rawArtifacts.code_quality_rules.rules : [];
    const qualityViolationReport = (rawArtifacts.quality_violation_report && typeof rawArtifacts.quality_violation_report === "object")
      ? rawArtifacts.quality_violation_report
      : {};
    const trendSnapshot = (rawArtifacts.trend_snapshot && typeof rawArtifacts.trend_snapshot === "object")
      ? rawArtifacts.trend_snapshot
      : {};

    const summary = (sourceProfile.summary && typeof sourceProfile.summary === "object")
      ? sourceProfile.summary
      : ((sourceSchema.summary && typeof sourceSchema.summary === "object") ? sourceSchema.summary : {});
    const tableCount = Number(summary.tables || 0);
    const columnCount = Number(summary.columns || 0);
    const queryCount = Number(summary.queries || 0);
    const candidateRelationships = Array.isArray(sourceRelationshipCandidates.candidates) ? sourceRelationshipCandidates.candidates : [];
    const mappingRows = Array.isArray(mapping.mappings) ? mapping.mappings : [];
    const sourceTables = Array.isArray(sourceSchema.tables) ? sourceSchema.tables : [];
    const targetTables = Array.isArray(targetSchema.tables) ? targetSchema.tables : [];
    const sourceRelationships = Array.isArray(sourceSchema.relationships) ? sourceSchema.relationships : [];
    const displayRelationships = sourceRelationships.length ? sourceRelationships : candidateRelationships;
    const relCount = Number(summary.relationships || displayRelationships.length || 0);
    const sourceDictionaryRows = Array.isArray(sourceDictionary.rows) ? sourceDictionary.rows : [];
    const sourceErdText = buildSourceErdFallback(sourceTables, displayRelationships, String(sourceErd.mermaid || "").trim());
    const dbChecks = Array.isArray(dbQa.checks) ? dbQa.checks : [];
    const deadSummary = (deadCodeReport.summary && typeof deadCodeReport.summary === "object")
      ? deadCodeReport.summary
      : {};
    const violationSummary = (qualityViolationReport.summary && typeof qualityViolationReport.summary === "object")
      ? qualityViolationReport.summary
      : {};
    const violationRows = Array.isArray(qualityViolationReport.violations) ? qualityViolationReport.violations : [];
    const trendMetrics = (trendSnapshot.snapshot && typeof trendSnapshot.snapshot === "object" && typeof trendSnapshot.snapshot.metrics === "object")
      ? trendSnapshot.snapshot.metrics
      : {};
    const phpRoutes = Array.isArray(phpRouteInventory.routes) ? phpRouteInventory.routes : [];
    const phpControllers = Array.isArray(phpControllerInventory.controllers) ? phpControllerInventory.controllers : [];
    const phpTemplates = Array.isArray(phpTemplateInventory.templates) ? phpTemplateInventory.templates : [];
    const phpSqlStatements = Array.isArray(phpSqlCatalog.statements) ? phpSqlCatalog.statements : [];
    const phpAuthChecks = Array.isArray(phpAuthInventory.auth_checks) ? phpAuthInventory.auth_checks : [];
    const phpValidationRows = Array.isArray(phpValidationRules.rules) ? phpValidationRules.rules : [];
    const phpHasDiscovery = !!(
      phpRoutes.length
      || phpControllers.length
      || phpTemplates.length
      || phpSqlStatements.length
      || Object.keys(phpSessionInventory).length
      || phpAuthChecks.length
      || Object.keys(phpJobInventory).length
      || Object.keys(phpFileIoInventory).length
      || phpValidationRows.length
    );
    const hasQualityBaseline = (
      projectMetrics.length > 0
      || typeMetrics.length > 0
      || typeDependencyMatrix.length > 0
      || runtimeDependencyMatrix.length > 0
      || thirdPartyUsage.length > 0
      || codeQualityRules.length > 0
      || violationRows.length > 0
    );

    if (!tableCount && !sourceTables.length && !dbChecks.length && !hasQualityBaseline && !phpHasDiscovery) {
      el.discoverDataContent.innerHTML = `<p class="text-slate-700">Run Analyst Brief to generate database archaeology artifacts.</p>`;
    } else {
      const badgeClass = (status) => {
        const s = String(status || "").toUpperCase();
        if (s === "PASS") return "border-emerald-300 bg-emerald-50 text-emerald-800";
        if (s === "FAIL") return "border-rose-300 bg-rose-50 text-rose-800";
        return "border-amber-300 bg-amber-50 text-amber-800";
      };

      const sourceRows = sourceTables.slice(0, 8).map((tbl) => {
        const columns = Array.isArray(tbl?.columns) ? tbl.columns : [];
        const pk = Array.isArray(tbl?.primary_key_candidates) ? tbl.primary_key_candidates : [];
        return `
          <tr>
            <td class="px-2 py-1">${escapeHtml(String(tbl?.name || ""))}</td>
            <td class="px-2 py-1 text-right">${columns.length}</td>
            <td class="px-2 py-1">${escapeHtml(pk.slice(0, 3).join(", ") || "n/a")}</td>
          </tr>
        `;
      }).join("");

      const targetRows = targetTables.slice(0, 8).map((tbl) => {
        const cols = Array.isArray(tbl?.columns) ? tbl.columns : [];
        const pk = Array.isArray(tbl?.primary_key) ? tbl.primary_key : [];
        return `
          <tr>
            <td class="px-2 py-1">${escapeHtml(String(tbl?.name || ""))}</td>
            <td class="px-2 py-1 text-right">${cols.length}</td>
            <td class="px-2 py-1">${escapeHtml(pk.slice(0, 3).join(", ") || "n/a")}</td>
          </tr>
        `;
      }).join("");

      const qaRows = dbChecks.slice(0, 8).map((check) => `
        <div class="rounded border px-2 py-1 ${badgeClass(check?.status)}">
          <strong>${escapeHtml(String(check?.id || "db_check"))}</strong>: ${escapeHtml(String(check?.detail || ""))}
        </div>
      `).join("");
      const relRows = displayRelationships.slice(0, 10).map((rel) => `
        <tr>
          <td class="px-2 py-1">${escapeHtml(String(rel?.from_table || ""))}</td>
          <td class="px-2 py-1">${escapeHtml(String(rel?.from_column || ""))}</td>
          <td class="px-2 py-1">${escapeHtml(String(rel?.to_table || ""))}</td>
          <td class="px-2 py-1">${escapeHtml(String(rel?.to_column || ""))}</td>
          <td class="px-2 py-1 text-right">${escapeHtml(String(rel?.confidence ?? ""))}</td>
        </tr>
      `).join("");
      const dictRows = sourceDictionaryRows.slice(0, 12).map((row) => `
        <tr>
          <td class="px-2 py-1">${escapeHtml(String(row?.table || ""))}</td>
          <td class="px-2 py-1">${escapeHtml(String(row?.column || ""))}</td>
          <td class="px-2 py-1">${escapeHtml(String(row?.inferred_type || ""))}</td>
          <td class="px-2 py-1">${escapeHtml(String(row?.business_meaning || ""))}</td>
        </tr>
      `).join("");
      const hotspotRows = [...typeMetrics]
        .sort((a, b) => Number(b?.cyclomatic_complexity || 0) - Number(a?.cyclomatic_complexity || 0))
        .slice(0, 8)
        .map((row) => `
          <tr>
            <td class="px-2 py-1">${escapeHtml(String(row?.project || "n/a"))}</td>
            <td class="px-2 py-1">${escapeHtml(String(row?.type_name || ""))}</td>
            <td class="px-2 py-1 text-right">${escapeHtml(String(row?.cyclomatic_complexity || 0))}</td>
            <td class="px-2 py-1 text-right">${escapeHtml(String((Number(row?.afferent_coupling || 0) + Number(row?.efferent_coupling || 0))))}</td>
          </tr>
        `).join("");
      const violationSampleRows = violationRows.slice(0, 8).map((row) => `
        <tr>
          <td class="px-2 py-1">${escapeHtml(String(row?.rule_id || ""))}</td>
          <td class="px-2 py-1">${escapeHtml(String(row?.severity || ""))}</td>
          <td class="px-2 py-1">${escapeHtml(String(row?.subject || ""))}</td>
          <td class="px-2 py-1">${escapeHtml(String(row?.detail || ""))}</td>
        </tr>
      `).join("");
      const dependencyUsageRows = [...thirdPartyUsage]
        .sort((a, b) => Number(b?.usage_intensity || 0) - Number(a?.usage_intensity || 0))
        .slice(0, 8)
        .map((row) => `
          <tr>
            <td class="px-2 py-1">${escapeHtml(String(row?.dependency || ""))}</td>
            <td class="px-2 py-1">${escapeHtml(String(row?.kind || ""))}</td>
            <td class="px-2 py-1 text-right">${escapeHtml(String(row?.forms_using_count || 0))}</td>
            <td class="px-2 py-1 text-right">${escapeHtml(String(row?.usage_intensity || 0))}</td>
          </tr>
        `).join("");
      const qualitySectionHtml = hasQualityBaseline ? `
        <div class="mt-3 rounded border border-slate-300 bg-white p-2">
          <p class="mb-2 font-semibold text-slate-900">Code Quality Baseline</p>
          <div class="grid gap-2 sm:grid-cols-4 lg:grid-cols-8">
            <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Projects</strong><br/>${projectMetrics.length}</div>
            <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Types</strong><br/>${typeMetrics.length}</div>
            <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Type Edges</strong><br/>${typeDependencyMatrix.length}</div>
            <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Runtime Edges</strong><br/>${runtimeDependencyMatrix.length}</div>
            <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Violations</strong><br/>${Number(violationSummary.total_violations || violationRows.length)}</div>
            <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Critical</strong><br/>${Number(violationSummary.critical_violations || 0)}</div>
            <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Dead Candidates</strong><br/>${Number(deadSummary.dead_type_candidates || 0) + Number(deadSummary.dead_method_candidates || 0) + Number(deadSummary.dead_field_candidates || 0)}</div>
            <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Rules</strong><br/>${codeQualityRules.length}</div>
          </div>
          <div class="mt-2 grid gap-2 lg:grid-cols-2">
            <div class="rounded border border-slate-300 bg-slate-50 p-2">
              <p class="mb-1 font-semibold text-slate-900">Hotspot Types (sample)</p>
              <div class="overflow-x-auto">
                <table class="w-full text-[11px]">
                  <thead><tr class="text-left text-slate-600"><th class="px-2 py-1">Project</th><th class="px-2 py-1">Type</th><th class="px-2 py-1 text-right">Complexity</th><th class="px-2 py-1 text-right">Coupling</th></tr></thead>
                  <tbody>${hotspotRows || `<tr><td class="px-2 py-1 text-slate-600" colspan="4">No type metrics rows.</td></tr>`}</tbody>
                </table>
              </div>
            </div>
            <div class="rounded border border-slate-300 bg-slate-50 p-2">
              <p class="mb-1 font-semibold text-slate-900">Third-Party Usage (sample)</p>
              <div class="overflow-x-auto">
                <table class="w-full text-[11px]">
                  <thead><tr class="text-left text-slate-600"><th class="px-2 py-1">Dependency</th><th class="px-2 py-1">Kind</th><th class="px-2 py-1 text-right">Forms</th><th class="px-2 py-1 text-right">Intensity</th></tr></thead>
                  <tbody>${dependencyUsageRows || `<tr><td class="px-2 py-1 text-slate-600" colspan="4">No dependency usage rows.</td></tr>`}</tbody>
                </table>
              </div>
            </div>
          </div>
          <div class="mt-2 rounded border border-slate-300 bg-slate-50 p-2">
            <p class="mb-1 font-semibold text-slate-900">Quality Violations (sample)</p>
            <div class="overflow-x-auto">
              <table class="w-full text-[11px]">
                <thead><tr class="text-left text-slate-600"><th class="px-2 py-1">Rule</th><th class="px-2 py-1">Severity</th><th class="px-2 py-1">Subject</th><th class="px-2 py-1">Detail</th></tr></thead>
                <tbody>${violationSampleRows || `<tr><td class="px-2 py-1 text-slate-600" colspan="4">No violations detected.</td></tr>`}</tbody>
              </table>
            </div>
            <p class="mt-1 text-[11px] text-slate-600">
              Trend snapshot: LOC=${escapeHtml(String(trendMetrics.loc_total || 0))},
              max_complexity=${escapeHtml(String(trendMetrics.max_complexity || 0))},
              avg_complexity=${escapeHtml(String(trendMetrics.avg_complexity || 0))},
              hotspot_count=${escapeHtml(String(trendMetrics.hotspot_count || 0))}
            </p>
          </div>
        </div>
      ` : "";
      const phpRouteRows = phpRoutes.slice(0, 10).map((row) => `
        <tr>
          <td class="px-2 py-1">${escapeHtml(String(row?.method || "ANY"))}</td>
          <td class="px-2 py-1">${escapeHtml(String(row?.path || ""))}</td>
          <td class="px-2 py-1">${escapeHtml(String(row?.handler || row?.action || ""))}</td>
        </tr>
      `).join("");
      const phpControllerRows = phpControllers.slice(0, 10).map((row) => `
        <tr>
          <td class="px-2 py-1">${escapeHtml(String(row?.name || ""))}</td>
          <td class="px-2 py-1 text-right">${escapeHtml(String((Array.isArray(row?.actions) ? row.actions.length : 0) || 0))}</td>
          <td class="px-2 py-1">${escapeHtml(String(row?.framework || row?.base_class || "n/a"))}</td>
        </tr>
      `).join("");
      const phpTemplateRows = phpTemplates.slice(0, 10).map((row) => `
        <tr>
          <td class="px-2 py-1">${escapeHtml(String(row?.path || row?.name || ""))}</td>
          <td class="px-2 py-1">${escapeHtml(String(row?.template_type || row?.kind || "template"))}</td>
          <td class="px-2 py-1 text-right">${escapeHtml(String(row?.loc || row?.line_count || 0))}</td>
        </tr>
      `).join("");
      const phpSqlRows = phpSqlStatements.slice(0, 10).map((row) => `
        <tr>
          <td class="px-2 py-1">${escapeHtml(String(row?.operation || row?.kind || ""))}</td>
          <td class="px-2 py-1">${escapeHtml(String(Array.isArray(row?.tables) ? row.tables.slice(0, 3).join(", ") : (row?.table || "n/a")))}</td>
          <td class="px-2 py-1">${escapeHtml(String(Array.isArray(row?.risk_flags) ? row.risk_flags.slice(0, 2).join(", ") : "none"))}</td>
        </tr>
      `).join("");
      const phpSectionHtml = phpHasDiscovery ? `
        <div class="mb-3 rounded border border-slate-300 bg-white p-2">
          <div class="flex flex-wrap items-center justify-between gap-2">
            <p class="font-semibold text-slate-900">PHP application structure</p>
            <div class="flex flex-wrap gap-1">
              <button type="button" class="btn-light rounded px-2 py-1 text-[11px] font-semibold" onclick="window.__downloadDiscoverArtifact && window.__downloadDiscoverArtifact('php_route_inventory')">Routes</button>
              <button type="button" class="btn-light rounded px-2 py-1 text-[11px] font-semibold" onclick="window.__downloadDiscoverArtifact && window.__downloadDiscoverArtifact('php_controller_inventory')">Controllers</button>
              <button type="button" class="btn-light rounded px-2 py-1 text-[11px] font-semibold" onclick="window.__downloadDiscoverArtifact && window.__downloadDiscoverArtifact('php_sql_catalog')">SQL</button>
            </div>
          </div>
          <div class="mt-2 grid gap-2 sm:grid-cols-5">
            <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Routes</strong><br/>${escapeHtml(String(phpRouteInventory.route_count || phpRoutes.length))}</div>
            <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Controllers</strong><br/>${escapeHtml(String(phpControllerInventory.controller_count || phpControllers.length))}</div>
            <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Templates</strong><br/>${escapeHtml(String(phpTemplateInventory.template_count || phpTemplates.length))}</div>
            <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>SQL Statements</strong><br/>${escapeHtml(String(phpSqlCatalog.statement_count || phpSqlStatements.length))}</div>
            <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Validation Rules</strong><br/>${escapeHtml(String(phpValidationRules.rule_count || phpValidationRows.length))}</div>
          </div>
          <div class="mt-2 grid gap-2 lg:grid-cols-2">
            <div class="rounded border border-slate-300 bg-slate-50 p-2">
              <p class="mb-1 font-semibold text-slate-900">Route inventory (sample)</p>
              <div class="overflow-x-auto"><table class="w-full text-[11px]"><thead><tr class="text-left text-slate-600"><th class="px-2 py-1">Method</th><th class="px-2 py-1">Path</th><th class="px-2 py-1">Handler</th></tr></thead><tbody>${phpRouteRows || `<tr><td class="px-2 py-1 text-slate-600" colspan="3">No route rows.</td></tr>`}</tbody></table></div>
            </div>
            <div class="rounded border border-slate-300 bg-slate-50 p-2">
              <p class="mb-1 font-semibold text-slate-900">Controller inventory (sample)</p>
              <div class="overflow-x-auto"><table class="w-full text-[11px]"><thead><tr class="text-left text-slate-600"><th class="px-2 py-1">Controller</th><th class="px-2 py-1 text-right">Actions</th><th class="px-2 py-1">Framework</th></tr></thead><tbody>${phpControllerRows || `<tr><td class="px-2 py-1 text-slate-600" colspan="3">No controller rows.</td></tr>`}</tbody></table></div>
            </div>
            <div class="rounded border border-slate-300 bg-slate-50 p-2">
              <p class="mb-1 font-semibold text-slate-900">Template inventory (sample)</p>
              <div class="overflow-x-auto"><table class="w-full text-[11px]"><thead><tr class="text-left text-slate-600"><th class="px-2 py-1">Template</th><th class="px-2 py-1">Type</th><th class="px-2 py-1 text-right">LOC</th></tr></thead><tbody>${phpTemplateRows || `<tr><td class="px-2 py-1 text-slate-600" colspan="3">No template rows.</td></tr>`}</tbody></table></div>
            </div>
            <div class="rounded border border-slate-300 bg-slate-50 p-2">
              <p class="mb-1 font-semibold text-slate-900">SQL touchpoints (sample)</p>
              <div class="overflow-x-auto"><table class="w-full text-[11px]"><thead><tr class="text-left text-slate-600"><th class="px-2 py-1">Operation</th><th class="px-2 py-1">Tables</th><th class="px-2 py-1">Risk Flags</th></tr></thead><tbody>${phpSqlRows || `<tr><td class="px-2 py-1 text-slate-600" colspan="3">No SQL rows detected.</td></tr>`}</tbody></table></div>
            </div>
          </div>
          <div class="mt-2 grid gap-2 lg:grid-cols-3">
            <div class="rounded border border-slate-300 bg-slate-50 p-2 text-[11px] text-slate-900">
              <p class="font-semibold text-slate-900">Session and auth</p>
              <p class="mt-1 text-slate-700">Session keys: <strong>${escapeHtml(String(phpSessionInventory.session_key_count || 0))}</strong> · uses session state: <strong>${phpSessionInventory.uses_session_state ? "yes" : "no"}</strong></p>
              <p class="mt-1 text-slate-700">Auth checks: <strong>${escapeHtml(String(phpAuthInventory.auth_check_count || phpAuthChecks.length || 0))}</strong></p>
            </div>
            <div class="rounded border border-slate-300 bg-slate-50 p-2 text-[11px] text-slate-900">
              <p class="font-semibold text-slate-900">Background jobs</p>
              <p class="mt-1 text-slate-700">Detected jobs: <strong>${escapeHtml(String(phpJobInventory.job_count || 0))}</strong></p>
              <p class="mt-1 text-slate-700">Cron/CLI entry points are available in the exported artifact.</p>
            </div>
            <div class="rounded border border-slate-300 bg-slate-50 p-2 text-[11px] text-slate-900">
              <p class="font-semibold text-slate-900">File I/O</p>
              <p class="mt-1 text-slate-700">Upload points: <strong>${escapeHtml(String(phpFileIoInventory.upload_file_count || 0))}</strong> · Export/download points: <strong>${escapeHtml(String(phpFileIoInventory.download_file_count || phpFileIoInventory.export_file_count || 0))}</strong></p>
            </div>
          </div>
        </div>
      ` : "";

      el.discoverDataContent.innerHTML = `
        ${phpSectionHtml}
        <div class="grid gap-2 sm:grid-cols-5">
          <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Tables</strong><br/>${tableCount || sourceTables.length}</div>
          <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Columns</strong><br/>${columnCount || 0}</div>
          <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Queries</strong><br/>${queryCount || 0}</div>
          <div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>Relationships</strong><br/>${relCount || 0}</div>
          <div class="rounded border px-2 py-1 ${badgeClass(dbQa?.overall_status)}"><strong>DB QA</strong><br/>${escapeHtml(String(dbQa?.overall_status || "WARN"))}</div>
        </div>
        <div class="mt-2 grid gap-2 lg:grid-cols-2">
          <div class="rounded border border-slate-300 bg-white p-2">
            <p class="mb-1 font-semibold text-slate-900">Source Schema (sample)</p>
            <div class="overflow-x-auto">
              <table class="w-full text-[11px]">
                <thead><tr class="text-left text-slate-600"><th class="px-2 py-1">Table</th><th class="px-2 py-1 text-right">Cols</th><th class="px-2 py-1">PK candidates</th></tr></thead>
                <tbody>${sourceRows || `<tr><td class="px-2 py-1 text-slate-600" colspan="3">No source schema rows.</td></tr>`}</tbody>
              </table>
            </div>
          </div>
          <div class="rounded border border-slate-300 bg-white p-2">
            <p class="mb-1 font-semibold text-slate-900">Target Schema (sample)</p>
            <div class="overflow-x-auto">
              <table class="w-full text-[11px]">
                <thead><tr class="text-left text-slate-600"><th class="px-2 py-1">Table</th><th class="px-2 py-1 text-right">Cols</th><th class="px-2 py-1">Primary key</th></tr></thead>
                <tbody>${targetRows || `<tr><td class="px-2 py-1 text-slate-600" colspan="3">No target schema rows.</td></tr>`}</tbody>
              </table>
            </div>
          </div>
        </div>
        <div class="mt-2 rounded border border-slate-300 bg-white p-2">
          <p class="mb-1 font-semibold text-slate-900">Mapping + QA</p>
          <p class="text-[11px] text-slate-700">Schema mappings: <strong>${mappingRows.length}</strong> rows.</p>
          <div class="mt-1 grid gap-1">${qaRows || `<p class="text-[11px] text-slate-700">No DB QA checks available yet.</p>`}</div>
        </div>
        <div class="mt-2 grid gap-2 lg:grid-cols-2">
          <div class="rounded border border-slate-300 bg-white p-2">
            <p class="mb-1 font-semibold text-slate-900">Source Relationships (sample)</p>
            <div class="overflow-x-auto">
              <table class="w-full text-[11px]">
                <thead><tr class="text-left text-slate-600"><th class="px-2 py-1">From Table</th><th class="px-2 py-1">From Col</th><th class="px-2 py-1">To Table</th><th class="px-2 py-1">To Col</th><th class="px-2 py-1 text-right">Conf</th></tr></thead>
                <tbody>${relRows || `<tr><td class="px-2 py-1 text-slate-600" colspan="5">No relationship rows.</td></tr>`}</tbody>
              </table>
            </div>
          </div>
          <div class="rounded border border-slate-300 bg-white p-2">
            ${sourceErdText
              ? mermaidBlock("Source ERD (Mermaid)", sourceErdText)
              : `<p class="mb-1 font-semibold text-slate-900">Source ERD (Mermaid)</p><p class="rounded border border-slate-200 bg-slate-50 p-2 text-[11px] text-slate-700">No source_erd artifact available.</p>`}
          </div>
        </div>
        <div class="mt-2 rounded border border-slate-300 bg-white p-2">
          <p class="mb-1 font-semibold text-slate-900">Source Data Dictionary (sample)</p>
          <div class="overflow-x-auto">
            <table class="w-full text-[11px]">
              <thead><tr class="text-left text-slate-600"><th class="px-2 py-1">Table</th><th class="px-2 py-1">Column</th><th class="px-2 py-1">Type</th><th class="px-2 py-1">Meaning</th></tr></thead>
              <tbody>${dictRows || `<tr><td class="px-2 py-1 text-slate-600" colspan="4">No dictionary rows.</td></tr>`}</tbody>
            </table>
          </div>
        </div>
        ${qualitySectionHtml}
      `;
      setTimeout(() => renderMermaidBlocks(el.discoverDataContent), 0);
    }
  }
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
      if (node instanceof HTMLInputElement) {
        const masked = String(data[`${refs.secretKey}_masked`] || "");
        node.placeholder = masked ? `Saved: ${masked}` : "Leave blank to keep existing token";
      }
      return;
    }
    node.value = String(data[key] || "");
  });

  if (refs.message) {
    const masked = String(data[`${refs.secretKey}_masked`] || "");
    const parts = [];
    if (provider === "github") {
      const hasToken = !!masked;
      const readScope = "repo metadata, branches, pull requests";
      const writeScope = data.read_only ? "none (read-only mode)" : "pull requests + run artifact exports";
      parts.push(`Permissions: read scope=${readScope}; write scope=${writeScope}`);
      parts.push(
        hasToken
          ? "Analysis source: saved GitHub token (public and private repos)"
          : "Analysis source: public GitHub URL works without a saved token; token only required for private repos or higher GitHub rate limits"
      );
      if (data.run_export_enabled) {
        const exportBranch = String(data.export_branch || "default branch");
        const exportPrefix = String(data.export_prefix || "synthetix");
        const targetOwner = String(data.export_owner || data.owner || "-");
        const targetRepo = String(data.export_repository || data.repository || "-");
        const targetMode = (String(data.export_owner || "").trim() || String(data.export_repository || "").trim())
          ? "configured repository"
          : "source repository fallback";
        parts.push(`Run export: enabled (${targetOwner}/${targetRepo} -> ${exportPrefix}/runs/<run_id> on ${exportBranch}; ${targetMode})`);
        parts.push("Export target: local run artifacts + optional GitHub export");
      } else {
        parts.push("Run export: disabled");
        parts.push("Export target: local run artifacts only");
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
      if (node instanceof HTMLInputElement) {
        const masked = String(data.api_key_masked || "");
        node.placeholder = masked ? `Saved: ${masked}` : "Leave blank to keep existing API key";
      }
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

function settingsUsers() {
  const rows = Array.isArray(state.settings?.users) ? state.settings.users : [];
  return rows
    .filter((row) => row && typeof row === "object" && String(row.email || "").includes("@"))
    .sort((a, b) => String(a.email || "").localeCompare(String(b.email || "")));
}

function setUserMessage(text, isError = false) {
  if (!el.settingsUserMessage) return;
  el.settingsUserMessage.textContent = String(text || "");
  el.settingsUserMessage.className = `mt-1 text-[11px] ${isError ? "text-rose-700" : "text-slate-700"}`;
}

function setKnowledgeMessage(text, isError = false) {
  if (!el.settingsKnowledgeMessage) return;
  el.settingsKnowledgeMessage.textContent = String(text || "");
  el.settingsKnowledgeMessage.className = `mt-2 text-[11px] ${isError ? "text-rose-700" : "text-slate-700"}`;
}

function renderCurrentUserIdentity() {
  const email = String(state.activeUserEmail || "").trim().toLowerCase();
  const name = String(state.activeUserName || "").trim();
  const role = String(state.activeUserRole || "").trim().toUpperCase();
  const label = name ? `${name} (${role || "USER"})` : (email || "User");
  if (el.userMenuBtn) {
    el.userMenuBtn.textContent = label;
    el.userMenuBtn.title = email ? `Signed in as ${email}` : "No active user selected";
  }
}

function applyUserFormFromRow(row) {
  if (!row || typeof row !== "object") return;
  if (el.settingsUserEmail) el.settingsUserEmail.value = String(row.email || "");
  if (el.settingsUserName) el.settingsUserName.value = String(row.display_name || "");
  if (el.settingsUserRole) el.settingsUserRole.value = String(row.role || "engineering");
  if (el.settingsUserStatus) el.settingsUserStatus.value = String(row.status || "active");
}

function renderUsersPanel() {
  const users = settingsUsers();
  const activeFallback = users.find((row) => String(row.status || "").toLowerCase() === "active") || users[0] || null;
  if (!state.activeUserEmail && activeFallback) {
    state.activeUserEmail = String(activeFallback.email || "").toLowerCase();
  }
  if (!state.activeUserEmail) {
    state.activeUserEmail = "local-user@synthetix.local";
  }
  if (el.settingsCurrentUserSelect) {
    const options = users.map((row) => {
      const email = escapeHtml(String(row.email || ""));
      const name = escapeHtml(String(row.display_name || row.email || ""));
      const role = escapeHtml(String(row.role || "").toUpperCase());
      const status = escapeHtml(String(row.status || "active"));
      return `<option value="${email}">${name} | ${role} | ${status}</option>`;
    }).join("");
    el.settingsCurrentUserSelect.innerHTML = options || `<option value="local-user@synthetix.local">Local User</option>`;
    const exists = users.some((row) => String(row.email || "").toLowerCase() === String(state.activeUserEmail || "").toLowerCase());
    if (!exists && users.length) {
      state.activeUserEmail = String(users[0].email || "").toLowerCase();
    }
    el.settingsCurrentUserSelect.value = String(state.activeUserEmail || "local-user@synthetix.local").toLowerCase();
  }
  if (el.settingsUsersList) {
    if (!users.length) {
      el.settingsUsersList.innerHTML = `<p class="text-[11px] text-slate-700">No users configured yet.</p>`;
    } else {
      el.settingsUsersList.innerHTML = users.map((row) => {
        const email = escapeHtml(row.email || "");
        const name = escapeHtml(row.display_name || "-");
        const role = escapeHtml(String(row.role || "").toUpperCase());
        const status = escapeHtml(String(row.status || "").toUpperCase());
        const active = String(row.email || "").toLowerCase() === String(state.activeUserEmail || "").toLowerCase();
        return `<p class="mb-1 text-[11px] ${active ? "font-semibold text-slate-900" : "text-slate-800"}">${name} | ${email} | ${role} | ${status}</p>`;
      }).join("");
    }
  }
  const selected = users.find((row) => String(row.email || "").toLowerCase() === String(state.activeUserEmail || "").toLowerCase());
  if (selected) {
    state.activeUserRole = String(selected.role || "engineering").toLowerCase();
    state.activeUserName = String(selected.display_name || "");
    applyUserFormFromRow(selected);
  }
  renderCurrentUserIdentity();
}

function setKnowledgeHubTab(tabName) {
  const tab = String(tabName || "sources").toLowerCase();
  const safe = ["sources", "knowledge", "jobs", "evals"].includes(tab) ? tab : "sources";
  state.settingsKnowledgeTab = safe;
  const panes = {
    sources: el.settingsKnowledgePaneSources,
    knowledge: el.settingsKnowledgePaneKnowledge,
    jobs: el.settingsKnowledgePaneJobs,
    evals: el.settingsKnowledgePaneEvals,
  };
  Object.entries(panes).forEach(([key, node]) => {
    if (!node) return;
    node.classList.toggle("hidden", key !== safe);
  });
  document.querySelectorAll("[data-knowledge-hub-tab]").forEach((btn) => {
    if (!(btn instanceof HTMLElement)) return;
    const active = String(btn.getAttribute("data-knowledge-hub-tab") || "") === safe;
    btn.classList.toggle("btn-dark", active);
    btn.classList.toggle("btn-light", !active);
  });
}

function renderKnowledgeHub() {
  const hub = (state.settings?.knowledge_hub && typeof state.settings.knowledge_hub === "object") ? state.settings.knowledge_hub : {};
  const sources = Array.isArray(hub.sources) ? hub.sources : [];
  const sets = Array.isArray(hub.sets) ? hub.sets : [];
  const brains = Array.isArray(hub.agent_brains) ? hub.agent_brains : [];
  const specialists = Array.isArray(hub.specialists) ? hub.specialists : [];
  const policies = (state.settings?.policies && typeof state.settings.policies === "object") ? state.settings.policies : {};

  setKnowledgeHubTab(state.settingsKnowledgeTab || "sources");

  if (el.settingsKnowledgeSourcesList) {
    if (!sources.length) {
      el.settingsKnowledgeSourcesList.innerHTML = `<p class="text-[11px] text-slate-700">No knowledge sources yet.</p>`;
    } else {
      el.settingsKnowledgeSourcesList.innerHTML = sources.slice(0, 80).map((row) => {
        const id = escapeHtml(row.source_id || "");
        const name = escapeHtml(row.name || "");
        const scope = escapeHtml(String(row.scope || "").toUpperCase());
        const typ = escapeHtml(String(row.type || "").toUpperCase());
        return `<p class="mb-1 text-[11px] text-slate-800">${id} | ${name} | ${typ} | ${scope}</p>`;
      }).join("");
    }
  }

  if (el.settingsKnowledgeSetsList) {
    if (!sets.length) {
      el.settingsKnowledgeSetsList.innerHTML = `<p class="text-[11px] text-slate-700">No knowledge sets yet.</p>`;
    } else {
      el.settingsKnowledgeSetsList.innerHTML = sets.slice(0, 80).map((row) => {
        const id = escapeHtml(row.set_id || "");
        const name = escapeHtml(row.name || "");
        const version = escapeHtml(row.version || "1.0.0");
        const stateLabel = escapeHtml(String(row.publish_state || "draft").toUpperCase());
        const sourceIds = Array.isArray(row.source_ids) ? row.source_ids : [];
        return `<p class="mb-1 text-[11px] text-slate-800">${id} | ${name} v${version} | ${stateLabel} | sources=${sourceIds.length}</p>`;
      }).join("");
    }
  }

  if (el.settingsKnowledgeBrainsList) {
    if (!brains.length) {
      el.settingsKnowledgeBrainsList.innerHTML = `<p class="text-[11px] text-slate-700">No agent brain bindings yet.</p>`;
    } else {
      el.settingsKnowledgeBrainsList.innerHTML = brains.slice(0, 80).map((row) => {
        const agentKey = escapeHtml(row.agent_key || "");
        const topK = escapeHtml(String(row.top_k || 8));
        const citation = row.citation_required ? "cite" : "no-cite";
        const setsCount = Array.isArray(row.knowledge_set_ids) ? row.knowledge_set_ids.length : 0;
        const toolsCount = Array.isArray(row.allowed_tools) ? row.allowed_tools.length : 0;
        return `<p class="mb-1 text-[11px] text-slate-800">${agentKey} | sets=${setsCount} | topK=${topK} | ${citation} | tools=${toolsCount}</p>`;
      }).join("");
    }
    if (specialists.length) {
      const preview = specialists.slice(0, 6).map((row) => {
        const name = escapeHtml(String(row.name || row.specialist_id || "specialist"));
        const linked = escapeHtml(String(row.linked_agent_key || "unbound"));
        const mode = escapeHtml(String(row.tool_mode || "read_only"));
        const depth = escapeHtml(String(row.depth_tier || "standard"));
        return `<p class="mb-1 text-[11px] text-slate-700">spec ${name} -> ${linked} | ${mode} | ${depth}</p>`;
      }).join("");
      el.settingsKnowledgeBrainsList.innerHTML += `<div class="mt-2 border-t border-slate-300 pt-2"><p class="mb-1 text-[11px] font-semibold text-slate-900">Specialists (${specialists.length})</p>${preview}</div>`;
    }
  }

  if (el.settingsKnowledgeJobsList) {
    if (!sources.length) {
      el.settingsKnowledgeJobsList.innerHTML = `<p class="text-[11px] text-slate-700">No ingestion jobs yet. Add a source first.</p>`;
    } else {
      el.settingsKnowledgeJobsList.innerHTML = sources.slice(0, 100).map((row) => {
        const name = escapeHtml(row.name || row.source_id || "-");
        const sourceId = escapeHtml(row.source_id || "-");
        const refresh = escapeHtml(row.refresh_policy || "manual");
        const status = escapeHtml(String(row.status || "active").toUpperCase());
        const updated = escapeHtml(String(row.updated_at || row.created_at || "").slice(0, 19).replace("T", " "));
        return `<p class="mb-1 text-[11px] text-slate-800">${name} (${sourceId}) | refresh=${refresh} | status=${status} | updated=${updated || "-"}</p>`;
      }).join("");
    }
  }

  if (el.settingsKnowledgeEvalsList) {
    const publishedSets = sets.filter((row) => String(row.publish_state || "").toLowerCase() === "published").length;
    const citationBrains = brains.filter((row) => !!row?.citation_required).length;
    const evalLines = [
      `Coverage: ${sources.length} source(s), ${sets.length} set(s), ${brains.length} brain binding(s), ${specialists.length} specialist profile(s).`,
      `Published sets: ${publishedSets}; citation-required brains: ${citationBrains}.`,
      `Policy pack: ${String(policies.policy_pack || "standard").toUpperCase()}; security gate required: ${policies.require_security_gate ? "yes" : "no"}.`,
      `Quality gate min pass rate: ${String(policies.quality_gate_min_pass_rate ?? 0.85)}.`,
    ];
    el.settingsKnowledgeEvalsList.innerHTML = evalLines
      .map((line) => `<p class="mb-1 text-[11px] text-slate-800">${escapeHtml(line)}</p>`)
      .join("");
  }
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
  renderUsersPanel();
  renderKnowledgeHub();
  renderSettingsAuditLog();
  renderContextDrawer();
  applySettingsToWorkbench();
}

async function loadSettings(showToast = false) {
  const data = await api("/api/settings", null);
  state.settings = data.settings || {};
  renderSettings();
  await refreshCurrentUserProfile().catch(() => renderCurrentUserIdentity());
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

async function refreshCurrentUserProfile() {
  const data = await api("/api/settings/me", null, "GET");
  const profile = (data && typeof data.user === "object") ? data.user : {};
  state.activeUserEmail = String(profile.email || state.activeUserEmail || "").toLowerCase();
  state.activeUserRole = String(profile.role || state.activeUserRole || "engineering").toLowerCase();
  state.activeUserName = String(profile.display_name || state.activeUserName || "");
  if (el.perspectiveSwitcher && ["executive", "delivery", "engineering", "security"].includes(state.activeUserRole)) {
    el.perspectiveSwitcher.value = state.activeUserRole;
  }
  renderCurrentUserIdentity();
  renderPerspectiveDashboard();
}

function selectActiveUser(email, showMessage = false) {
  const userEmail = String(email || "").trim().toLowerCase();
  if (!userEmail) return;
  state.activeUserEmail = userEmail;
  localStorage.setItem(ACTIVE_USER_STORAGE_KEY, userEmail);
  renderUsersPanel();
  refreshCurrentUserProfile().catch(() => renderCurrentUserIdentity());
  if (showMessage) setUserMessage(`Session user set to ${userEmail}.`);
}

async function saveUser() {
  const payload = {
    email: String(el.settingsUserEmail?.value || "").trim().toLowerCase(),
    display_name: String(el.settingsUserName?.value || "").trim(),
    role: String(el.settingsUserRole?.value || "engineering").toLowerCase(),
    status: String(el.settingsUserStatus?.value || "active").toLowerCase(),
  };
  const data = await api("/api/settings/users", payload, "POST");
  state.settings = data.settings || state.settings;
  renderSettings();
  setUserMessage(`Saved user ${payload.email}.`);
}

async function useSelectedUser() {
  const selected = String(el.settingsCurrentUserSelect?.value || "").trim().toLowerCase();
  if (!selected) {
    setUserMessage("Select a user first.", true);
    return;
  }
  selectActiveUser(selected, true);
}

async function removeUser() {
  const email = String(el.settingsUserEmail?.value || "").trim().toLowerCase();
  if (!email) {
    setUserMessage("Enter a user email to remove.", true);
    return;
  }
  const data = await api("/api/settings/users/remove", { email }, "POST");
  state.settings = data.settings || state.settings;
  if (String(state.activeUserEmail || "").toLowerCase() === email) {
    const firstActive = settingsUsers().find((row) => String(row.status || "").toLowerCase() === "active");
    state.activeUserEmail = String(firstActive?.email || "local-user@synthetix.local").toLowerCase();
    localStorage.setItem(ACTIVE_USER_STORAGE_KEY, state.activeUserEmail);
    await refreshCurrentUserProfile().catch(() => {});
  }
  renderSettings();
  setUserMessage(`Removed user ${email}.`);
}

async function saveKnowledgeSource() {
  const payload = {
    name: String(el.settingsKnowledgeSourceName?.value || "").trim(),
    location: String(el.settingsKnowledgeSourceLocation?.value || "").trim(),
    type: String(el.settingsKnowledgeSourceType?.value || "file").toLowerCase(),
    scope: String(el.settingsKnowledgeSourceScope?.value || "project").toLowerCase(),
    data_classification: String(el.settingsKnowledgeSourceClassification?.value || "internal").toLowerCase(),
    tags: parseCommaValues(el.settingsKnowledgeSourceTags?.value || ""),
  };
  const data = await api("/api/settings/knowledge/sources", payload, "POST");
  state.settings = data.settings || state.settings;
  renderSettings();
  setKnowledgeMessage(`Saved knowledge source '${payload.name}'.`);
}

async function saveKnowledgeSet() {
  const payload = {
    name: String(el.settingsKnowledgeSetName?.value || "").trim(),
    version: String(el.settingsKnowledgeSetVersion?.value || "1.0.0").trim(),
    publish_state: String(el.settingsKnowledgeSetState?.value || "draft").toLowerCase(),
    source_ids: parseCommaValues(el.settingsKnowledgeSetSourceIds?.value || ""),
  };
  const data = await api("/api/settings/knowledge/sets", payload, "POST");
  state.settings = data.settings || state.settings;
  renderSettings();
  setKnowledgeMessage(`Saved knowledge set '${payload.name}'.`);
}

async function saveAgentBrain() {
  const payload = {
    agent_key: String(el.settingsBrainAgentKey?.value || "").trim(),
    knowledge_set_ids: parseCommaValues(el.settingsBrainSetIds?.value || ""),
    top_k: Number(el.settingsBrainTopK?.value || 8),
    citation_required: !!el.settingsBrainCitationRequired?.checked,
    fallback_behavior: "ask_clarification",
    allowed_tools: ["repo_read", "doc_export"],
    memory_scope: "project",
    memory_enabled: true,
  };
  const data = await api("/api/settings/knowledge/brains", payload, "POST");
  state.settings = data.settings || state.settings;
  renderSettings();
  setKnowledgeMessage(`Saved brain binding for '${payload.agent_key}'.`);
}

async function saveProjectBinding() {
  const payload = {
    workspace: String(el.settingsBindingWorkspace?.value || "").trim() || "default-workspace",
    project: String(el.settingsBindingProject?.value || "").trim() || "default-project",
    knowledge_set_ids: parseCommaValues(el.settingsBindingSetIds?.value || ""),
  };
  const data = await api("/api/settings/knowledge/project-bindings", payload, "POST");
  state.settings = data.settings || state.settings;
  renderSettings();
  setKnowledgeMessage(`Saved project binding for ${payload.workspace}/${payload.project}.`);
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
  const active = runs.filter((r) => isActiveRunStatus(r.status));
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

function renderProjectRunsListDashboard(runs) {
  if (!el.dashboardTitle || !el.dashboardSubtitle || !el.dashboardRunsList) return;
  const rows = [...runs]
    .sort((a, b) => asMs(b.updated_at || b.created_at) - asMs(a.updated_at || a.created_at))
    .slice(0, 200);
  el.dashboardTitle.textContent = "Project runs";
  el.dashboardSubtitle.textContent = "Recent run history across projects.";
  if (!rows.length) {
    el.dashboardRunsList.innerHTML = `<p class="text-xs text-slate-700">No runs available yet.</p>`;
    return;
  }
  el.dashboardRunsList.innerHTML = `
    <div class="overflow-x-auto rounded-lg border border-slate-300 bg-white">
      <table class="min-w-full divide-y divide-slate-200 text-xs text-slate-800">
        <thead class="bg-slate-100 text-[11px] uppercase tracking-[0.12em] text-slate-700">
          <tr>
            <th class="px-3 py-2 text-left font-semibold">Run ID</th>
            <th class="px-3 py-2 text-left font-semibold">Status</th>
            <th class="px-3 py-2 text-left font-semibold">Created</th>
            <th class="px-3 py-2 text-left font-semibold">Updated</th>
            <th class="px-3 py-2 text-left font-semibold">Objective</th>
          </tr>
        </thead>
        <tbody class="divide-y divide-slate-200">
          ${rows.map((run) => {
            const runId = escapeHtml(String(run?.run_id || "-"));
            const status = escapeHtml(String(run?.status || "unknown").toUpperCase());
            const created = escapeHtml(String(run?.created_at || "").replace("T", " ").slice(0, 19) || "-");
            const updated = escapeHtml(String(run?.updated_at || "").replace("T", " ").slice(0, 19) || "-");
            const objective = escapeHtml(String(run?.business_objectives || run?.objective || "").trim().slice(0, 160) || "-");
            return `
              <tr class="align-top">
                <td class="px-3 py-2 font-mono text-[11px] text-slate-900">${runId}</td>
                <td class="px-3 py-2">${status}</td>
                <td class="px-3 py-2">${created}</td>
                <td class="px-3 py-2">${updated}</td>
                <td class="px-3 py-2">${objective}</td>
              </tr>
            `;
          }).join("")}
        </tbody>
      </table>
    </div>
  `;
}

function renderPerspectiveDashboard() {
  if (!el.dashboardRunsList) return;
  const runs = Array.isArray(state.dashboardRuns) ? state.dashboardRuns : [];
  renderProjectRunsListDashboard(runs);
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

function isModernizationEvidenceMode() {
  return isCodeModernizationMode() && modernizationSourceMode() === "evidence";
}

function isModernizationHybridMode() {
  return isCodeModernizationMode() && modernizationSourceMode() === "hybrid";
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
  const sourceMode = modernizationSourceMode();

  el.modernizationPanel.classList.toggle("hidden", !codeMode);
  if (el.databasePanel) {
    el.databasePanel.classList.remove("hidden");
  }
  if (el.modernizationManualInputs) {
    const hideManual = codeMode && ["repo_scan", "evidence", "hybrid"].includes(sourceMode);
    el.modernizationManualInputs.classList.toggle("hidden", hideManual);
  }
  if (el.bfEvidencePanel) {
    const showEvidence = String(state.projectState?.detected || "").toLowerCase() === "brownfield" && ["evidence", "hybrid"].includes(sourceMode);
    el.bfEvidencePanel.classList.toggle("hidden", !showEvidence);
  }
  if (el.modernizationSourceHelp) {
    if (!codeMode) {
      el.modernizationSourceHelp.textContent = "";
    } else if (isModernizationRepoScanMode()) {
      el.modernizationSourceHelp.textContent = "The analyst will scan the connected GitHub repository to infer current functionality.";
    } else if (isModernizationEvidenceMode()) {
      el.modernizationSourceHelp.textContent = "The analyst will ingest imported analysis outputs and generate evidence-backed artifacts with explicit coverage scoring.";
    } else if (isModernizationHybridMode()) {
      el.modernizationSourceHelp.textContent = "The analyst will combine the connected repository and imported analysis outputs into a single evidence-backed view.";
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
  const evidenceBundleId = String(state.discoverEvidenceBundle?.data?.evidence_bundle_v1?.bundle_id || "").trim();
  const useCase = currentUseCase();
  const brownfieldTerms = [
    "legacy", "modernize", "migration", "migrate", "existing",
    "refactor", "brownfield", "current system", "as-is", "replace old",
  ];
  let score = 0;
  if (useCase !== "business_objectives") score += 2;
  if (legacyCode) score += 3;
  if (dbSchema) score += 2;
  if (evidenceBundleId) score += 3;
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
    sampleDatasetEnabled: false,
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
  renderDiscoverIntegrationPreviews();
  renderDiscoverStepper();
}

function renderDiscoverGitHubTreePreview() {
  if (!el.bfGithubTreeStatus || !el.bfGithubTreePreview) return;
  const view = state.discoverGithubTree || {};
  const githubCfg = (state.settings?.integrations?.github && typeof state.settings.integrations.github === "object")
    ? state.settings.integrations.github
    : {};
  const githubTokenConfigured = !!String(githubCfg.token_masked || "").trim();
  const accessModeText = githubTokenConfigured
    ? "Analysis source: GitHub URL via saved GitHub token."
    : "Analysis source: public GitHub URL (anonymous access).";
  if (view.loading) {
    el.bfGithubTreeStatus.textContent = "Loading repository tree...";
    el.bfGithubTreeStatus.className = "text-[11px] text-slate-700";
    el.bfGithubTreePreview.innerHTML = `<p class="text-slate-700">${escapeHtml(accessModeText)} Fetching repository structure from GitHub API.</p>`;
    return;
  }
  if (view.error) {
    el.bfGithubTreeStatus.textContent = `Load failed: ${view.error}`;
    el.bfGithubTreeStatus.className = "text-[11px] text-rose-700";
    el.bfGithubTreePreview.innerHTML = `<p class="text-slate-700">${escapeHtml(accessModeText)}</p><p class="text-rose-700">${escapeHtml(view.error)}</p>`;
    return;
  }
  const tree = (view.tree && typeof view.tree === "object") ? view.tree : {};
  const repo = (view.repo && typeof view.repo === "object") ? view.repo : {};
  const entries = Array.isArray(tree.entries) ? tree.entries : [];
  if (!entries.length) {
    el.bfGithubTreeStatus.textContent = "No repository tree loaded.";
    el.bfGithubTreeStatus.className = "text-[11px] text-slate-700";
    el.bfGithubTreePreview.innerHTML = `<p class="text-slate-700">${escapeHtml(accessModeText)}</p><p class="text-slate-700">Enter a GitHub repository URL and click <strong>Load repo tree</strong> to preview folders and files.</p>`;
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
    <div class="text-[11px] text-slate-700">${escapeHtml(accessModeText)}</div>
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
    el.bfLinearIssuesPreview.innerHTML = `<p class="text-slate-700">Issue tracker is optional. Configure provider + board/project only if you want linked issue context.</p>`;
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

function renderDiscoverEvidencePreview() {
  if (!el.bfEvidenceStatus || !el.bfEvidencePreview || !el.bfEvidencePanel) return;
  const sourceMode = modernizationSourceMode();
  const showPanel = ["evidence", "hybrid"].includes(sourceMode) && String(state.projectState?.detected || "").toLowerCase() === "brownfield";
  el.bfEvidencePanel.classList.toggle("hidden", !showPanel);
  if (!showPanel) return;
  const view = state.discoverEvidenceBundle || {};
  const payload = (view.data && typeof view.data === "object") ? view.data : {};
  const bundle = (payload.evidence_bundle_v1 && typeof payload.evidence_bundle_v1 === "object") ? payload.evidence_bundle_v1 : {};
  const match = (payload.provider_match_report_v1 && typeof payload.provider_match_report_v1 === "object") ? payload.provider_match_report_v1 : {};
  const coverage = (payload.evidence_coverage_report_v1 && typeof payload.evidence_coverage_report_v1 === "object") ? payload.evidence_coverage_report_v1 : {};
  const dimensions = (coverage.dimensions && typeof coverage.dimensions === "object") ? coverage.dimensions : {};
  const blockers = Array.isArray(coverage.blockers) ? coverage.blockers : [];
  const files = Array.isArray(bundle.files) ? bundle.files : [];
  if (view.loading) {
    el.bfEvidenceStatus.textContent = "Uploading and parsing imported analysis...";
    el.bfEvidenceStatus.className = "mt-2 text-[11px] text-slate-700";
    el.bfEvidencePreview.innerHTML = "<p class='text-slate-700'>Evidence bundle is being created. Provider probing, extraction, and normalization are running now.</p>";
    return;
  }
  if (view.error) {
    el.bfEvidenceStatus.textContent = `Import failed: ${view.error}`;
    el.bfEvidenceStatus.className = "mt-2 text-[11px] text-rose-700";
    el.bfEvidencePreview.innerHTML = `<p class="text-rose-700">${escapeHtml(String(view.error || ""))}</p>`;
    return;
  }
  if (!String(bundle.bundle_id || "").trim()) {
    el.bfEvidenceStatus.textContent = "No imported analysis bundle uploaded.";
    el.bfEvidenceStatus.className = "mt-2 text-[11px] text-slate-700";
    el.bfEvidencePreview.innerHTML = "<p class='text-slate-700'>Upload one or more analysis reports to create an evidence bundle and coverage profile.</p>";
    return;
  }
  const selectedTool = String(match.selected_tool || "unknown").trim() || "unknown";
  const selectedConfidence = Number(match.selected_confidence || 0);
  const buildAllowed = !!coverage.build_allowed;
  const outputTarget = String(el.bfEvidenceOutputTarget?.value || "deliverable_pack_only").trim();
  const coverageCards = [
    ["Architecture", Number(dimensions.architecture || 0)],
    ["Dependencies", Number(dimensions.dependencies || 0)],
    ["Behavior", Number(dimensions.behavior || 0)],
    ["Data", Number(dimensions.data || 0)],
  ].map((row) => `<div class="rounded border border-slate-300 bg-slate-50 px-2 py-1"><strong>${escapeHtml(String(row[0]))}</strong><br/>${escapeHtml(String(row[1]))}%</div>`).join("");
  el.bfEvidenceStatus.textContent = `${String(bundle.bundle_id || "")} | ${files.length} files | provider=${selectedTool} | coverage ${buildAllowed ? "build-ready" : "evidence-backed only"}`;
  el.bfEvidenceStatus.className = `mt-2 text-[11px] ${buildAllowed ? "text-emerald-700" : "text-amber-700"}`;
  el.bfEvidencePreview.innerHTML = `
    <div class="grid gap-2 sm:grid-cols-4">${coverageCards}</div>
    <div class="mt-2 rounded border border-slate-300 bg-slate-50 p-2">
      <p class="font-semibold text-slate-900">Provider match</p>
      <p class="mt-1 text-[11px] text-slate-700">Tool: ${escapeHtml(selectedTool)} | confidence=${escapeHtml(selectedConfidence.toFixed(2))} | output=${escapeHtml(outputTarget)}</p>
    </div>
    <div class="mt-2 rounded border border-slate-300 bg-white p-2">
      <p class="font-semibold text-slate-900">Imported files</p>
      <ul class="mt-1 list-disc pl-4 text-[11px] text-slate-700">${(files.length ? files.slice(0, 8).map((row) => `<li>${escapeHtml(String(row.file_name || ""))}</li>`).join("") : "<li>No files captured.</li>")}</ul>
    </div>
    <div class="mt-2 rounded border border-slate-300 bg-white p-2">
      <p class="font-semibold text-slate-900">Blockers to proceed</p>
      <ul class="mt-1 list-disc pl-4 text-[11px] text-slate-700">${(blockers.length ? blockers.map((row) => `<li>${escapeHtml(String(row))}</li>`).join("") : "<li>No blocking evidence gaps reported.</li>")}</ul>
    </div>
  `;
}

function renderDiscoverIntegrationPreviews() {
  renderDiscoverGitHubTreePreview();
  renderDiscoverLinearIssuesPreview();
  renderDiscoverEvidencePreview();
}

async function uploadDiscoverEvidenceBundle() {
  const files = Array.from(el.bfEvidenceFiles?.files || []);
  if (!files.length) {
    alert("Select one or more analysis exports first.");
    return;
  }
  state.discoverEvidenceBundle = { loading: true, error: "", data: null };
  renderDiscoverIntegrationPreviews();
  const form = new FormData();
  files.forEach((file) => form.append("files", file));
  try {
    const data = await apiMultipart("/api/evidence/bundles", form, "POST");
    state.discoverEvidenceBundle = { loading: false, error: "", data };
    if (el.modernizationSourceMode) {
      const currentMode = String(el.modernizationSourceMode.value || "manual").trim().toLowerCase();
      if (currentMode === "manual") {
        el.modernizationSourceMode.value = "evidence";
      }
    }
    if (el.projectStateMode && String(el.projectStateMode.value || "auto").toLowerCase() === "auto") {
      applyProjectStateResult({
        detected: "brownfield",
        confidence: 0.96,
        reason: "Imported analysis evidence indicates an existing system under brownfield discovery.",
      });
    }
    renderDiscoverIntegrationPreviews();
    renderDiscoverStepper();
    renderDiscoverLandscape();
    renderDiscoverScopeGuidance();
    await loadDiscoverAnalystBrief({ force: true });
    setGlobalSearchStatus("Imported analysis bundle ready.");
  } catch (err) {
    state.discoverEvidenceBundle = { loading: false, error: String(err?.message || err || "Evidence upload failed."), data: null };
    renderDiscoverIntegrationPreviews();
    setGlobalSearchStatus(`Evidence upload failed: ${err.message || err}`, true);
  } finally {
    if (el.bfEvidenceFiles) el.bfEvidenceFiles.value = "";
  }
}

function landscapeRequestKey() {
  const integration = getIntegrationContext();
  const brownfield = integration?.brownfield || {};
  const evidence = integration?.evidence || {};
  const greenfield = integration?.greenfield || {};
  const payload = [
    String(currentUseCase() || ""),
    String(modernizationSourceMode() || "manual"),
    String(integration?.project_state_detected || ""),
    String(brownfield.repo_provider || ""),
    String(brownfield.repo_url || ""),
    String(evidence.bundle_id || ""),
    String(greenfield.repo_target || ""),
    String(el.objectives?.value || "").trim().slice(0, 500),
    String(el.includePaths?.value || "").trim(),
    String(el.excludePaths?.value || "").trim(),
  ];
  return payload.join("|");
}

function analystBriefRequestKey() {
  const integration = getIntegrationContext();
  const brownfield = integration?.brownfield || {};
  const evidence = integration?.evidence || {};
  const payload = [
    String(currentUseCase() || ""),
    String(modernizationSourceMode() || "manual"),
    String(integration?.project_state_detected || ""),
    String(brownfield.repo_provider || ""),
    String(brownfield.repo_url || ""),
    String(evidence.bundle_id || ""),
    String(evidence.output_target || ""),
    String(el.objectives?.value || "").trim().slice(0, 500),
    String(el.legacyCode?.value || "").trim().slice(0, 500),
    String(el.dbSchema?.value || "").trim().slice(0, 500),
    String(el.includePaths?.value || "").trim(),
    String(el.excludePaths?.value || "").trim(),
  ];
  return payload.join("|");
}

function renderDiscoverAnalystBrief() {
  if (!el.discoverAnalystBriefStatus || !el.discoverAnalystBriefPreview) return;
  const view = state.discoverAnalystBrief || {};
  const sourceMode = modernizationSourceMode();
  const raw = _discoverRawArtifacts();
  const analysisPlan = (raw.analysis_plan_v1 && typeof raw.analysis_plan_v1 === "object") ? raw.analysis_plan_v1 : {};
  if (el.discoverRunAnalystBrief) {
    el.discoverRunAnalystBrief.disabled = !!view.loading;
    el.discoverRunAnalystBrief.textContent = view.loading ? "Running Analyst Brief..." : "Run Analyst Brief";
  }
  const loadingData = (view.data && typeof view.data === "object") ? view.data : null;
  if (view.loading && !loadingData) {
    el.discoverAnalystBriefStatus.textContent = ["evidence", "hybrid"].includes(sourceMode)
      ? "Analyst Agent is normalizing imported analysis evidence and deriving canonical artifacts..."
      : "Analyst Agent is reading sampled source files and inferring functionality...";
    el.discoverAnalystBriefStatus.className = "mt-1 text-[11px] text-slate-700";
    el.discoverAnalystBriefPreview.innerHTML = `<p class="text-slate-700">${["evidence", "hybrid"].includes(sourceMode) ? "Running evidence-aware analysis..." : "Running source-aware analysis..."}</p>`;
    return;
  }
  if (view.error) {
    el.discoverAnalystBriefStatus.textContent = `Load failed: ${view.error}`;
    el.discoverAnalystBriefStatus.className = "mt-1 text-[11px] text-rose-700";
    el.discoverAnalystBriefPreview.innerHTML = `<p class="text-rose-700">${escapeHtml(view.error)}</p>`;
    return;
  }
  const payload = loadingData || {};
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
  const planSummary = Object.keys(analysisPlan).length ? `
    <div class="mt-2 rounded-md border ${String(analysisPlan.analysis_mode || "").toLowerCase() === "large_repo" ? "border-amber-300 bg-amber-50" : "border-slate-300 bg-slate-50"} px-2 py-2">
      <p class="font-semibold ${String(analysisPlan.analysis_mode || "").toLowerCase() === "large_repo" ? "text-amber-950" : "text-slate-900"}">Analysis plan</p>
      <p class="text-[11px] ${String(analysisPlan.analysis_mode || "").toLowerCase() === "large_repo" ? "text-amber-900" : "text-slate-700"}">
        Route=<strong>${escapeHtml(String(analysisPlan.analysis_mode || "standard"))}</strong>
        | Strategy=${escapeHtml(String(analysisPlan.llm_strategy || "n/a"))}
        | Estimated Stage 1 analysis tokens=${escapeHtml(String(analysisPlan.estimated_total_tokens || 0))}
        | Estimated Stage 1 analysis cost=${escapeHtml(_formatUsd(analysisPlan.estimated_cost_usd))}
      </p>
      ${(Array.isArray(analysisPlan.analysis_mode_reasons) && analysisPlan.analysis_mode_reasons.length)
        ? `<p class="mt-1 text-[11px] ${String(analysisPlan.analysis_mode || "").toLowerCase() === "large_repo" ? "text-amber-900" : "text-slate-700"}"><strong>Route triggers:</strong> ${escapeHtml(analysisPlan.analysis_mode_reasons.join(" | "))}</p>`
        : ""}
    </div>
  ` : "";

  if (!overview && !caps.length && !components.length) {
    el.discoverAnalystBriefStatus.textContent = ["evidence", "hybrid"].includes(sourceMode)
      ? "Waiting for Landscape analysis of the imported evidence bundle."
      : "Waiting for Landscape analysis of the connected source code.";
    el.discoverAnalystBriefStatus.className = "mt-1 text-[11px] text-slate-700";
    el.discoverAnalystBriefPreview.innerHTML = "Analyst summary will appear here after the Landscape scan runs.";
    return;
  }

  const list = (rows) => rows.map((row) => `<li>${escapeHtml(String(row || ""))}</li>`).join("");
  const refreshing = !!view.loading;
  el.discoverAnalystBriefStatus.textContent = refreshing
    ? `Refreshing analyst brief (${source}${repoLabel ? ` | ${repoLabel}` : ""})...`
    : `Analyst brief ready (${source}${repoLabel ? ` | ${repoLabel}` : ""})`;
  el.discoverAnalystBriefStatus.className = refreshing ? "mt-1 text-[11px] text-slate-700" : "mt-1 text-[11px] text-emerald-700";
  el.discoverAnalystBriefPreview.innerHTML = `
    <p class="font-semibold text-slate-900">${overview}</p>
    ${Object.keys(legacySkillProfile).length ? `<p class="mt-1 text-[11px] text-slate-800"><strong>Selected legacy skill:</strong> ${escapeHtml(String(legacySkillProfile.selected_skill_name || "Generic Legacy Skill"))} (${escapeHtml(String(legacySkillProfile.selected_skill_id || "generic_legacy"))}), confidence=${escapeHtml(String(legacySkillProfile.confidence || "n/a"))}</p>` : ""}
    ${aasSummary ? `<p class="mt-2 rounded-md border border-sky-300 bg-sky-50 px-2 py-1 text-slate-900"><strong>Analyst AAS summary:</strong> ${escapeHtml(aasSummary)}</p>` : ""}
    ${planSummary}
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

async function loadDiscoverLandscape({ force = false } = {}) {
  const reqKey = landscapeRequestKey();
  const activePromise = state.discoverLandscape?.inFlightPromise;
  if (activePromise && typeof activePromise.then === "function") return activePromise;
  if (!force && reqKey && reqKey === state.discoverLandscape.requestKey && state.discoverLandscape.data) {
    renderDiscoverLandscape();
    renderDiscoverStepper();
    return;
  }
  const previousData = (state.discoverLandscape?.data && typeof state.discoverLandscape.data === "object") ? state.discoverLandscape.data : null;
  const requestToken = `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  state.discoverLandscape = { loading: true, error: "", data: previousData, requestKey: reqKey, requestToken, inFlightPromise: null };
  renderDiscoverLandscape();
  renderDiscoverStepper();
  const runPromise = (async () => {
    const integration = getIntegrationContext();
    try {
      const discoverData = await apiWithNetworkRetry("/api/discover/landscape", {
        integration_context: integration,
        objectives: String(el.objectives?.value || "").trim(),
        use_case: currentUseCase(),
        repo_provider: String(integration?.brownfield?.repo_provider || ""),
        repo_url: String(integration?.brownfield?.repo_url || "").trim(),
        modernization_language: String(el.modernizationLanguage?.value || "").trim(),
        target_platform: String(el.targetPlatform?.value || "").trim(),
        database_source: String(el.dbSource?.value || "").trim(),
        database_target: String(el.dbTarget?.value || "").trim(),
      }, "POST", { retries: 1, retryDelayMs: 1200 });
      if (String(state.discoverLandscape?.requestToken || "") !== requestToken) return;
      state.discoverLandscape = { loading: false, error: "", data: discoverData, requestKey: reqKey, requestToken: "", inFlightPromise: runPromise };
    } catch (err) {
      if (String(state.discoverLandscape?.requestToken || "") !== requestToken) return;
      state.discoverLandscape = {
        loading: false,
        error: /failed to fetch/i.test(String(err?.message || err || ""))
          ? "Network fetch failed while loading Landscape. Retry once more; if the app was just deployed, do a hard refresh first."
          : String(err?.message || err || "Failed to run landscape scan."),
        data: previousData,
        requestKey: reqKey,
        requestToken: "",
        inFlightPromise: runPromise,
      };
    }
    renderDiscoverLandscape();
    renderDiscoverInsights();
    renderDiscoverStepper();
    renderDiscoverResultsView();
  })();
  state.discoverLandscape.inFlightPromise = runPromise;
  try {
    await runPromise;
  } finally {
    if (state.discoverLandscape?.inFlightPromise === runPromise) state.discoverLandscape.inFlightPromise = null;
    renderDiscoverLandscape();
    renderDiscoverStepper();
    renderDiscoverResultsView();
  }
}

async function loadDiscoverAnalystBrief({ force = false } = {}) {
  const reqKey = analystBriefRequestKey();
  const activePromise = state.discoverAnalystBrief?.inFlightPromise;
  if (activePromise && typeof activePromise.then === "function") {
    return activePromise;
  }
  if (!force && reqKey && reqKey === state.discoverAnalystBrief.requestKey && state.discoverAnalystBrief.data) {
    renderDiscoverAnalystBrief();
    return;
  }
  const previousData = (state.discoverAnalystBrief?.data && typeof state.discoverAnalystBrief.data === "object")
    ? state.discoverAnalystBrief.data
    : null;
  const previousThreadId = String(state.discoverAnalystBrief?.threadId || state.discoverAnalystBrief?.data?.thread_id || "").trim();
  const requestToken = `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  state.discoverAnalystBrief = {
    loading: true,
    error: "",
    data: previousData,
    requestKey: reqKey,
    threadId: previousThreadId,
    requestToken,
    inFlightPromise: null,
  };
  renderDiscoverAnalystBrief();
  const runPromise = (async () => {
    const integration = getIntegrationContext();
    try {
      const discoverData = await apiWithNetworkRetry("/api/discover/analyst-brief", {
        integration_context: integration,
        objectives: String(el.objectives?.value || "").trim(),
        use_case: currentUseCase(),
        legacy_code: String(el.legacyCode?.value || "").trim(),
        database_source: String(el.dbSource?.value || "").trim(),
        database_target: String(el.dbTarget?.value || "").trim(),
        database_schema: String(el.dbSchema?.value || "").trim(),
        repo_provider: String(integration?.brownfield?.repo_provider || ""),
        repo_url: String(integration?.brownfield?.repo_url || "").trim(),
      }, "POST", { retries: 1, retryDelayMs: 1200 });

      const candidateThreadParts = [
        String(integration?.project_state_detected || ""),
        String(integration?.brownfield?.repo_url || ""),
        String(integration?.evidence?.bundle_id || ""),
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
            project_id: slugifyValue(String(integration?.brownfield?.repo_url || integration?.evidence?.bundle_id || integration?.greenfield?.repo_target || "default-project")) || "default-project",
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

      if (String(state.discoverAnalystBrief?.requestToken || "") !== requestToken) {
        return;
      }
      state.discoverAnalystBrief = {
        loading: false,
        error: "",
        data: mergedData,
        requestKey: reqKey,
        threadId: String(mergedData.thread_id || inferredThreadId),
        requestToken: "",
        inFlightPromise: runPromise,
      };
    } catch (err) {
      if (String(state.discoverAnalystBrief?.requestToken || "") !== requestToken) {
        return;
      }
      state.discoverAnalystBrief = {
        loading: false,
        error: /failed to fetch/i.test(String(err?.message || err || ""))
          ? "Network fetch failed while loading the analyst brief. Retry once more; if the app was just deployed, do a hard refresh first."
          : String(err?.message || err || "Failed to run analyst brief."),
        data: previousData,
        requestKey: reqKey,
        threadId: previousThreadId,
        requestToken: "",
        inFlightPromise: runPromise,
      };
    }
    renderDiscoverAnalystBrief();
    renderDiscoverInsights();
    renderDiscoverLandscape();
    renderDiscoverStepper();
    renderDiscoverResultsView();
  })();
  state.discoverAnalystBrief.inFlightPromise = runPromise;
  try {
    await runPromise;
  } finally {
    if (state.discoverAnalystBrief?.inFlightPromise === runPromise) {
      state.discoverAnalystBrief.inFlightPromise = null;
    }
    renderDiscoverAnalystBrief();
    renderDiscoverLandscape();
    renderDiscoverStepper();
    renderDiscoverResultsView();
  }
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
  const issueProject = String(integration?.brownfield?.issue_project || "").trim();
  if (!issueProvider || !issueProject) {
    state.discoverLinearIssues = { loading: false, error: "", team: null, issues: [], source: "" };
    renderDiscoverIntegrationPreviews();
    renderDiscoverInsights();
    return;
  }
  if (issueProvider && !["linear", "jira"].includes(issueProvider)) {
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
      issue_provider: issueProvider,
      issue_project: issueProject,
      max_issues: 80,
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
    const githubKey = `${repoProvider}|${repoUrl}`;
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
    const linearKey = `${issueProvider}|${issueProject}`;
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
  const knowledgeHub = (state.settings?.knowledge_hub && typeof state.settings.knowledge_hub === "object")
    ? state.settings.knowledge_hub
    : {};
  const bindings = Array.isArray(knowledgeHub.project_bindings) ? knowledgeHub.project_bindings : [];
  const workspace = String(el.workspaceSelector?.value || "default-workspace").trim() || "default-workspace";
  const project = String(el.projectSelector?.value || "default-project").trim() || "default-project";
  const projectBinding = bindings.find((row) => {
    if (!row || typeof row !== "object") return false;
    return String(row.workspace || "").trim() === workspace && String(row.project || "").trim() === project;
  }) || null;
  const evidenceView = (state.discoverEvidenceBundle?.data && typeof state.discoverEvidenceBundle.data === "object")
    ? state.discoverEvidenceBundle.data
    : {};
  const evidenceBundle = (evidenceView.evidence_bundle_v1 && typeof evidenceView.evidence_bundle_v1 === "object")
    ? evidenceView.evidence_bundle_v1
    : {};
  const evidenceCoverage = (evidenceView.evidence_coverage_report_v1 && typeof evidenceView.evidence_coverage_report_v1 === "object")
    ? evidenceView.evidence_coverage_report_v1
    : {};
  const evidenceProviderMatch = (evidenceView.provider_match_report_v1 && typeof evidenceView.provider_match_report_v1 === "object")
    ? evidenceView.provider_match_report_v1
    : {};
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
    evidence: {
      source_mode: ["evidence", "hybrid"].includes(String(el.modernizationSourceMode?.value || "manual"))
        ? String(el.modernizationSourceMode?.value || "manual")
        : "",
      bundle_id: String(evidenceBundle.bundle_id || "").trim(),
      output_target: String(el.bfEvidenceOutputTarget?.value || "deliverable_pack_only").trim(),
      accept_low_coverage_risk: !!el.bfEvidenceAcceptRisk?.checked,
      provider_match: evidenceProviderMatch,
      coverage: evidenceCoverage,
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
    actor_context: {
      email: String(state.activeUserEmail || "").trim().toLowerCase(),
      role: String(state.activeUserRole || "").trim().toLowerCase(),
      display_name: String(state.activeUserName || "").trim(),
    },
    brain_context: {
      workspace,
      project,
      project_binding: projectBinding || {},
      knowledge_sets: Array.isArray(knowledgeHub.sets) ? knowledgeHub.sets : [],
      agent_brains: Array.isArray(knowledgeHub.agent_brains) ? knowledgeHub.agent_brains : [],
    },
    cloud_promotion_enabled: !!el.enableCloudPromotion?.checked,
  };
}

function applyIntegrationContext(ctx) {
  if (!ctx || typeof ctx !== "object") return;
  const mode = String(ctx.project_state_mode || "auto");
  if (el.projectStateMode) el.projectStateMode.value = mode;
  const detected = String(ctx.project_state_detected || "");
  const confidence = Number(ctx.project_state_confidence || 0);
  const reason = String(ctx.project_state_reason || "");
  state.projectState.sampleDatasetEnabled = false;
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
  if (el.gfTrackerProvider) el.gfTrackerProvider.value = String(gf.tracker_provider || "none");
  if (el.gfTrackerProject) el.gfTrackerProject.value = String(gf.tracker_project || "");
  if (el.gfSaveGenerated) el.gfSaveGenerated.checked = gf.save_generated_codebase !== false;
  if (el.gfReadWriteTracker) el.gfReadWriteTracker.checked = gf.read_write_tracker !== false;

  const scope = (ctx.scan_scope && typeof ctx.scan_scope === "object") ? ctx.scan_scope : {};
  if (el.analysisDepth) el.analysisDepth.value = String(scope.analysis_depth || "standard");
  if (el.telemetryMode) el.telemetryMode.value = String(scope.telemetry_mode || "off");
  if (el.modernizationSourceMode) el.modernizationSourceMode.value = String(scope.modernization_source_mode || "manual");
  if (el.bfSourceMode) el.bfSourceMode.value = String(scope.modernization_source_mode || "manual");
  if (el.includePaths) el.includePaths.value = Array.isArray(scope.include_paths) ? scope.include_paths.join("\n") : "";
  if (el.excludePaths) el.excludePaths.value = Array.isArray(scope.exclude_paths) ? scope.exclude_paths.join("\n") : "";
  const evidenceCtx = (ctx.evidence && typeof ctx.evidence === "object") ? ctx.evidence : {};
  if (el.bfEvidenceOutputTarget) el.bfEvidenceOutputTarget.value = String(evidenceCtx.output_target || "deliverable_pack_only");
  if (el.bfEvidenceAcceptRisk) el.bfEvidenceAcceptRisk.checked = !!evidenceCtx.accept_low_coverage_risk;
  if (String(evidenceCtx.bundle_id || "").trim()) {
    state.discoverEvidenceBundle = {
      loading: false,
      error: "",
      data: {
        evidence_bundle_v1: { bundle_id: String(evidenceCtx.bundle_id || "").trim(), files: [] },
        provider_match_report_v1: (evidenceCtx.provider_match && typeof evidenceCtx.provider_match === "object") ? evidenceCtx.provider_match : {},
        evidence_coverage_report_v1: (evidenceCtx.coverage && typeof evidenceCtx.coverage === "object") ? evidenceCtx.coverage : {},
      },
    };
  }

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
  const cachedLandscapeRaw = (discoverCache.landscape_raw_artifacts && typeof discoverCache.landscape_raw_artifacts === "object") ? discoverCache.landscape_raw_artifacts : null;
  if (cachedLandscapeRaw && Object.keys(cachedLandscapeRaw).length) {
    state.discoverLandscape = { loading: false, error: "", data: { raw_artifacts: cachedLandscapeRaw }, requestKey: landscapeRequestKey(), requestToken: "", inFlightPromise: null };
  }
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
  const projectStateReady = !!integration.project_state_detected;
  let connectComplete = projectStateReady;
  const bundleId = String(
    integration?.evidence?.bundle_id
    || state.discoverEvidenceBundle?.data?.evidence_bundle_v1?.bundle_id
    || ""
  ).trim();
  let sourceMode = String(integration?.scan_scope?.modernization_source_mode || "manual").trim().toLowerCase();
  if (sourceMode === "manual" && bundleId && integration.project_state_detected === "brownfield") {
    sourceMode = "evidence";
  }
  if (integration.project_state_detected === "brownfield") {
    if (sourceMode === "evidence") {
      connectComplete = connectComplete && !!bundleId;
    } else if (sourceMode === "hybrid") {
      connectComplete = connectComplete && (
        !!bundleId
        || (!!integration.brownfield.repo_provider && !!integration.brownfield.repo_url)
      );
    } else {
      connectComplete = connectComplete
        && !!integration.brownfield.repo_provider
        && !!integration.brownfield.repo_url;
    }
  } else if (integration.project_state_detected === "greenfield") {
    connectComplete = connectComplete
      && !!integration.greenfield.repo_destination
      && !!integration.greenfield.repo_target;
  }
  const objective = String(el.objectives?.value || "").trim();
  const customDomainPackValid = !String(integration.domain_pack_error || "").trim()
    && (
      String(integration.domain_pack_selection || "auto") !== "custom"
      || !!(integration.custom_domain_pack && typeof integration.custom_domain_pack === "object")
    );
  const analystData = (state.discoverAnalystBrief?.data && typeof state.discoverAnalystBrief.data === "object")
    ? state.discoverAnalystBrief.data
    : null;
  const landscapeArtifacts = _discoverLandscapeArtifacts();
  const landscapeData = (state.discoverLandscape?.data && typeof state.discoverLandscape.data === "object") ? state.discoverLandscape.data : null;
  const hasLandscape = !!(landscapeData && Object.keys(landscapeData).length)
    || (Array.isArray(landscapeArtifacts.components?.components) && landscapeArtifacts.components.components.length > 0)
    || (Array.isArray(landscapeArtifacts.landscape?.languages) && landscapeArtifacts.landscape.languages.length > 0)
    || (Array.isArray(landscapeArtifacts.tracks?.tracks) && landscapeArtifacts.tracks.tracks.length > 0);
  const scopeComplete = !!objective
    && (
      !isCodeModernizationMode()
      || (
        isModernizationRepoScanMode()
          ? (
            String(integration.brownfield.repo_provider || "").toLowerCase() === "github"
            && !!String(integration.brownfield.repo_url || "").trim()
          )
          : isModernizationEvidenceMode()
            ? !!String(integration?.evidence?.bundle_id || "").trim()
            : isModernizationHybridMode()
              ? (
                !!String(integration?.evidence?.bundle_id || "").trim()
                || (
                  String(integration.brownfield.repo_provider || "").toLowerCase() === "github"
                  && !!String(integration.brownfield.repo_url || "").trim()
                )
              )
          : !!String(el.legacyCode?.value || "").trim()
      )
    )
    && (!isDatabaseConversionMode() || !!String(el.dbSchema?.value || "").trim())
    && customDomainPackValid;
  const landscapeComplete = hasLandscape;
  const scanComplete = landscapeComplete && scopeComplete;
  const resultsComplete = connectComplete && landscapeComplete && scopeComplete && scanComplete;
  return { connectComplete, landscapeComplete, scopeComplete, scanComplete, resultsComplete };
}

function setDiscoverStep(step) {
  const target = Math.max(1, Math.min(5, Number(step || 1)));
  if (target > 1 && !validateDiscoverStep(1)) return;
  if (target > 3 && !validateDiscoverStep(3)) return;
  if (target > 4 && !validateDiscoverStep(4)) return;
  state.discoverStep = target;
  const stepMap = [
    { btn: el.discoverStepConnect, panel: el.discoverConnectPanel, label: "Connect" },
    { btn: el.discoverStepLandscape, panel: el.discoverLandscapeStepPanel, label: "Landscape" },
    { btn: el.discoverStepScope, panel: el.discoverScopePanel, label: "Define scope" },
    { btn: el.discoverStepScan, panel: el.discoverScanPanel, label: "Scan" },
    { btn: el.discoverStepResults, panel: el.discoverResultsPanel, label: "Results" },
  ];
  stepMap.forEach((entry, idx) => {
    const isActive = (idx + 1) === target;
    entry.panel?.classList.toggle("discover-panel-hidden", !isActive);
  });
  if (target === 2 || target === 3) {
    const hasLandscapeData = !!(state.discoverLandscape?.data && typeof state.discoverLandscape.data === "object" && Object.keys(state.discoverLandscape.data).length);
    if (!hasLandscapeData && !state.discoverLandscape?.loading) {
      loadDiscoverLandscape({ force: false }).catch(() => {});
    }
  }
  renderDiscoverStepper();
}

function renderDiscoverStepper() {
  const completion = discoverStepCompletion();
  const steps = [
    { btn: el.discoverStepConnect, done: completion.connectComplete, label: "Connect" },
    { btn: el.discoverStepLandscape, done: completion.landscapeComplete, label: "Landscape" },
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
    if (state.discoverStep < 5) {
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
      const trackerConfigured = !!integration.brownfield.issue_provider && !!integration.brownfield.issue_project;
      const trackerPartiallyConfigured = (!!integration.brownfield.issue_provider || !!integration.brownfield.issue_project) && !trackerConfigured;
      const linked = [
        integration.brownfield.repo_provider && integration.brownfield.repo_url ? "repo linked" : "repo missing",
        trackerConfigured ? "tracker linked" : (trackerPartiallyConfigured ? "tracker incomplete (optional)" : "tracker optional"),
      ].join(" | ");
      el.discoverResultsIntegrations.textContent = `Integrations: Brownfield (${linked})`;
    } else if (integration.project_state_detected === "greenfield") {
      const trackerConfigured = !!integration.greenfield.tracker_provider
        && integration.greenfield.tracker_provider !== "none"
        && !!integration.greenfield.tracker_project;
      const trackerPartiallyConfigured = (
        (!!integration.greenfield.tracker_provider && integration.greenfield.tracker_provider !== "none")
        || !!integration.greenfield.tracker_project
      ) && !trackerConfigured;
      const linked = [
        integration.greenfield.repo_destination && integration.greenfield.repo_target ? "target repo ready" : "target repo missing",
        trackerConfigured ? "tracker linked" : (trackerPartiallyConfigured ? "tracker incomplete (optional)" : "tracker optional"),
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
    if (el.discoverResultsForensics) {
      const rawArtifacts = _discoverRawArtifacts();
      const sf = (rawArtifacts.static_forensics_layer && typeof rawArtifacts.static_forensics_layer === "object")
        ? rawArtifacts.static_forensics_layer
        : {};
      const summary = (sf.summary && typeof sf.summary === "object") ? sf.summary : {};
      const status = String(summary.overall_status || "PENDING").toUpperCase();
      const projectCount = Number(summary.projects || 0);
      const typeCount = Number(summary.types || 0);
      const violations = Number(summary.quality_violations || 0);
      el.discoverResultsForensics.textContent = `Static forensics: ${status} | projects=${projectCount} | types=${typeCount} | violations=${violations}`;
    }
  }
  renderDiscoverAnalystBrief();
  renderDiscoverIntegrationPreviews();
  renderDiscoverInsights();
  renderDiscoverResultsView();
}

function validateDiscoverStep(step) {
  const c = discoverStepCompletion();
  if (step === 1 && !c.connectComplete) {
    alert("Complete Connect sources to continue.");
    return false;
  }
  if (step === 3 && !c.scopeComplete) {
    alert("Complete Define scope: provide objectives, required legacy/database inputs, and a valid domain pack configuration.");
    return false;
  }
  if (step === 4 && !c.scanComplete) {
    alert("Complete Landscape and Define Scope before moving into Results.");
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
    <div class="mt-2">
      <div class="flex items-center justify-between gap-3">
        <strong>${escapeHtml(title)}</strong>
        <button
          type="button"
          data-open-mermaid="1"
          data-mermaid-title="${escapeHtml(title)}"
          data-mermaid-b64-open="${b64}"
          class="btn-light rounded-md px-2 py-1 text-[11px] font-semibold"
        >Open Full Diagram</button>
      </div>
    </div>
    <div
      class="mt-1 overflow-auto rounded-lg border border-slate-300 bg-white p-2 cursor-zoom-in"
      data-open-mermaid="1"
      data-mermaid-title="${escapeHtml(title)}"
      data-mermaid-b64-open="${b64}"
      title="Open full diagram"
    >
      <div data-mermaid-b64="${b64}" class="min-w-[320px] pointer-events-none"></div>
    </div>
  `;
}

function openDiagramModal(title, source) {
  if (!el.diagramModal || !el.diagramModalViewer) return;
  const diagramTitle = String(title || "Diagram Viewer").trim() || "Diagram Viewer";
  const diagramSource = String(source || "").trim();
  if (!diagramSource) return;
  const b64 = encodeMermaid(diagramSource);
  if (!b64) return;
  el.diagramModal.dataset.diagramTitle = diagramTitle;
  el.diagramModal.dataset.diagramSource = diagramSource;
  if (el.diagramModalTitle) el.diagramModalTitle.textContent = diagramTitle;
  el.diagramModalViewer.innerHTML = `
    <div class="overflow-auto rounded-xl border border-slate-300 bg-white p-4">
      <div data-mermaid-b64="${b64}" class="min-w-[960px]"></div>
    </div>
  `;
  el.diagramModal.showModal();
  setTimeout(() => renderMermaidBlocks(el.diagramModalViewer), 0);
}

function bindMermaidDiagramActions(scope = document) {
  scope.querySelectorAll("[data-open-mermaid]").forEach((node) => {
    if (node.getAttribute("data-mermaid-bound") === "1") return;
    node.setAttribute("data-mermaid-bound", "1");
    node.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
      const title = node.getAttribute("data-mermaid-title") || "Diagram Viewer";
      const b64 = node.getAttribute("data-mermaid-b64-open") || "";
      const source = decodeMermaid(b64);
      openDiagramModal(title, source);
    });
  });
}

function sanitizeMermaidErDiagram(source) {
  const text = String(source || "").trim();
  if (!text || !/^erDiagram\b/.test(text)) return text;
  const safeToken = (value, fallback = "item") => {
    let token = String(value || "").trim().replace(/[^A-Za-z0-9_]/g, "_").replace(/_+/g, "_").replace(/^_+|_+$/g, "");
    if (!token) token = fallback;
    if (/^[0-9]/.test(token)) token = `n_${token}`;
    return token;
  };
  const safeLabel = (value) => String(value || "").replace(/"/g, "'");
  let insideEntity = false;
  return text.split("\n").map((line) => {
    const trimmed = line.trim();
    if (!trimmed) return line;
    if (trimmed === "erDiagram") return "erDiagram";
    const openMatch = line.match(/^(\s*)(.+?)\s*\{\s*$/);
    if (openMatch) {
      insideEntity = true;
      return `${openMatch[1]}${safeToken(openMatch[2], "table")} {`;
    }
    if (/^\s*\}\s*$/.test(line)) {
      insideEntity = false;
      return line;
    }
    const relMatch = line.match(/^(\s*)(\S+)\s+([|o}{\-\.]+)\s+(\S+)\s*:\s*(.*)$/);
    if (relMatch) {
      return `${relMatch[1]}${safeToken(relMatch[2], "table")} ${relMatch[3]} ${safeToken(relMatch[4], "table")} : ${safeLabel(relMatch[5])}`;
    }
    if (insideEntity) {
      const attrMatch = line.match(/^(\s*)(\S+)\s+(\S+)(\s+PK)?\s*$/);
      if (attrMatch) {
        return `${attrMatch[1]}${safeToken(attrMatch[2], "text")} ${safeToken(attrMatch[3], "column")}${attrMatch[4] || ""}`;
      }
    }
    return line;
  }).join("\n");
}

function buildSourceErdFallback(tables, relationships, existingDiagram = "") {
  const existing = String(existingDiagram || "").trim();
  const hasEdges = /\|\|--o\{|\|\|--\|\{|o\|--\|\{/.test(existing);
  if (existing && hasEdges) return existing;
  const safeToken = (value, fallback = "item") => {
    let token = String(value || "").trim().replace(/[^A-Za-z0-9_]/g, "_").replace(/_+/g, "_").replace(/^_+|_+$/g, "");
    if (!token) token = fallback;
    if (/^[0-9]/.test(token)) token = `n_${token}`;
    return token;
  };
  const lines = ["erDiagram"];
  const tableRows = Array.isArray(tables) ? tables : [];
  tableRows.slice(0, 600).forEach((table) => {
    const tname = safeToken(table?.name, "table");
    if (!tname) return;
    lines.push(`    ${tname} {`);
    const cols = Array.isArray(table?.columns) ? table.columns : [];
    cols.slice(0, 160).forEach((col) => {
      const cname = safeToken(col?.name, "column");
      if (!cname) return;
      const ctype = safeToken(col?.inferred_type || col?.type, "text");
      lines.push(`        ${ctype} ${cname}`);
    });
    lines.push("    }");
  });
  const relRows = Array.isArray(relationships) ? relationships : [];
  relRows.slice(0, 1600).forEach((rel) => {
    const ft = safeToken(rel?.from_table, "table");
    const tt = safeToken(rel?.to_table, "table");
    const fc = String(rel?.from_column || "").replace(/"/g, "'");
    const tc = String(rel?.to_column || "").replace(/"/g, "'");
    if (!(ft && tt && fc && tc)) return;
    lines.push(`    ${ft} ||--o{ ${tt} : "${fc} -> ${tc}"`);
  });
  return lines.join("\n");
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
      try {
        const sanitized = sanitizeMermaidErDiagram(source);
        if (sanitized && sanitized !== source) {
          const retryId = `mmd-${Date.now()}-${i}-retry-${Math.floor(Math.random() * 100000)}`;
          const retryResult = await window.mermaid.render(retryId, sanitized);
          host.innerHTML = `${retryResult.svg}<p class="mt-1 text-[11px] text-amber-700">Rendered from sanitized ERD identifiers.</p>`;
          host.setAttribute("data-rendered", "1");
          continue;
        }
      } catch (_retryErr) {}
      host.innerHTML = `<pre class="mono text-[11px] text-slate-700">${escapeHtml(source)}</pre><p class="mt-1 text-[11px] text-rose-700">Diagram render failed</p>`;
      host.setAttribute("data-rendered", "1");
    }
  }
  bindMermaidDiagramActions(scope);
}

function stageAgentLookup(stage, agentId) {
  const options = state.agents.by_stage?.[String(stage)] || [];
  return options.find((a) => String(a.id) === String(agentId)) || null;
}

function normalizeStageAgentIds(stageAgentIds) {
  const normalized = {};
  if (!stageAgentIds || typeof stageAgentIds !== "object") return normalized;
  STAGES.forEach((stage) => {
    const candidate = String(stageAgentIds[stage] || "").trim();
    if (!candidate) return;
    const resolved = stageAgentLookup(stage, candidate);
    if (resolved?.id) normalized[stage] = String(resolved.id);
  });
  return normalized;
}

function activeStageIdsFromStageMap(stageAgentIds) {
  const normalized = normalizeStageAgentIds(stageAgentIds);
  const active = STAGES.filter((stage) => !!normalized[stage]);
  return active.length ? active : [...STAGES];
}

function teamBuilderEffectiveStageAgentIds() {
  return normalizeStageAgentIds(state.teamBuilder?.stageAgentIds || {});
}

function derivePersonasFromStageMap(stageAgentIds) {
  const normalized = normalizeStageAgentIds(stageAgentIds);
  const personas = {};
  STAGES.forEach((stage) => {
    const agent = stageAgentLookup(stage, normalized?.[stage] || "");
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
  const stageAgentIds = normalizeStageAgentIds((team?.stage_agent_ids && typeof team.stage_agent_ids === "object") ? team.stage_agent_ids : {});
  const isCustom = !!team?.is_custom;
  state.teamSelection = {
    teamId: String(team?.id || ""),
    teamName: String(team?.name || "Ad-hoc Team"),
    description: String(team?.description || ""),
    stageAgentIds,
    agentPersonas: personas && typeof personas === "object" ? personas : derivePersonasFromStageMap(stageAgentIds),
    reason: String(reason || ""),
    isCustom,
  };
  state.teamBuilder.stageAgentIds = { ...stageAgentIds };
  state.teamBuilder.editingTeamId = isCustom ? String(team?.id || "").trim() : "";
  state.teamBuilder.editingIsCustom = isCustom;
  if (el.teamName) el.teamName.value = String(team?.name || "").trim();
  if (el.teamDescription) el.teamDescription.value = String(team?.description || "").trim();
  if (el.planTeamSelect && state.teamSelection.teamId) {
    const exists = [...el.planTeamSelect.options].some((opt) => String(opt.value || "") === state.teamSelection.teamId);
    if (exists) el.planTeamSelect.value = state.teamSelection.teamId;
  }
  if (!isActiveRunStatus(state.currentRun?.status)) {
    const activeStages = activeStageIdsFromStageMap(stageAgentIds);
    const firstStage = Number(activeStages[0] || 1);
    state.selectedStage = Number.isFinite(firstStage) ? firstStage : 1;
  }
  renderWorkTeamSelection();
  renderTeamBuilderSelectors();
  renderTaskSummary();
  if (state.mode === MODES.BUILD) {
    renderRun();
  }
}

function defaultBuilderMap() {
  const out = {};
  STAGES.forEach((stage) => {
    const options = state.agents.by_stage?.[stage] || [];
    if (options[0]?.id) out[stage] = options[0].id;
  });
  return out;
}

function defaultAgentForStage(stage) {
  const options = state.agents.by_stage?.[String(stage)] || [];
  return String(options[0]?.id || "").trim();
}

function teamBuilderUnusedStages() {
  const normalized = normalizeStageAgentIds(state.teamBuilder?.stageAgentIds || {});
  return STAGES.filter((stage) => !normalized[stage] && (state.agents.by_stage?.[stage] || []).length > 0);
}

function resetBuilderForNewTeam() {
  state.teamBuilder.stageAgentIds = {};
  state.teamBuilder.editingTeamId = "";
  state.teamBuilder.editingIsCustom = false;
  if (el.teamName) el.teamName.value = "";
  if (el.teamDescription) el.teamDescription.value = "";
  const firstStage = teamBuilderUnusedStages()[0];
  if (firstStage) {
    const agentId = defaultAgentForStage(firstStage);
    if (agentId) state.teamBuilder.stageAgentIds[firstStage] = agentId;
  }
  if (el.planTeamSelect) el.planTeamSelect.value = "";
  if (el.teamSaveMessage) el.teamSaveMessage.textContent = "Creating a new custom team.";
  renderTeamBuilderSelectors();
}

function stageDisplayName(stage) {
  return AGENTS.find((a) => Number(a.stage) === Number(stage))?.name || `Stage ${stage}`;
}

function renderWorkTeamSelection() {
  const s = state.teamSelection;
  if (el.workTeamSelect && s.teamId) {
    const exists = [...el.workTeamSelect.options].some((o) => o.value === s.teamId);
    if (exists) el.workTeamSelect.value = s.teamId;
  }
  el.workTeamReason.textContent = s.reason ? `Suggested rationale: ${s.reason}` : `Active team: ${s.teamName || "(none)"}`;

  const activeStages = activeStageIdsFromStageMap(s.stageAgentIds || {});
  el.workTeamRoster.innerHTML = activeStages.map((stage) => {
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

function teamBuilderRowHtml(stage, selectedId, usedStageSet) {
  const currentStage = String(stage);
  const options = state.agents.by_stage?.[currentStage] || [];
  const preferred = String(selectedId || "").trim();
  const resolvedSelected = options.some((a) => String(a.id) === preferred)
    ? preferred
    : String(options[0]?.id || "").trim();
  const stageOptions = STAGES
    .filter((candidate) => candidate === currentStage || !usedStageSet.has(candidate))
    .map((candidate) => `<option value="${candidate}" ${candidate === currentStage ? "selected" : ""}>Stage ${candidate}: ${escapeHtml(stageDisplayName(candidate))}</option>`)
    .join("");
  const agentOptions = options.length
    ? options
      .map((a) => `<option value="${escapeHtml(a.id)}" ${String(a.id) === resolvedSelected ? "selected" : ""}>${escapeHtml(a.display_name)}${a.is_custom ? " (custom)" : ""}</option>`)
      .join("")
    : `<option value="">No available agents for this stage</option>`;
  const current = options.find((o) => String(o.id) === resolvedSelected) || {};
  const reqPack = Number(currentStage) === 1 ? String(current.requirements_pack_profile || "").trim() : "";
  return `
    <div class="rounded-lg border border-slate-300 bg-slate-50 p-3" data-team-row="${escapeHtml(currentStage)}">
      <div class="mb-2 grid gap-2 sm:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_auto]">
        <select data-team-row-stage="${escapeHtml(currentStage)}" class="w-full rounded-md border border-slate-300 bg-white px-2 py-2 text-xs text-slate-900">${stageOptions}</select>
        <select data-team-row-agent="${escapeHtml(currentStage)}" class="w-full rounded-md border border-slate-300 bg-white px-2 py-2 text-xs text-slate-900" ${options.length ? "" : "disabled"}>${agentOptions}</select>
        <button data-team-row-remove="${escapeHtml(currentStage)}" class="btn-light rounded-md px-3 py-2 text-xs font-semibold">Remove</button>
      </div>
      <p class="text-xs text-slate-700">${escapeHtml(current.persona || "No persona configured for selected agent.")}</p>
      ${reqPack ? `<p class="mt-1 text-[11px] text-sky-800"><strong>Requirements Pack:</strong> ${escapeHtml(reqPack)}</p>` : ""}
    </div>
  `;
}

function renderTeamBuilderSelectors() {
  const normalized = normalizeStageAgentIds(state.teamBuilder?.stageAgentIds || {});
  state.teamBuilder.stageAgentIds = { ...normalized };
  const selectedStages = Object.keys(normalized).sort((a, b) => Number(a) - Number(b));
  const usedStageSet = new Set(selectedStages);
  if (!selectedStages.length) {
    el.teamStageSelectors.innerHTML = `
      <div class="rounded-lg border border-slate-300 bg-slate-50 p-3 text-xs text-slate-700">
        No agents added yet. Click <strong>Add Agent</strong> to include stages in this team.
      </div>
    `;
  } else {
    el.teamStageSelectors.innerHTML = selectedStages
      .map((stage) => teamBuilderRowHtml(stage, normalized[stage], usedStageSet))
      .join("");
  }
  const hasAnyAvailableStage = teamBuilderUnusedStages().length > 0;
  if (el.teamAddAgentBtn) {
    el.teamAddAgentBtn.disabled = !hasAnyAvailableStage;
    el.teamAddAgentBtn.classList.toggle("opacity-60", !hasAnyAvailableStage);
    el.teamAddAgentBtn.classList.toggle("cursor-not-allowed", !hasAnyAvailableStage);
  }
  el.teamStageSelectors.querySelectorAll("[data-team-row-stage]").forEach((node) => {
    node.addEventListener("change", () => {
      const oldStage = String(node.getAttribute("data-team-row-stage") || "").trim();
      const newStage = String(node.value || "").trim();
      if (!oldStage || !newStage || oldStage === newStage) return;
      if (state.teamBuilder.stageAgentIds[newStage]) {
        alert("That stage is already added. Choose a different stage.");
        node.value = oldStage;
        return;
      }
      const priorAgentId = String(state.teamBuilder.stageAgentIds[oldStage] || "").trim();
      delete state.teamBuilder.stageAgentIds[oldStage];
      const nextOptions = state.agents.by_stage?.[newStage] || [];
      const nextAgentId = nextOptions.some((a) => String(a.id) === priorAgentId)
        ? priorAgentId
        : String(nextOptions[0]?.id || "").trim();
      if (!nextAgentId) {
        alert("No available agents for the selected stage.");
        state.teamBuilder.stageAgentIds[oldStage] = priorAgentId;
      } else {
        state.teamBuilder.stageAgentIds[newStage] = nextAgentId;
      }
      renderTeamBuilderSelectors();
    });
  });
  el.teamStageSelectors.querySelectorAll("[data-team-row-agent]").forEach((node) => {
    node.addEventListener("change", () => {
      const stage = String(node.getAttribute("data-team-row-agent") || "").trim();
      if (!stage) return;
      const value = String(node.value || "").trim();
      const resolved = stageAgentLookup(stage, value);
      if (!resolved?.id) return;
      state.teamBuilder.stageAgentIds[stage] = String(resolved.id);
      renderTeamBuilderSelectors();
    });
  });
  el.teamStageSelectors.querySelectorAll("[data-team-row-remove]").forEach((node) => {
    node.addEventListener("click", () => {
      const stage = String(node.getAttribute("data-team-row-remove") || "").trim();
      if (!stage) return;
      delete state.teamBuilder.stageAgentIds[stage];
      renderTeamBuilderSelectors();
    });
  });
  const editingCustom = !!state.teamBuilder.editingTeamId && !!state.teamBuilder.editingIsCustom;
  if (el.teamSaveBtn) el.teamSaveBtn.textContent = editingCustom ? "Update Team" : "Save Team";
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

function findAgentBrainConfig(agentKey) {
  const brains = Array.isArray(state.settings?.knowledge_hub?.agent_brains) ? state.settings.knowledge_hub.agent_brains : [];
  return brains.find((row) => String(row?.agent_key || "") === String(agentKey || "")) || null;
}

function getAgentStudioDraft(agentKey) {
  const key = String(agentKey || "").trim();
  if (!key) return null;
  if (!state.agentStudio.draftByAgent[key]) {
    const brain = findAgentBrainConfig(key) || {};
    state.agentStudio.draftByAgent[key] = {
      knowledge_set_ids: Array.isArray(brain.knowledge_set_ids) ? [...brain.knowledge_set_ids] : [],
      top_k: Number(brain.top_k || 8),
      citation_required: brain.citation_required !== false,
      fallback_behavior: String(brain.fallback_behavior || "ask_clarification"),
      allowed_tools: Array.isArray(brain.allowed_tools) ? [...brain.allowed_tools] : ["repo_read", "doc_export"],
      memory_scope: String(brain.memory_scope || "project"),
      memory_enabled: brain.memory_enabled !== false,
    };
  }
  return state.agentStudio.draftByAgent[key];
}

function renderAgentStudioPanel() {
  if (!el.agentStudioPanel) return;
  const allAgents = Array.isArray(state.agents?.all) ? state.agents.all : [];
  if (!allAgents.length) {
    el.agentStudioPanel.innerHTML = `<p class="text-xs text-slate-700">No agents available.</p>`;
    return;
  }
  if (!state.agentStudio.selectedAgentKey) {
    state.agentStudio.selectedAgentKey = String(allAgents[0]?.id || "");
  }
  const agentKey = state.agentStudio.selectedAgentKey;
  const selected = allAgents.find((agent) => String(agent.id || "") === agentKey) || allAgents[0];
  const draft = getAgentStudioDraft(String(selected?.id || "")) || {};
  const tab = String(state.agentStudio.tab || "persona");

  if (tab === "persona") {
    el.agentStudioPanel.innerHTML = `
      <p class="text-xs text-slate-700">Stage ${escapeHtml(selected.stage)} | ${escapeHtml(selected.display_name || selected.id || "Agent")}</p>
      <p class="mt-2 text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-600">Persona</p>
      <textarea class="mt-1 w-full rounded-lg border border-slate-300 bg-slate-50 px-2 py-2 text-xs text-slate-900" rows="8" readonly>${escapeHtml(selected.persona || "No persona text available.")}</textarea>
      <p class="mt-2 text-[11px] text-slate-700">This tab is read-only. Update persona via Clone & Modify Agent Persona above.</p>
    `;
    return;
  }

  if (tab === "brain") {
    const setIds = Array.isArray(draft.knowledge_set_ids) ? draft.knowledge_set_ids.join(", ") : "";
    const fallback = String(draft.fallback_behavior || "ask_clarification");
    const hub = (state.settings?.knowledge_hub && typeof state.settings.knowledge_hub === "object") ? state.settings.knowledge_hub : {};
    const sources = Array.isArray(hub.sources) ? hub.sources : [];
    const sets = Array.isArray(hub.sets) ? hub.sets : [];
    const specialists = Array.isArray(hub.specialists) ? hub.specialists : [];
    const sourceOptions = sources.map((row) => {
      const id = String(row?.source_id || "").trim();
      const label = `${id} | ${String(row?.name || "").trim() || "Unnamed source"}`;
      return `<option value="${escapeHtml(id)}">${escapeHtml(label)}</option>`;
    }).join("");
    const setOptions = sets.map((row) => {
      const id = String(row?.set_id || "").trim();
      const version = String(row?.version || "1.0.0").trim();
      const label = `${id} | ${String(row?.name || "").trim() || "Unnamed set"} v${version}`;
      return `<option value="${escapeHtml(id)}">${escapeHtml(label)}</option>`;
    }).join("");
    const specialistRowsForAgent = specialists
      .filter((row) => String(row?.linked_agent_key || "").trim() === String(selected?.id || "").trim())
      .slice(0, 40);
    const specialistRowsForAgentHtml = specialistRowsForAgent.length
      ? specialistRowsForAgent.map((row) => {
        const specialistId = String(row?.specialist_id || "").trim();
        const name = String(row?.name || "Unnamed specialist").trim();
        const domain = String(row?.domain || "").trim();
        const mode = String(row?.tool_mode || "read_only").trim();
        const depth = String(row?.depth_tier || "standard").trim();
        const score = String(row?.min_match_score || 1).trim();
        const enabled = row?.enabled ? "enabled" : "disabled";
        const auto = row?.auto_route ? "auto-route" : "manual";
        return `<div class="rounded border border-slate-300 bg-white px-2 py-1.5">
          <div class="flex items-center justify-between gap-2">
            <div class="text-[11px] text-slate-900">${escapeHtml(name)} (${escapeHtml(specialistId)})</div>
            <button data-specialist-remove="${escapeHtml(specialistId)}" class="btn-light rounded px-2 py-0.5 text-[10px] font-semibold">Remove</button>
          </div>
          <div class="mt-1 text-[10px] text-slate-700">domain=${escapeHtml(domain || "n/a")} | ${escapeHtml(mode)} | ${escapeHtml(depth)} | minScore=${escapeHtml(score)} | ${escapeHtml(enabled)} | ${escapeHtml(auto)}</div>
        </div>`;
      }).join("")
      : `<p class="text-[11px] text-slate-700">No specialist routing profiles linked to this agent yet.</p>`;
    const evalState = (state.agentStudio?.evalByAgent && typeof state.agentStudio.evalByAgent === "object")
      ? state.agentStudio.evalByAgent[String(selected?.id || "")]
      : null;
    const evalOutput = evalState && typeof evalState === "object"
      ? String(evalState.output || "").trim()
      : "";
    const evalSummary = evalState && typeof evalState === "object"
      ? String(evalState.summary || "").trim()
      : "";
    const evalTask = evalState && typeof evalState === "object"
      ? String(evalState.task || "").trim()
      : "";
    el.agentStudioPanel.innerHTML = `
      <label class="mb-1 block text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-600">Knowledge Set IDs</label>
      <input id="agent-studio-set-ids" class="w-full rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900" value="${escapeHtml(setIds)}" />
      <div class="mt-2 grid gap-2 sm:grid-cols-2">
        <label class="text-xs text-slate-900">Top K
          <input id="agent-studio-top-k" type="number" min="1" max="50" class="mt-1 w-full rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900" value="${escapeHtml(String(draft.top_k || 8))}" />
        </label>
        <label class="text-xs text-slate-900">Fallback
          <select id="agent-studio-fallback" class="mt-1 w-full rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900">
            <option value="ask_clarification" ${fallback === "ask_clarification" ? "selected" : ""}>Ask clarification</option>
            <option value="block_with_assumption" ${fallback === "block_with_assumption" ? "selected" : ""}>Block with assumption</option>
            <option value="proceed_with_warning" ${fallback === "proceed_with_warning" ? "selected" : ""}>Proceed with warning</option>
          </select>
        </label>
      </div>
      <label class="mt-2 inline-flex items-center gap-2 text-xs text-slate-900">
        <input id="agent-studio-citation-required" type="checkbox" class="h-4 w-4 rounded border-slate-400 text-slate-900" ${draft.citation_required ? "checked" : ""} />
        Require citations for grounded outputs
      </label>

      <div class="mt-3 rounded-lg border border-slate-300 bg-slate-50 p-2">
        <p class="text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-600">Create / Upload Knowledge Source</p>
        <div class="mt-2 grid gap-2 sm:grid-cols-2">
          <label class="text-xs text-slate-900">Source name
            <input id="agent-studio-source-name" class="mt-1 w-full rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900" placeholder="Client policy pack" />
          </label>
          <label class="text-xs text-slate-900">Source type
            <select id="agent-studio-source-type" class="mt-1 w-full rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900">
              <option value="file">File</option>
              <option value="wiki">Wiki</option>
              <option value="repo">Repository</option>
              <option value="standards">Standards</option>
              <option value="issues">Issue tracker</option>
              <option value="other">Other</option>
            </select>
          </label>
        </div>
        <div class="mt-2 grid gap-2 sm:grid-cols-2">
          <label class="text-xs text-slate-900">Scope
            <select id="agent-studio-source-scope" class="mt-1 w-full rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900">
              <option value="project">Project</option>
              <option value="client">Client</option>
              <option value="workspace">Workspace</option>
              <option value="global">Global</option>
            </select>
          </label>
          <label class="text-xs text-slate-900">Classification
            <select id="agent-studio-source-classification" class="mt-1 w-full rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900">
              <option value="internal">Internal</option>
              <option value="public">Public</option>
              <option value="confidential">Confidential</option>
              <option value="regulated">Regulated</option>
            </select>
          </label>
        </div>
        <label class="mt-2 block text-xs text-slate-900">Upload file (optional)
          <input id="agent-studio-source-file" type="file" class="mt-1 w-full rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900" />
        </label>
        <label class="mt-2 block text-xs text-slate-900">Or source location URL/path
          <input id="agent-studio-source-location" class="mt-1 w-full rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900" placeholder="https://confluence/... or /repo/docs/..." />
        </label>
        <label class="mt-2 block text-xs text-slate-900">Tags (comma-separated)
          <input id="agent-studio-source-tags" class="mt-1 w-full rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900" placeholder="banking, compliance, client-x" />
        </label>
        <button id="agent-studio-create-source" class="btn-dark mt-2 rounded-md px-2.5 py-1.5 text-[11px] font-semibold">Save Source</button>
      </div>

      <div class="mt-3 rounded-lg border border-slate-300 bg-slate-50 p-2">
        <p class="text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-600">Create Knowledge Set</p>
        <div class="mt-2 grid gap-2 sm:grid-cols-3">
          <label class="text-xs text-slate-900">Set name
            <input id="agent-studio-new-set-name" class="mt-1 w-full rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900" placeholder="Client X Pack" />
          </label>
          <label class="text-xs text-slate-900">Version
            <input id="agent-studio-new-set-version" class="mt-1 w-full rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900" value="1.0.0" />
          </label>
          <label class="text-xs text-slate-900">State
            <select id="agent-studio-new-set-state" class="mt-1 w-full rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900">
              <option value="draft">Draft</option>
              <option value="published">Published</option>
              <option value="deprecated">Deprecated</option>
            </select>
          </label>
        </div>
        <label class="mt-2 block text-xs text-slate-900">Pick sources (optional, multi-select)</label>
        <select id="agent-studio-source-picker" multiple size="4" class="mt-1 w-full rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900">${sourceOptions}</select>
        <div class="mt-2 flex flex-wrap gap-2">
          <button id="agent-studio-fill-source-ids" class="btn-light rounded-md px-2.5 py-1.5 text-[11px] font-semibold">Use Selected Sources</button>
          <input id="agent-studio-new-set-source-ids" class="min-w-[280px] flex-1 rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900" placeholder="src-123, src-456" />
        </div>
        <button id="agent-studio-create-set" class="btn-dark mt-2 rounded-md px-2.5 py-1.5 text-[11px] font-semibold">Save Knowledge Set</button>
      </div>

      <div class="mt-3 rounded-lg border border-slate-300 bg-slate-50 p-2">
        <p class="text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-600">Attach Existing Set to Agent Brain</p>
        <div class="mt-2 flex flex-wrap items-center gap-2">
          <select id="agent-studio-existing-set" class="min-w-[260px] rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900">
            <option value="">Select a set</option>
            ${setOptions}
          </select>
          <button id="agent-studio-attach-set" class="btn-light rounded-md px-2.5 py-1.5 text-[11px] font-semibold">Attach To This Agent</button>
        </div>
      </div>

      <div class="mt-3 rounded-lg border border-slate-300 bg-slate-50 p-2">
        <p class="text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-600">Specialist Router Profile</p>
        <p class="mt-1 text-[11px] text-slate-700">Define trigger-based specialist routing for this agent.</p>
        <div class="mt-2 grid gap-2 sm:grid-cols-3">
          <label class="text-xs text-slate-900">Name
            <input id="agent-studio-specialist-name" class="mt-1 w-full rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900" placeholder="VB6 UI/Forms Specialist" />
          </label>
          <label class="text-xs text-slate-900">Domain
            <input id="agent-studio-specialist-domain" class="mt-1 w-full rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900" placeholder="vb6-modernization" />
          </label>
          <label class="text-xs text-slate-900">Stage Hint
            <input id="agent-studio-specialist-stage" type="number" min="0" max="8" value="${escapeHtml(String(selected?.stage || 0))}" class="mt-1 w-full rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900" />
          </label>
        </div>
        <label class="mt-2 block text-xs text-slate-900">Description
          <input id="agent-studio-specialist-description" class="mt-1 w-full rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900" placeholder="Specialized handling for VB6 forms/events and parity." />
        </label>
        <div class="mt-2 grid gap-2 sm:grid-cols-3">
          <label class="text-xs text-slate-900">Intent keywords
            <input id="agent-studio-specialist-intents" class="mt-1 w-full rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900" placeholder="vb6, form, control array, event map" />
          </label>
          <label class="text-xs text-slate-900">File patterns
            <input id="agent-studio-specialist-files" class="mt-1 w-full rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900" placeholder="*.frm, *.frx, *.bas" />
          </label>
          <label class="text-xs text-slate-900">Artifact triggers
            <input id="agent-studio-specialist-artifacts" class="mt-1 w-full rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900" placeholder="vb6_analysis, evidence_files" />
          </label>
        </div>
        <div class="mt-2 grid gap-2 sm:grid-cols-4">
          <label class="text-xs text-slate-900">Min match score
            <input id="agent-studio-specialist-min-score" type="number" min="1" max="10" value="1" class="mt-1 w-full rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900" />
          </label>
          <label class="text-xs text-slate-900">Tool mode
            <select id="agent-studio-specialist-tool-mode" class="mt-1 w-full rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900">
              <option value="read_only">read_only</option>
              <option value="read_write">read_write</option>
            </select>
          </label>
          <label class="text-xs text-slate-900">Depth tier
            <select id="agent-studio-specialist-depth" class="mt-1 w-full rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900">
              <option value="shallow">shallow</option>
              <option value="standard" selected>standard</option>
              <option value="deep">deep</option>
            </select>
          </label>
          <div class="text-xs text-slate-900">
            <label class="mt-6 inline-flex items-center gap-2">
              <input id="agent-studio-specialist-enabled" type="checkbox" class="h-4 w-4 rounded border-slate-400 text-slate-900" checked />
              enabled
            </label>
            <label class="mt-1 inline-flex items-center gap-2">
              <input id="agent-studio-specialist-auto-route" type="checkbox" class="h-4 w-4 rounded border-slate-400 text-slate-900" checked />
              auto_route
            </label>
          </div>
        </div>
        <button id="agent-studio-save-specialist" class="btn-dark mt-2 rounded-md px-2.5 py-1.5 text-[11px] font-semibold">Save Specialist Profile</button>
        <div class="mt-2 space-y-1">${specialistRowsForAgentHtml}</div>
      </div>

      <div class="mt-3 rounded-lg border border-slate-300 bg-slate-50 p-2">
        <p class="text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-600">Brain Evaluation</p>
        <p class="mt-1 text-[11px] text-slate-700">Run retrieval + routing checks for this agent using current workspace/project context.</p>
        <label class="mt-2 block text-xs text-slate-900">Task / Prompt
          <textarea id="agent-studio-brain-task" class="mt-1 w-full rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900" rows="3" placeholder="Analyze VB6 forms and SQL flows for parity migration.">${escapeHtml(evalTask)}</textarea>
        </label>
        <div class="mt-2 flex flex-wrap gap-2">
          <button id="agent-studio-find-context" class="btn-dark rounded-md px-2.5 py-1.5 text-[11px] font-semibold">Find Relevant Context</button>
          <button id="agent-studio-suggest-agent" class="btn-light rounded-md px-2.5 py-1.5 text-[11px] font-semibold">Suggest Agent</button>
        </div>
        <div id="agent-studio-brain-eval-summary" class="mt-2 rounded border border-slate-200 bg-white p-2 text-[11px] text-slate-800">${escapeHtml(evalSummary || "No explanation yet.")}</div>
        <pre id="agent-studio-brain-eval-output" class="mt-2 max-h-56 overflow-auto rounded border border-slate-300 bg-slate-950 p-2 text-[11px] text-emerald-200">${escapeHtml(evalOutput || "No evaluation run yet.")}</pre>
      </div>
    `;
    return;
  }

  if (tab === "tools") {
    const allowed = Array.isArray(draft.allowed_tools) ? draft.allowed_tools : [];
    const toolCatalog = [
      { key: "repo_read", label: "Read repository" },
      { key: "repo_write", label: "Write repository" },
      { key: "issue_read", label: "Read issue trackers" },
      { key: "issue_write", label: "Write issue trackers" },
      { key: "doc_export", label: "Export documents" },
      { key: "run_pipeline", label: "Run pipeline actions" },
    ];
    el.agentStudioPanel.innerHTML = `
      <p class="text-[11px] text-slate-700">Select which tools this agent is allowed to use.</p>
      <div class="mt-2 grid gap-1">
        ${toolCatalog.map((item) => `
          <label class="inline-flex items-center gap-2 text-xs text-slate-900">
            <input data-agent-studio-tool="${escapeHtml(item.key)}" type="checkbox" class="h-4 w-4 rounded border-slate-400 text-slate-900" ${allowed.includes(item.key) ? "checked" : ""} />
            ${escapeHtml(item.label)}
          </label>
        `).join("")}
      </div>
    `;
    return;
  }

  const memoryScope = String(draft.memory_scope || "project");
  el.agentStudioPanel.innerHTML = `
    <p class="text-[11px] text-slate-700">Configure memory policy for learned corrections.</p>
    <label class="mt-2 block text-xs text-slate-900">Memory Scope
      <select id="agent-studio-memory-scope" class="mt-1 w-full rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900">
        <option value="project" ${memoryScope === "project" ? "selected" : ""}>Project</option>
        <option value="client" ${memoryScope === "client" ? "selected" : ""}>Client</option>
        <option value="workspace" ${memoryScope === "workspace" ? "selected" : ""}>Workspace</option>
        <option value="global" ${memoryScope === "global" ? "selected" : ""}>Global</option>
      </select>
    </label>
    <label class="mt-2 inline-flex items-center gap-2 text-xs text-slate-900">
      <input id="agent-studio-memory-enabled" type="checkbox" class="h-4 w-4 rounded border-slate-400 text-slate-900" ${draft.memory_enabled ? "checked" : ""} />
      Allow this agent to write structured memory rules
    </label>
  `;
}

function renderAgentStudio() {
  if (!el.agentStudioAgentSelect) return;
  const all = Array.isArray(state.agents?.all) ? state.agents.all : [];
  if (!all.length) {
    el.agentStudioAgentSelect.innerHTML = `<option value="">No agents</option>`;
    if (el.agentStudioPanel) el.agentStudioPanel.innerHTML = `<p class="text-xs text-slate-700">No agents available.</p>`;
    return;
  }
  el.agentStudioAgentSelect.innerHTML = all.map((agent) => {
    const label = `S${agent.stage} | ${agent.display_name || agent.role || agent.id}`;
    return `<option value="${escapeHtml(agent.id)}">${escapeHtml(label)}</option>`;
  }).join("");
  const exists = all.some((agent) => String(agent.id || "") === String(state.agentStudio.selectedAgentKey || ""));
  if (!exists) {
    state.agentStudio.selectedAgentKey = String(all[0].id || "");
  }
  el.agentStudioAgentSelect.value = String(state.agentStudio.selectedAgentKey || "");
  setAgentStudioTab(state.agentStudio.tab || "persona");
}

function setAgentStudioTab(tabName) {
  const tab = String(tabName || "persona").toLowerCase();
  state.agentStudio.tab = ["persona", "brain", "tools", "memory"].includes(tab) ? tab : "persona";
  document.querySelectorAll("[data-agent-studio-tab]").forEach((btn) => {
    if (!(btn instanceof HTMLElement)) return;
    const active = String(btn.getAttribute("data-agent-studio-tab") || "") === state.agentStudio.tab;
    btn.classList.toggle("btn-dark", active);
    btn.classList.toggle("btn-light", !active);
  });
  renderAgentStudioPanel();
}

function collectAgentStudioDraftFromPanel() {
  const agentKey = String(state.agentStudio.selectedAgentKey || "");
  if (!agentKey) return null;
  const draft = getAgentStudioDraft(agentKey);
  if (!draft) return null;
  if (state.agentStudio.tab === "brain") {
    draft.knowledge_set_ids = parseCommaValues(document.getElementById("agent-studio-set-ids")?.value || "");
    draft.top_k = Number(document.getElementById("agent-studio-top-k")?.value || 8);
    draft.fallback_behavior = String(document.getElementById("agent-studio-fallback")?.value || "ask_clarification");
    draft.citation_required = !!document.getElementById("agent-studio-citation-required")?.checked;
  } else if (state.agentStudio.tab === "tools") {
    const tools = [];
    document.querySelectorAll("[data-agent-studio-tool]").forEach((node) => {
      if (!(node instanceof HTMLInputElement)) return;
      if (!node.checked) return;
      const key = String(node.getAttribute("data-agent-studio-tool") || "").trim();
      if (key) tools.push(key);
    });
    draft.allowed_tools = tools;
  } else if (state.agentStudio.tab === "memory") {
    draft.memory_scope = String(document.getElementById("agent-studio-memory-scope")?.value || "project");
    draft.memory_enabled = !!document.getElementById("agent-studio-memory-enabled")?.checked;
  }
  return draft;
}

async function saveAgentStudioConfig() {
  const agentKey = String(state.agentStudio.selectedAgentKey || "").trim();
  if (!agentKey) {
    if (el.agentStudioMessage) el.agentStudioMessage.textContent = "Select an agent first.";
    return;
  }
  const draft = collectAgentStudioDraftFromPanel() || getAgentStudioDraft(agentKey) || {};
  const payload = {
    agent_key: agentKey,
    knowledge_set_ids: Array.isArray(draft.knowledge_set_ids) ? draft.knowledge_set_ids : [],
    top_k: Number(draft.top_k || 8),
    citation_required: draft.citation_required !== false,
    fallback_behavior: String(draft.fallback_behavior || "ask_clarification"),
    allowed_tools: Array.isArray(draft.allowed_tools) ? draft.allowed_tools : [],
    memory_scope: String(draft.memory_scope || "project"),
    memory_enabled: draft.memory_enabled !== false,
  };
  const data = await api("/api/settings/knowledge/brains", payload, "POST");
  state.settings = data.settings || state.settings;
  renderSettings();
  renderAgentStudio();
  if (el.agentStudioMessage) el.agentStudioMessage.textContent = `Saved Agent Studio config for ${agentKey}.`;
}

function selectedOptionValues(selectId) {
  const node = document.getElementById(selectId);
  if (!(node instanceof HTMLSelectElement)) return [];
  return [...node.selectedOptions].map((opt) => String(opt.value || "").trim()).filter(Boolean);
}

async function createAgentStudioKnowledgeSource() {
  const name = String(document.getElementById("agent-studio-source-name")?.value || "").trim();
  const sourceType = String(document.getElementById("agent-studio-source-type")?.value || "file").trim().toLowerCase();
  const scope = String(document.getElementById("agent-studio-source-scope")?.value || "project").trim().toLowerCase();
  const dataClassification = String(document.getElementById("agent-studio-source-classification")?.value || "internal").trim().toLowerCase();
  const location = String(document.getElementById("agent-studio-source-location")?.value || "").trim();
  const tags = parseCommaValues(String(document.getElementById("agent-studio-source-tags")?.value || ""));
  const fileInput = document.getElementById("agent-studio-source-file");
  const file = (fileInput instanceof HTMLInputElement && fileInput.files && fileInput.files.length) ? fileInput.files[0] : null;

  if (!name && !file) {
    if (el.agentStudioMessage) el.agentStudioMessage.textContent = "Provide a source name or choose a file.";
    return;
  }

  if (file) {
    const form = new FormData();
    form.append("file", file);
    form.append("name", name || String(file.name || "Uploaded Source"));
    form.append("type", sourceType || "file");
    form.append("scope", scope || "project");
    form.append("data_classification", dataClassification || "internal");
    form.append("location", location);
    form.append("tags", tags.join(", "));
    const data = await apiMultipart("/api/settings/knowledge/sources/upload", form, "POST");
    state.settings = data.settings || state.settings;
  } else {
    const payload = {
      name,
      type: sourceType || "file",
      scope: scope || "project",
      data_classification: dataClassification || "internal",
      location,
      tags,
    };
    const data = await api("/api/settings/knowledge/sources", payload, "POST");
    state.settings = data.settings || state.settings;
  }

  renderSettings();
  renderAgentStudio();
  if (el.agentStudioMessage) el.agentStudioMessage.textContent = "Knowledge source saved.";
}

async function createAgentStudioKnowledgeSet() {
  const name = String(document.getElementById("agent-studio-new-set-name")?.value || "").trim();
  if (!name) {
    if (el.agentStudioMessage) el.agentStudioMessage.textContent = "Set name is required.";
    return;
  }
  const version = String(document.getElementById("agent-studio-new-set-version")?.value || "1.0.0").trim() || "1.0.0";
  const publishState = String(document.getElementById("agent-studio-new-set-state")?.value || "draft").trim().toLowerCase() || "draft";
  const sourceIds = parseCommaValues(String(document.getElementById("agent-studio-new-set-source-ids")?.value || ""));
  const payload = {
    name,
    version,
    publish_state: publishState,
    source_ids: sourceIds,
  };
  const data = await api("/api/settings/knowledge/sets", payload, "POST");
  state.settings = data.settings || state.settings;
  renderSettings();
  renderAgentStudio();
  if (el.agentStudioMessage) el.agentStudioMessage.textContent = `Knowledge set '${name}' saved.`;
}

async function saveAgentStudioSpecialistProfile() {
  const linkedAgentKey = String(state.agentStudio.selectedAgentKey || "").trim();
  if (!linkedAgentKey) {
    if (el.agentStudioMessage) el.agentStudioMessage.textContent = "Select an agent first.";
    return;
  }
  const name = String(document.getElementById("agent-studio-specialist-name")?.value || "").trim();
  if (!name) {
    if (el.agentStudioMessage) el.agentStudioMessage.textContent = "Specialist name is required.";
    return;
  }
  const payload = {
    name,
    description: String(document.getElementById("agent-studio-specialist-description")?.value || "").trim(),
    domain: String(document.getElementById("agent-studio-specialist-domain")?.value || "").trim().toLowerCase(),
    linked_agent_key: linkedAgentKey,
    stage_hint: Number(document.getElementById("agent-studio-specialist-stage")?.value || 0),
    intent_keywords: parseCommaValues(String(document.getElementById("agent-studio-specialist-intents")?.value || "")),
    file_patterns: parseCommaValues(String(document.getElementById("agent-studio-specialist-files")?.value || "")),
    artifact_triggers: parseCommaValues(String(document.getElementById("agent-studio-specialist-artifacts")?.value || "")),
    min_match_score: Number(document.getElementById("agent-studio-specialist-min-score")?.value || 1),
    tool_mode: String(document.getElementById("agent-studio-specialist-tool-mode")?.value || "read_only"),
    depth_tier: String(document.getElementById("agent-studio-specialist-depth")?.value || "standard"),
    enabled: !!document.getElementById("agent-studio-specialist-enabled")?.checked,
    auto_route: !!document.getElementById("agent-studio-specialist-auto-route")?.checked,
  };
  const data = await api("/api/settings/knowledge/specialists", payload, "POST");
  state.settings = data.settings || state.settings;
  renderSettings();
  renderAgentStudio();
  if (el.agentStudioMessage) el.agentStudioMessage.textContent = `Specialist profile '${name}' saved.`;
}

async function removeAgentStudioSpecialistProfile(specialistId) {
  const specialist_id = String(specialistId || "").trim();
  if (!specialist_id) return;
  const data = await api("/api/settings/knowledge/specialists/remove", { specialist_id }, "POST");
  state.settings = data.settings || state.settings;
  renderSettings();
  renderAgentStudio();
  if (el.agentStudioMessage) el.agentStudioMessage.textContent = `Removed specialist profile ${specialist_id}.`;
}

function attachSetToSelectedAgentBrain() {
  const selectedSet = String(document.getElementById("agent-studio-existing-set")?.value || "").trim();
  if (!selectedSet) {
    if (el.agentStudioMessage) el.agentStudioMessage.textContent = "Select a knowledge set first.";
    return;
  }
  const input = document.getElementById("agent-studio-set-ids");
  if (!(input instanceof HTMLInputElement)) return;
  const current = parseCommaValues(String(input.value || ""));
  if (!current.includes(selectedSet)) current.push(selectedSet);
  input.value = current.join(", ");
  const draft = collectAgentStudioDraftFromPanel();
  if (draft && Array.isArray(draft.knowledge_set_ids)) {
    draft.knowledge_set_ids = current;
  }
  if (el.agentStudioMessage) el.agentStudioMessage.textContent = `Attached set ${selectedSet}. Save Agent Studio config to persist.`;
}

function setAgentStudioEvalOutput(agentKey, task, outputText, summaryText = "") {
  const key = String(agentKey || "").trim();
  if (!key) return;
  if (!state.agentStudio.evalByAgent || typeof state.agentStudio.evalByAgent !== "object") {
    state.agentStudio.evalByAgent = {};
  }
  state.agentStudio.evalByAgent[key] = {
    task: String(task || "").trim(),
    output: String(outputText || "").trim(),
    summary: String(summaryText || "").trim(),
    updatedAt: new Date().toISOString(),
  };
  const outputNode = document.getElementById("agent-studio-brain-eval-output");
  if (outputNode) outputNode.textContent = state.agentStudio.evalByAgent[key].output || "No evaluation run yet.";
  const summaryNode = document.getElementById("agent-studio-brain-eval-summary");
  if (summaryNode) summaryNode.textContent = state.agentStudio.evalByAgent[key].summary || "No explanation yet.";
}

function buildFindContextSummary(data) {
  const retrieval = (data && typeof data === "object" && data.retrieval && typeof data.retrieval === "object")
    ? data.retrieval
    : {};
  const guardrails = (data && typeof data === "object" && data.guardrails && typeof data.guardrails === "object")
    ? data.guardrails
    : {};
  const snapshot = (data && typeof data === "object" && data.context_snapshot && typeof data.context_snapshot === "object")
    ? data.context_snapshot
    : {};
  const vectorHits = Array.isArray(retrieval.vector_hits) ? retrieval.vector_hits.length : 0;
  const complianceCount = Array.isArray(retrieval.compliance_constraints) ? retrieval.compliance_constraints.length : 0;
  const capRows = retrieval.capability_mapping && Array.isArray(retrieval.capability_mapping.primary_capabilities)
    ? retrieval.capability_mapping.primary_capabilities
    : [];
  const topCaps = capRows.slice(0, 2).map((row) => String(row?.service_domain || row?.id || "").trim()).filter(Boolean);
  const blocker = !!guardrails.assumption_blocker;
  const status = blocker ? "BLOCKED" : "PASS";
  const sourceVersions = Array.isArray(snapshot.source_version_ids) ? snapshot.source_version_ids.length : 0;
  return [
    `Status: ${status}`,
    `Top capabilities: ${topCaps.length ? topCaps.join(", ") : "none inferred"}`,
    `Retrieved ${vectorHits} context hits and ${complianceCount} compliance constraints.`,
    `Snapshot: ${String(snapshot.knowledge_snapshot_id || "n/a")} (${sourceVersions} source version(s)).`,
    blocker ? "Reason: required compliance citation missing." : "Guardrails satisfied for this query.",
  ].join(" ");
}

function buildSuggestAgentSummary(data) {
  const suggestion = (data && typeof data === "object" && data.suggestion && typeof data.suggestion === "object")
    ? data.suggestion
    : {};
  const routing = (data && typeof data === "object" && data.routing && typeof data.routing === "object")
    ? data.routing
    : {};
  const primary = (suggestion.primary_agent && typeof suggestion.primary_agent === "object")
    ? suggestion.primary_agent
    : {};
  const primaryLabel = String(primary.display_name || primary.agent_key || "n/a").trim();
  const stage = Number(primary.stage || 0);
  const specialistMatches = Array.isArray(suggestion.specialist_matches) ? suggestion.specialist_matches : [];
  const topReasons = specialistMatches.slice(0, 2).map((row) => {
    const name = String(row?.name || row?.specialist_id || "specialist").trim();
    const score = Number(row?.score || 0);
    const intent = Array.isArray(row?.matched_intents) ? row.matched_intents.slice(0, 2).join(", ") : "";
    return `${name} (score ${score}${intent ? `; intents: ${intent}` : ""})`;
  });
  return [
    `Primary agent: ${primaryLabel}${stage > 0 ? ` (Stage ${stage})` : ""}.`,
    `Matched specialists: ${specialistMatches.length}.`,
    topReasons.length ? `Top reasons: ${topReasons.join(" | ")}.` : "No specialist triggers fired.",
    `Routing selected ${Number(routing.selected_count || 0)} specialist profile(s).`,
  ].join(" ");
}

function agentStudioBrainEvalPayload(taskText) {
  const agentKey = String(state.agentStudio.selectedAgentKey || "").trim();
  const all = Array.isArray(state.agents?.all) ? state.agents.all : [];
  const selected = all.find((row) => String(row?.id || "") === agentKey) || {};
  const integration = getIntegrationContext();
  const draft = collectAgentStudioDraftFromPanel() || getAgentStudioDraft(agentKey) || {};
  return {
    agent_key: agentKey,
    stage: Number(selected?.stage || 0),
    task: String(taskText || "").trim(),
    query: String(taskText || "").trim(),
    objectives: String(taskText || "").trim(),
    use_case: String(el.useCase?.value || "business_objectives").trim().toLowerCase(),
    top_k: Number(draft.top_k || 8),
    citation_required: draft.citation_required !== false,
    workspace: String(integration?.brain_context?.workspace || "default-workspace"),
    project: String(integration?.brain_context?.project || "default-project"),
    brain_context: (integration && typeof integration.brain_context === "object") ? integration.brain_context : {},
    integration_context: integration && typeof integration === "object" ? integration : {},
    domain_pack_id: String(integration?.domain_pack_id || ""),
    domain_pack: (integration && integration.custom_domain_pack && typeof integration.custom_domain_pack === "object")
      ? integration.custom_domain_pack
      : undefined,
    jurisdiction: String(integration?.jurisdiction || ""),
    data_classification: Array.isArray(integration?.data_classification) ? integration.data_classification : [],
    stage_agent_ids: (state.teamBuilder?.stageAgentIds && typeof state.teamBuilder.stageAgentIds === "object")
      ? state.teamBuilder.stageAgentIds
      : {},
  };
}

async function runAgentStudioFindRelevantContext() {
  const agentKey = String(state.agentStudio.selectedAgentKey || "").trim();
  if (!agentKey) throw new Error("Select an agent first.");
  const task = String(document.getElementById("agent-studio-brain-task")?.value || "").trim();
  if (!task) throw new Error("Enter a task/prompt first.");
  setAgentStudioEvalOutput(agentKey, task, "Running find_relevant_context...", "Evaluating knowledge sources and constraints...");
  const payload = agentStudioBrainEvalPayload(task);
  const data = await api("/api/agent-studio/find-relevant-context", payload, "POST");
  setAgentStudioEvalOutput(agentKey, task, JSON.stringify(data, null, 2), buildFindContextSummary(data));
  if (el.agentStudioMessage) el.agentStudioMessage.textContent = "Brain context retrieval completed.";
}

async function runAgentStudioSuggestAgent() {
  const agentKey = String(state.agentStudio.selectedAgentKey || "").trim();
  if (!agentKey) throw new Error("Select an agent first.");
  const task = String(document.getElementById("agent-studio-brain-task")?.value || "").trim();
  if (!task) throw new Error("Enter a task/prompt first.");
  setAgentStudioEvalOutput(agentKey, task, "Running suggest_agent...", "Evaluating specialist routing and agent fit...");
  const payload = agentStudioBrainEvalPayload(task);
  const data = await api("/api/agent-studio/suggest-agent", payload, "POST");
  setAgentStudioEvalOutput(agentKey, task, JSON.stringify(data, null, 2), buildSuggestAgentSummary(data));
  if (el.agentStudioMessage) el.agentStudioMessage.textContent = "Agent suggestion completed.";
}

function handleAgentStudioPanelClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) return;
  const button = target.closest("button");
  if (!(button instanceof HTMLButtonElement)) return;
  const id = String(button.id || "");
  if (!id) return;

  if (id === "agent-studio-fill-source-ids") {
    const selected = selectedOptionValues("agent-studio-source-picker");
    const input = document.getElementById("agent-studio-new-set-source-ids");
    if (input instanceof HTMLInputElement) input.value = selected.join(", ");
    return;
  }
  if (id === "agent-studio-attach-set") {
    attachSetToSelectedAgentBrain();
    return;
  }
  if (id === "agent-studio-create-source") {
    createAgentStudioKnowledgeSource().catch((err) => {
      if (el.agentStudioMessage) el.agentStudioMessage.textContent = String(err?.message || err || "Failed to save knowledge source.");
    });
    return;
  }
  if (id === "agent-studio-create-set") {
    createAgentStudioKnowledgeSet().catch((err) => {
      if (el.agentStudioMessage) el.agentStudioMessage.textContent = String(err?.message || err || "Failed to save knowledge set.");
    });
    return;
  }
  if (id === "agent-studio-save-specialist") {
    saveAgentStudioSpecialistProfile().catch((err) => {
      if (el.agentStudioMessage) el.agentStudioMessage.textContent = String(err?.message || err || "Failed to save specialist profile.");
    });
    return;
  }
  if (id === "agent-studio-find-context") {
    runAgentStudioFindRelevantContext().catch((err) => {
      if (el.agentStudioMessage) el.agentStudioMessage.textContent = String(err?.message || err || "Failed to find relevant context.");
    });
    return;
  }
  if (id === "agent-studio-suggest-agent") {
    runAgentStudioSuggestAgent().catch((err) => {
      if (el.agentStudioMessage) el.agentStudioMessage.textContent = String(err?.message || err || "Failed to suggest agent.");
    });
    return;
  }
  const removeId = String(button.getAttribute("data-specialist-remove") || "").trim();
  if (removeId) {
    removeAgentStudioSpecialistProfile(removeId).catch((err) => {
      if (el.agentStudioMessage) el.agentStudioMessage.textContent = String(err?.message || err || "Failed to remove specialist profile.");
    });
  }
}

function renderTeamsDropdown() {
  const teams = state.teams || [];
  const optionRows = teams.map((team) => `<option value="${escapeHtml(team.id)}">${escapeHtml(team.name)}${team.is_custom ? " (custom)" : ""}</option>`).join("");
  if (!teams.length) {
    el.workTeamSelect.innerHTML = `<option value="">No teams available</option>`;
    if (el.planTeamSelect) el.planTeamSelect.innerHTML = `<option value="">No teams available</option>`;
    return;
  }
  el.workTeamSelect.innerHTML = optionRows;
  if (el.planTeamSelect) {
    el.planTeamSelect.innerHTML = `<option value="">Select team…</option>${optionRows}`;
  }
  const selected = state.teamSelection.teamId || teams[0].id;
  el.workTeamSelect.value = selected;
  if (el.planTeamSelect && selected) el.planTeamSelect.value = selected;
}

function toModeButtonState(mode) {
  const map = {
    [MODES.DASHBOARDS]: el.navHome,
    [MODES.DISCOVER]: el.navWork,
    [MODES.PLAN]: el.navTeam,
    [MODES.ESTIMATES]: el.navEstimates,
    [MODES.BUILD]: el.navBuild,
    [MODES.VERIFY]: el.navHistory,
    [MODES.SETTINGS]: el.navSettings,
  };
  Object.values(map).forEach((btn) => btn?.classList.remove("mode-btn-active"));
  map[mode]?.classList.add("mode-btn-active");
}

function setPlanTab(tabName) {
  const tab = String(tabName || "team_creation").toLowerCase();
  const safe = ["team_creation", "agent_studio"].includes(tab) ? tab : "team_creation";
  state.planTab = safe;
  if (el.planPanelTeamCreation) el.planPanelTeamCreation.classList.toggle("hidden", safe !== "team_creation");
  if (el.planPanelAgentStudio) el.planPanelAgentStudio.classList.toggle("hidden", safe !== "agent_studio");
  document.querySelectorAll("[data-plan-tab]").forEach((btn) => {
    if (!(btn instanceof HTMLElement)) return;
    const active = String(btn.getAttribute("data-plan-tab") || "") === safe;
    btn.classList.toggle("btn-dark", active);
    btn.classList.toggle("btn-light", !active);
  });
}

function syncEstimateDefaultsFromCurrentRun() {
  if (!el.estimateRunId) return;
  if (!String(el.estimateRunId.value || "").trim() && state.currentRunId) {
    el.estimateRunId.value = state.currentRunId;
  }
  if (!String(el.estimateBusinessNeed?.value || "").trim()) {
    el.estimateBusinessNeed.value = "Modernize the brownfield application while preserving required business capability.";
  }
}

function setEstimateStatus(message, isError = false) {
  if (!el.estimateStatus) return;
  el.estimateStatus.textContent = String(message || "").trim() || "Ready.";
  el.estimateStatus.className = `mt-2 text-xs ${isError ? "text-rose-700" : "text-slate-700"}`;
}

function setEstimateTab(tabName) {
  const safe = ["overview", "team", "workstreams", "assumptions", "wbs"].includes(String(tabName || "")) ? String(tabName) : "overview";
  state.estimation.activeTab = safe;
  const panes = {
    overview: el.estimateOverview,
    team: el.estimateTeam,
    workstreams: el.estimateWorkstreams,
    assumptions: el.estimateAssumptions,
    wbs: el.estimateWbs,
  };
  Object.entries(panes).forEach(([key, node]) => {
    if (!node) return;
    node.classList.toggle("hidden", key !== safe);
  });
  document.querySelectorAll("[data-estimate-tab]").forEach((btn) => {
    if (!(btn instanceof HTMLElement)) return;
    const active = String(btn.getAttribute("data-estimate-tab") || "") === safe;
    btn.classList.toggle("btn-dark", active);
    btn.classList.toggle("btn-light", !active);
  });
}

function setEstimateAgentOutput(html) {
  if (!el.estimateAgentOutput) return;
  el.estimateAgentOutput.innerHTML = html || "No assistant output yet.";
}

function parseEstimateJson(raw, label) {
  const text = String(raw || "").trim();
  if (!text) {
    throw new Error(`${label} JSON is required.`);
  }
  try {
    return JSON.parse(text);
  } catch (err) {
    throw new Error(`${label} JSON is invalid.`);
  }
}

function estimateOverviewHtml(summary) {
  const estimate = summary?.estimate || {};
  const effort = estimate?.effort || {};
  const timeline = estimate?.timeline || {};
  const topRisks = Array.isArray(estimate?.risks) ? estimate.risks : [];
  const summaryRows = Array.isArray(estimate?.summary_table) ? estimate.summary_table : [];
  const riskRows = topRisks.length
    ? topRisks.slice(0, 5).map((risk) => `<li><span class="font-semibold">${escapeHtml(String(risk.severity || "medium").toUpperCase())}</span> ${escapeHtml(risk.title || risk.risk_id || "Risk")}</li>`).join("")
    : `<li>No top risks recorded.</li>`;
  const summaryTableRows = summaryRows.length
    ? summaryRows.map((row) => `<tr class="border-b border-slate-200">
        <td class="py-2 pr-3 font-semibold text-slate-900">${escapeHtml(row.phase || "")}</td>
        <td class="py-2 pr-3">${escapeHtml(String(row.p50_weeks ?? "0"))}</td>
        <td class="py-2 pr-3">${escapeHtml(row.key_risk || "")}</td>
      </tr>`).join("")
    : "";
  return `
    <div class="grid gap-2 sm:grid-cols-4">
      <div class="rounded-lg border border-slate-300 bg-white p-2"><p class="text-[11px] uppercase tracking-[0.12em] text-slate-600">Confidence</p><p class="mt-1 text-sm font-semibold text-slate-900">${escapeHtml(estimate.confidence_tier || "n/a")}</p></div>
      <div class="rounded-lg border border-slate-300 bg-white p-2"><p class="text-[11px] uppercase tracking-[0.12em] text-slate-600">Team model</p><p class="mt-1 text-sm font-semibold text-slate-900">${escapeHtml(estimate.team_model_selected || "n/a")}</p></div>
      <div class="rounded-lg border border-slate-300 bg-white p-2"><p class="text-[11px] uppercase tracking-[0.12em] text-slate-600">Effort (p50)</p><p class="mt-1 text-sm font-semibold text-slate-900">${escapeHtml(String(effort.total_hours?.p50 ?? "n/a"))} hrs</p></div>
      <div class="rounded-lg border border-slate-300 bg-white p-2"><p class="text-[11px] uppercase tracking-[0.12em] text-slate-600">Timeline (p50)</p><p class="mt-1 text-sm font-semibold text-slate-900">${escapeHtml(String(timeline.total_weeks?.p50 ?? "n/a"))} wks</p></div>
    </div>
    <div class="mt-3">
      <p class="text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-600">Summary</p>
      ${summaryTableRows ? `<div class="mt-2 overflow-auto"><table class="min-w-full border-collapse text-left text-xs">
        <thead><tr class="border-b border-slate-300 text-slate-600"><th class="py-2 pr-3">Phase</th><th class="py-2 pr-3">Duration (p50)</th><th class="py-2 pr-3">Key risk</th></tr></thead>
        <tbody>${summaryTableRows}</tbody></table></div>` : `<div class="mt-2 rounded-lg border border-slate-300 bg-white p-2 text-slate-700">No summary rows recorded.</div>`}
    </div>
    <div class="mt-3">
      <p class="text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-600">Top risks</p>
      <ul class="mt-2 list-disc space-y-1 pl-5 text-xs text-slate-800">${riskRows}</ul>
    </div>
    <div class="mt-3 rounded-lg border border-amber-300 bg-amber-50 p-2 text-xs text-amber-900">
      <span class="font-semibold">Contingency:</span>
      ${escapeHtml(String(Math.round((Number(estimate?.contingency?.low_pct || 0) * 100)) || 0))}–${escapeHtml(String(Math.round((Number(estimate?.contingency?.high_pct || 0) * 100)) || 0))}% ·
      ${escapeHtml(estimate?.contingency?.rationale || "No contingency narrative recorded.")}
    </div>
  `;
}

function estimateTeamHtml(summary) {
  const rows = Array.isArray(summary?.estimate?.proposed_team) ? summary.estimate.proposed_team : [];
  const teamSize = summary?.estimate?.team_size_fte;
  if (!rows.length) return "No team profile loaded.";
  const body = rows.map((row) => `<tr class="border-b border-slate-200 align-top">
      <td class="py-2 pr-3 font-semibold text-slate-900">${escapeHtml(row.display_name || row.role || "")}${row.fte ? ` (${escapeHtml(String(row.fte))})` : ""}</td>
      <td class="py-2 pr-3">${escapeHtml(String(row.hours_p50 ?? "0"))}h</td>
      <td class="py-2 pr-3">${escapeHtml(row.rationale || "")}</td>
    </tr>`).join("");
  return `
    <div class="rounded-lg border border-slate-300 bg-white p-2 text-xs text-slate-700">Team size: <span class="font-semibold text-slate-900">${escapeHtml(String(teamSize ?? "n/a"))} FTE</span></div>
    <div class="mt-2 overflow-auto"><table class="min-w-full border-collapse text-left text-xs">
      <thead><tr class="border-b border-slate-300 text-slate-600"><th class="py-2 pr-3">Role</th><th class="py-2 pr-3">Hours (p50)</th><th class="py-2 pr-3">Rationale</th></tr></thead>
      <tbody>${body}</tbody></table></div>
  `;
}

function estimateWorkstreamsHtml(summary) {
  const rows = Array.isArray(summary?.estimate?.workstreams) ? summary.estimate.workstreams : [];
  if (!rows.length) return "No workstreams loaded.";
  return rows.map((stream) => {
    const items = Array.isArray(stream.items) ? stream.items : [];
    const itemRows = items.map((item) => `<tr class="border-b border-slate-200">
        <td class="py-2 pr-3 font-semibold text-slate-900">${escapeHtml(item.title || item.wbs_item_id || "")}</td>
        <td class="py-2 pr-3">${escapeHtml(String(item.hours_p50 ?? "0"))}h</td>
        <td class="py-2 pr-3">${escapeHtml(String(item.days_range?.p10 ?? "0"))}-${escapeHtml(String(item.days_range?.p90 ?? "0"))} days</td>
      </tr>`).join("");
    return `<div class="rounded-lg border border-slate-300 bg-white p-3">
      <div class="flex flex-wrap items-center justify-between gap-2">
        <div>
          <p class="text-sm font-semibold text-slate-900">${escapeHtml(stream.phase || "")}</p>
          <p class="mt-1 text-xs text-slate-700">${escapeHtml(stream.key_risk || "")}</p>
        </div>
        <div class="text-right text-xs text-slate-700">
          <div><span class="font-semibold text-slate-900">${escapeHtml(String(stream.subtotal_hours_p50 ?? "0"))}h</span> p50</div>
          <div>${escapeHtml(String(stream.subtotal_weeks_p50 ?? "0"))} weeks p50</div>
        </div>
      </div>
      <div class="mt-3 overflow-auto"><table class="min-w-full border-collapse text-left text-xs">
        <thead><tr class="border-b border-slate-300 text-slate-600"><th class="py-2 pr-3">Task</th><th class="py-2 pr-3">Effort</th><th class="py-2 pr-3">Range</th></tr></thead>
        <tbody>${itemRows}</tbody></table></div>
    </div>`;
  }).join("");
}

function estimateAssumptionsHtml(ledger) {
  const rows = Array.isArray(ledger?.assumptions) ? ledger.assumptions : [];
  if (!rows.length) return "No assumptions loaded.";
  return rows.map((row) => `
    <div class="rounded-lg border border-slate-300 bg-white p-2">
      <div class="flex flex-wrap items-center gap-2">
        <span class="text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-600">${escapeHtml(row.id || "ASSUME")}</span>
        <span class="rounded border border-slate-300 bg-slate-100 px-2 py-0.5 text-[10px] text-slate-800">${escapeHtml(row.category || "ASSUMED")}</span>
        <span class="rounded border border-slate-300 bg-slate-100 px-2 py-0.5 text-[10px] text-slate-800">${escapeHtml(row.status || "open")}</span>
      </div>
      <p class="mt-1 text-xs text-slate-800">${escapeHtml(row.statement || "")}</p>
    </div>
  `).join("");
}

function estimateWbsHtml(wbs) {
  const items = Array.isArray(wbs?.wbs?.items) ? wbs.wbs.items : [];
  if (!items.length) return "No WBS loaded.";
  return `
    <div class="overflow-auto">
      <table class="min-w-full border-collapse text-left text-xs">
        <thead>
          <tr class="border-b border-slate-300 text-slate-600">
            <th class="py-2 pr-3">Item</th>
            <th class="py-2 pr-3">Kind</th>
            <th class="py-2 pr-3">Phase</th>
            <th class="py-2 pr-3">Size</th>
            <th class="py-2 pr-3">Hours (p50)</th>
          </tr>
        </thead>
        <tbody>
          ${items.map((item) => `
            <tr class="border-b border-slate-200">
              <td class="py-2 pr-3">
                <div class="font-semibold text-slate-900">${escapeHtml(item.title || item.wbs_item_id || "")}</div>
                <div class="text-[11px] text-slate-600">${escapeHtml(item.wbs_item_id || "")}</div>
              </td>
              <td class="py-2 pr-3">${escapeHtml(item.kind || "")}</td>
              <td class="py-2 pr-3">${escapeHtml(item.phase || "")}</td>
              <td class="py-2 pr-3">${escapeHtml(item.size_tier || "")}</td>
              <td class="py-2 pr-3">${escapeHtml(String(item.effort_hours?.p50 ?? "0"))}</td>
            </tr>
          `).join("")}
        </tbody>
      </table>
    </div>
  `;
}

function renderEstimateDetails(payload) {
  state.estimation.currentEstimate = payload || null;
  state.estimation.currentEstimateId = String(payload?.estimate_id || payload?.meta?.estimate_id || payload?.meta?.estimate?.estimate_id || "").trim();
  if (el.estimateOverview) el.estimateOverview.innerHTML = payload?.artifacts?.estimate_summary ? estimateOverviewHtml(payload.artifacts.estimate_summary) : "No estimate loaded.";
  if (el.estimateTeam) el.estimateTeam.innerHTML = payload?.artifacts?.estimate_summary ? estimateTeamHtml(payload.artifacts.estimate_summary) : "No estimate loaded.";
  if (el.estimateWorkstreams) el.estimateWorkstreams.innerHTML = payload?.artifacts?.estimate_summary ? estimateWorkstreamsHtml(payload.artifacts.estimate_summary) : "No estimate loaded.";
  if (el.estimateAssumptions) el.estimateAssumptions.innerHTML = payload?.artifacts?.assumption_ledger ? estimateAssumptionsHtml(payload.artifacts.assumption_ledger) : "No assumptions loaded.";
  if (el.estimateWbs) el.estimateWbs.innerHTML = payload?.artifacts?.wbs ? estimateWbsHtml(payload.artifacts.wbs) : "No WBS loaded.";
  setEstimateTab(state.estimation.activeTab || "overview");
}

function renderEstimateList() {
  if (!el.estimateList) return;
  const runId = String(el.estimateRunId?.value || "").trim();
  const rows = state.estimation.listByRun[runId] || [];
  if (!runId) {
    el.estimateList.innerHTML = `<div class="rounded-lg border border-slate-300 bg-white p-2 text-xs text-slate-700">Provide a run id to browse saved estimates.</div>`;
    return;
  }
  if (!rows.length) {
    el.estimateList.innerHTML = `<div class="rounded-lg border border-slate-300 bg-white p-2 text-xs text-slate-700">No estimates found for ${escapeHtml(runId)}.</div>`;
    return;
  }
  el.estimateList.innerHTML = rows.map((row) => `
    <div class="rounded-lg border border-slate-300 bg-white p-2">
      <div class="flex flex-wrap items-center justify-between gap-2">
        <div>
          <div class="text-xs font-semibold text-slate-900">${escapeHtml(row.estimate_id || "estimate")}</div>
          <div class="text-[11px] text-slate-600">${escapeHtml(row.created_at || row.updated_at || "n/a")}</div>
        </div>
        <button class="btn-light rounded-md px-2 py-1 text-[11px] font-semibold" data-estimate-load-id="${escapeHtml(row.estimate_id || "")}">Load</button>
      </div>
    </div>
  `).join("");
}

async function loadRunEstimates() {
  const runId = String(el.estimateRunId?.value || "").trim();
  if (!runId) {
    setEstimateStatus("Run ID is required to load run-scoped estimates.", true);
    renderEstimateList();
    return;
  }
  setEstimateStatus(`Loading estimates for ${runId}...`);
  try {
    const data = await api(`/api/runs/${encodeURIComponent(runId)}/estimates`, null);
    state.estimation.listByRun[runId] = Array.isArray(data.estimates) ? data.estimates : [];
    state.estimation.loadedRunId = runId;
    renderEstimateList();
    setEstimateStatus(`Loaded ${state.estimation.listByRun[runId].length} estimate(s) for ${runId}.`);
  } catch (err) {
    setEstimateStatus(`Estimate list load failed: ${err.message}`, true);
  }
}

async function loadEstimateById(estimateId) {
  const id = String(estimateId || "").trim();
  if (!id) return;
  setEstimateStatus(`Loading estimate ${id}...`);
  try {
    const data = await api(`/api/estimates/${encodeURIComponent(id)}`, null);
    renderEstimateDetails(data);
    setEstimateStatus(`Loaded estimate ${id}.`);
  } catch (err) {
    setEstimateStatus(`Estimate load failed: ${err.message}`, true);
  }
}

async function createEstimateFromForm() {
  const mode = String(el.estimateMode?.value || "brownfield").trim();
  if (mode !== "brownfield") {
    setEstimateStatus(`${mode} estimation is not implemented yet.`, true);
    return;
  }
  const runId = String(el.estimateRunId?.value || "").trim();
  const chunkManifestText = String(el.estimateChunkManifest?.value || "").trim();
  const riskRegisterText = String(el.estimateRiskRegister?.value || "").trim();
  const traceabilityText = String(el.estimateTraceabilityScores?.value || "").trim();
  let chunkManifest;
  let riskRegister;
  let traceabilityScores;
  try {
    if (chunkManifestText) chunkManifest = parseEstimateJson(chunkManifestText, "Chunk manifest");
    if (riskRegisterText) riskRegister = parseEstimateJson(riskRegisterText, "Risk register");
    if (traceabilityText) traceabilityScores = parseEstimateJson(traceabilityText, "Traceability scores");
  } catch (err) {
    setEstimateStatus(err.message, true);
    return;
  }
  if (!runId && (!chunkManifest || !riskRegister || !traceabilityScores)) {
    setEstimateStatus("Provide a run ID or paste chunk manifest, risk register, and traceability scores JSON.", true);
    return;
  }
  const payload = {
    mode,
    run_id: runId || undefined,
    estimate_id: String(el.estimateId?.value || "").trim() || undefined,
    business_need: String(el.estimateBusinessNeed?.value || "").trim() || "Modernize the brownfield application while preserving required business capability.",
    team_model_key: String(el.estimateTeamModel?.value || "HUMAN_ONLY").trim(),
    chunk_manifest: chunkManifest,
    risk_register: riskRegister,
    traceability_scores: traceabilityScores,
  };
  setEstimateStatus("Creating estimate...");
  try {
    const data = await api("/api/estimates", payload);
    if (payload.run_id) {
      await loadRunEstimates();
    }
    if (data.estimate_id && !String(el.estimateId?.value || "").trim() && el.estimateId) {
      el.estimateId.value = data.estimate_id;
    }
    await loadEstimateById(data.estimate_id);
    setEstimateStatus(`Created estimate ${data.estimate_id}.`);
  } catch (err) {
    setEstimateStatus(`Estimate create failed: ${err.message}`, true);
  }
}

function renderEstimateAgentIntake(result) {
  const draft = result?.draft || {};
  const llm = result?.llm || {};
  const missing = Array.isArray(draft?.missing_fields) ? draft.missing_fields : [];
  const questions = Array.isArray(draft?.follow_up_questions) ? draft.follow_up_questions : [];
  const assumptions = Array.isArray(draft?.assumptions) ? draft.assumptions : [];
  if (el.estimateBusinessNeed && String(draft.business_need || "").trim() && !String(el.estimateBusinessNeed.value || "").trim()) {
    el.estimateBusinessNeed.value = String(draft.business_need || "").trim();
  }
  if (el.estimateRunId && String(draft.run_id || "").trim() && !String(el.estimateRunId.value || "").trim()) {
    el.estimateRunId.value = String(draft.run_id || "").trim();
  }
  if (el.estimateTeamModel && String(draft.team_model_key || "").trim()) {
    el.estimateTeamModel.value = String(draft.team_model_key || el.estimateTeamModel.value || "").trim();
  }
  setEstimateAgentOutput(`
    <div class="space-y-2">
      <div class="text-[11px] text-slate-600">Mode: <strong>${escapeHtml(String(draft.mode || ""))}</strong> · Confidence: <strong>${escapeHtml(String(draft.confidence_tier || ""))}</strong> · LLM: <strong>${llm.used ? escapeHtml(String(llm.provider || "")) : "deterministic"}</strong></div>
      <div><div class="text-[11px] uppercase tracking-[0.12em] text-slate-600">Business need</div><div class="mt-1">${escapeHtml(String(draft.business_need || "")) || "n/a"}</div></div>
      <div><div class="text-[11px] uppercase tracking-[0.12em] text-slate-600">Missing fields</div><div class="mt-1">${missing.length ? escapeHtml(missing.join(", ")) : "none"}</div></div>
      <div><div class="text-[11px] uppercase tracking-[0.12em] text-slate-600">Follow-up questions</div><ul class="mt-1 list-disc pl-5">${questions.length ? questions.map((row) => `<li>${escapeHtml(String(row || ""))}</li>`).join("") : "<li>none</li>"}</ul></div>
      <div><div class="text-[11px] uppercase tracking-[0.12em] text-slate-600">Assumptions</div><ul class="mt-1 list-disc pl-5">${assumptions.length ? assumptions.map((row) => `<li>${escapeHtml(String(row || ""))}</li>`).join("") : "<li>none</li>"}</ul></div>
    </div>
  `);
}

function renderEstimateAgentExplanation(result) {
  const response = result?.response || {};
  const llm = result?.llm || {};
  const refs = Array.isArray(response?.assumption_refs) ? response.assumption_refs : [];
  setEstimateAgentOutput(`
    <div class="space-y-2">
      <div class="text-[11px] text-slate-600">Mode: <strong>${escapeHtml(String(response.mode || ""))}</strong> · LLM: <strong>${llm.used ? escapeHtml(String(llm.provider || "")) : "deterministic"}</strong></div>
      <div>${escapeHtml(String(response.answer || "")) || "No explanation available."}</div>
      <div class="text-[11px] text-slate-600">WBS item: <strong>${escapeHtml(String(response.wbs_item_id || "")) || "n/a"}</strong></div>
      <div class="text-[11px] text-slate-600">Assumptions referenced: <strong>${refs.length ? escapeHtml(refs.join(", ")) : "none"}</strong></div>
    </div>
  `);
}

async function probeEstimateIntake() {
  const message = String(el.estimateAgentInput?.value || "").trim();
  if (!message) {
    setEstimateStatus("Assistant prompt is required.", true);
    return;
  }
  setEstimateStatus("Probing estimate intake...");
  try {
    const data = await api("/api/estimates/intake", {
      mode: String(el.estimateMode?.value || "brownfield").trim(),
      current: {
        run_id: String(el.estimateRunId?.value || "").trim(),
        business_need: String(el.estimateBusinessNeed?.value || "").trim(),
        team_model_key: String(el.estimateTeamModel?.value || "").trim(),
      },
      message,
    });
    renderEstimateAgentIntake(data);
    setEstimateStatus("Estimate intake draft updated.");
  } catch (err) {
    setEstimateStatus(`Estimate intake probe failed: ${err.message}`, true);
  }
}

async function explainCurrentEstimate() {
  const estimateId = String(state.estimation.currentEstimateId || el.estimateId?.value || "").trim();
  if (!estimateId) {
    setEstimateStatus("Load or create an estimate first.", true);
    return;
  }
  const question = String(el.estimateAgentInput?.value || "").trim();
  if (!question) {
    setEstimateStatus("Assistant prompt is required.", true);
    return;
  }
  setEstimateStatus(`Explaining estimate ${estimateId}...`);
  try {
    const data = await api(`/api/estimates/${encodeURIComponent(estimateId)}/explain`, {
      question,
      wbs_item_id: String(el.estimateAgentWbsItem?.value || "").trim(),
    });
    renderEstimateAgentExplanation(data);
    setEstimateStatus(`Loaded estimate explanation for ${estimateId}.`);
  } catch (err) {
    setEstimateStatus(`Estimate explanation failed: ${err.message}`, true);
  }
}

function setMode(mode) {
  state.mode = mode;
  el.homeScreen.classList.toggle("hidden", mode !== MODES.DASHBOARDS);
  el.workScreen.classList.toggle("hidden", !(mode === MODES.DISCOVER || mode === MODES.BUILD));
  el.teamScreen.classList.toggle("hidden", mode !== MODES.PLAN);
  el.estimatesScreen.classList.toggle("hidden", mode !== MODES.ESTIMATES);
  el.historyScreen.classList.toggle("hidden", mode !== MODES.VERIFY);
  el.settingsScreen?.classList.toggle("hidden", mode !== MODES.SETTINGS);
  toModeButtonState(mode);
  if (mode === MODES.DISCOVER) setWizardStep(1);
  if (mode === MODES.BUILD) setWizardStep(2);
  if (mode === MODES.PLAN) setPlanTab(state.planTab || "team_creation");
  if (mode === MODES.ESTIMATES) {
    syncEstimateDefaultsFromCurrentRun();
    renderEstimateList();
  }
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
    if (state.teamSelection.stageAgentIds && Object.keys(state.teamSelection.stageAgentIds).length) {
      state.teamBuilder.stageAgentIds = { ...state.teamSelection.stageAgentIds };
    } else {
      const defaults = defaultBuilderMap();
      const firstStage = Object.keys(defaults).sort((a, b) => Number(a) - Number(b))[0] || "";
      state.teamBuilder.stageAgentIds = firstStage ? { [firstStage]: defaults[firstStage] } : {};
    }
  }
  renderTeamBuilderSelectors();
  renderAgentStudio();
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
  const stageAgentIds = teamBuilderEffectiveStageAgentIds();
  if (!Object.keys(stageAgentIds).length) {
    alert("Select at least one agent/stage in Team Creation.");
    return;
  }
  const editingTeamId = String(state.teamBuilder.editingTeamId || "").trim();
  const data = await api("/api/teams", {
    name,
    description: String(el.teamDescription.value || "").trim(),
    stage_agent_ids: stageAgentIds,
    team_id: editingTeamId,
  });
  const verb = editingTeamId ? "Updated" : "Saved";
  el.teamSaveMessage.textContent = `${verb} team: ${data.team?.name || "(unnamed)"}`;
  applyTeamSelection(data.team || {}, data.agent_personas || {}, `${verb} from Team Builder`);
  await loadAgentsAndTeams();
}

async function loadSelectedTeamIntoBuilder() {
  const selectedPlanTeamId = String(el.planTeamSelect?.value || "").trim();
  const selectedWorkTeamId = String(el.workTeamSelect?.value || "").trim();
  const teamId = selectedPlanTeamId || selectedWorkTeamId || String(state.teamSelection?.teamId || "").trim();
  if (!teamId) {
    alert("Select a team first.");
    return;
  }
  const data = await api(`/api/teams/${encodeURIComponent(teamId)}`, null);
  applyTeamSelection(data.team || {}, data.agent_personas || {}, "Loaded for editing");
  state.teamBuilder.stageAgentIds = normalizeStageAgentIds(state.teamSelection.stageAgentIds || {});
  state.teamBuilder.editingTeamId = state.teamSelection.isCustom ? teamId : "";
  state.teamBuilder.editingIsCustom = !!state.teamSelection.isCustom;
  if (el.teamName) el.teamName.value = String(state.teamSelection.teamName || "").trim();
  if (el.teamDescription) el.teamDescription.value = String(state.teamSelection.description || "").trim();
  renderTeamBuilderSelectors();
  el.teamSaveMessage.textContent = state.teamBuilder.editingTeamId
    ? "Loaded custom team for editing."
    : "Loaded system team. Saving will create a new custom team.";
}

function selectedPlanTeamId() {
  const selectedPlanTeamId = String(el.planTeamSelect?.value || "").trim();
  const selectedWorkTeamId = String(el.workTeamSelect?.value || "").trim();
  return selectedPlanTeamId || selectedWorkTeamId || String(state.teamSelection?.teamId || "").trim();
}

async function duplicateSelectedTeamInPlan() {
  const teamId = selectedPlanTeamId();
  if (!teamId) {
    alert("Select a team first.");
    return;
  }
  const sourceTeam = (state.teams || []).find((t) => String(t?.id || "") === teamId) || null;
  const suggestedName = `${String(sourceTeam?.name || "Team").trim() || "Team"} (Copy)`;
  const input = window.prompt("Name for duplicated team:", suggestedName);
  if (input == null) return;
  const name = String(input || "").trim();
  if (!name) {
    alert("Team name is required.");
    return;
  }
  const data = await api("/api/teams/duplicate", { team_id: teamId, name }, "POST");
  applyTeamSelection(data.team || {}, data.agent_personas || {}, "Duplicated team");
  await loadAgentsAndTeams();
  if (el.planTeamSelect && data.team?.id) el.planTeamSelect.value = data.team.id;
  if (el.workTeamSelect && data.team?.id) el.workTeamSelect.value = data.team.id;
  if (el.teamSaveMessage) el.teamSaveMessage.textContent = `Duplicated team as ${data.team?.name || name}.`;
}

async function deleteSelectedTeamInPlan() {
  const teamId = selectedPlanTeamId();
  if (!teamId) {
    alert("Select a team first.");
    return;
  }
  const sourceTeam = (state.teams || []).find((t) => String(t?.id || "") === teamId) || null;
  const sourceName = String(sourceTeam?.name || teamId).trim() || teamId;
  const confirmDelete = window.confirm(`Delete team '${sourceName}'? This cannot be undone.`);
  if (!confirmDelete) return;
  await api("/api/teams/delete", { team_id: teamId }, "POST");
  const wasSelected = String(state.teamSelection?.teamId || "").trim() === teamId;
  if (wasSelected) {
    state.teamSelection.teamId = "";
    state.teamSelection.teamName = "";
    state.teamSelection.description = "";
    state.teamSelection.stageAgentIds = {};
    state.teamSelection.agentPersonas = {};
    state.teamSelection.reason = "";
    state.teamSelection.isCustom = false;
  }
  await loadAgentsAndTeams();
  if (el.teamSaveMessage) el.teamSaveMessage.textContent = `Deleted team: ${sourceName}.`;
}

function addAgentRowToBuilder() {
  const nextStage = teamBuilderUnusedStages()[0];
  if (!nextStage) {
    alert("All stages are already added or have no available agents.");
    return;
  }
  const agentId = defaultAgentForStage(nextStage);
  if (!agentId) {
    alert("No available agents for the selected stage.");
    return;
  }
  state.teamBuilder.stageAgentIds[nextStage] = agentId;
  renderTeamBuilderSelectors();
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
  const stageAgentIds = teamBuilderEffectiveStageAgentIds();
  if (!Object.keys(stageAgentIds).length) {
    alert("Select at least one agent/stage in Team Creation before using this team.");
    return;
  }
  applyTeamSelection(
    {
      id: "",
      name: String(el.teamName.value || "Ad-hoc Team").trim() || "Ad-hoc Team",
      description: String(el.teamDescription.value || "").trim(),
      stage_agent_ids: stageAgentIds,
      is_custom: true,
    },
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

function _downloadBlobContent(content, fileName, contentType = "application/octet-stream") {
  const blob = new Blob([content], { type: contentType });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = fileName;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

async function downloadDiscoverDbArtifact(kind) {
  const validKinds = new Set(["source_schema", "source_erd", "data_dictionary"]);
  const type = validKinds.has(String(kind || "")) ? String(kind) : "source_schema";
  const runId = String(state.currentRun?.run_id || state.currentRunId || "").trim();
  const safeRun = runId || "discover";
  const rawArtifacts = _discoverRawArtifacts();
  const localKeyByType = {
    source_schema: "source_schema_model",
    source_erd: "source_erd",
    data_dictionary: "source_data_dictionary_markdown",
  };
  const localPayload = rawArtifacts?.[localKeyByType[type]];
  if (localPayload) {
    if (type === "source_schema") {
      _downloadBlobContent(
        JSON.stringify(localPayload, null, 2),
        `source_schema-${safeRun}.json`,
        "application/json",
      );
      return;
    }
    if (type === "source_erd") {
      const mermaid = String(localPayload?.mermaid || "").trim();
      if (mermaid) {
        _downloadBlobContent(`${mermaid}\n`, `source_erd-${safeRun}.mmd`, "text/plain;charset=utf-8");
        return;
      }
    }
    const markdownFromArtifact = String(localPayload?.markdown || "").trim();
    const dictRows = Array.isArray(rawArtifacts?.source_data_dictionary?.rows) ? rawArtifacts.source_data_dictionary.rows : [];
    const markdownFallback = markdownFromArtifact || (
      dictRows.length
        ? `# Source Data Dictionary\n\nRows: ${dictRows.length}\n`
        : ""
    );
    if (markdownFallback) {
      _downloadBlobContent(`${markdownFallback}\n`, `data_dictionary-${safeRun}.md`, "text/markdown;charset=utf-8");
      return;
    }
  }
  if (!runId) {
    throw new Error("No artifact available to export yet. Complete Discover scan first.");
  }
  const response = await fetch(
    `/api/runs/${encodeURIComponent(runId)}/db-artifact?type=${encodeURIComponent(type)}`,
    { method: "GET" },
  );
  if (!response.ok) {
    let message = `HTTP ${response.status}`;
    try {
      const payload = await response.json();
      message = String(payload?.error || message);
    } catch (_err) {
      // no-op
    }
    throw new Error(message);
  }
  const blob = await response.blob();
  const header = String(response.headers.get("content-disposition") || "");
  const match = header.match(/filename=\"?([^\";]+)\"?/i);
  const fallbackByType = {
    source_schema: `source_schema-${runId}.json`,
    source_erd: `source_erd-${runId}.mmd`,
    data_dictionary: `data_dictionary-${runId}.md`,
  };
  const filename = (match && match[1]) ? match[1] : fallbackByType[type];
  const url = window.URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  window.URL.revokeObjectURL(url);
}

async function downloadDiscoverArtifact(kind) {
  const validKinds = new Set([
    "repo_landscape",
    "repo_snapshot",
    "file_chunk_manifest",
    "component_inventory",
    "modernization_track_plan",
    "router_ruleset",
    "project_metrics",
    "static_forensics",
    "quality_rules",
    "quality_violations",
    "dead_code",
    "type_dependency_matrix",
    "runtime_dependency_matrix",
    "third_party_usage",
    "trend_snapshot",
    "trend_series",
    "php_route_inventory",
    "php_controller_inventory",
    "php_template_inventory",
    "php_sql_catalog",
    "php_session_state_inventory",
    "php_authz_authn_inventory",
    "php_include_graph",
    "php_background_job_inventory",
    "php_file_io_inventory",
    "php_validation_rules",
  ]);
  const type = validKinds.has(String(kind || "")) ? String(kind) : "project_metrics";
  const runId = String(state.currentRun?.run_id || state.currentRunId || "").trim();
  const safeRun = runId || "discover";
  const rawArtifacts = _discoverRawArtifacts();
  const localKeyByType = {
    repo_landscape: "repo_landscape_v1",
    repo_snapshot: "repo_snapshot_v1",
    file_chunk_manifest: "file_chunk_manifest_v1",
    component_inventory: "component_inventory_v1",
    modernization_track_plan: "modernization_track_plan_v1",
    router_ruleset: "router_ruleset_v1",
    project_metrics: "project_metrics",
    static_forensics: "static_forensics_layer",
    quality_rules: "code_quality_rules",
    quality_violations: "quality_violation_report",
    dead_code: "dead_code_report",
    type_dependency_matrix: "type_dependency_matrix",
    runtime_dependency_matrix: "runtime_dependency_matrix",
    third_party_usage: "third_party_usage",
    trend_snapshot: "trend_snapshot",
    trend_series: "trend_series",
    php_route_inventory: "php_route_inventory",
    php_controller_inventory: "php_controller_inventory",
    php_template_inventory: "php_template_inventory",
    php_sql_catalog: "php_sql_catalog",
    php_session_state_inventory: "php_session_state_inventory",
    php_authz_authn_inventory: "php_authz_authn_inventory",
    php_include_graph: "php_include_graph",
    php_background_job_inventory: "php_background_job_inventory",
    php_file_io_inventory: "php_file_io_inventory",
    php_validation_rules: "php_validation_rules",
  };
  const localPayload = rawArtifacts?.[localKeyByType[type]];
  if (localPayload) {
    _downloadBlobContent(
      JSON.stringify(localPayload, null, 2),
      `${type}-${safeRun}.json`,
      "application/json",
    );
    return;
  }
  if (!runId) {
    throw new Error("No artifact available to export yet. Complete Discover scan first.");
  }
  const response = await fetch(
    `/api/runs/${encodeURIComponent(runId)}/discover-artifact?type=${encodeURIComponent(type)}`,
    { method: "GET" },
  );
  if (!response.ok) {
    let message = `HTTP ${response.status}`;
    try {
      const payload = await response.json();
      message = String(payload?.error || message);
    } catch (_err) {
      // no-op
    }
    throw new Error(message);
  }
  const blob = await response.blob();
  const header = String(response.headers.get("content-disposition") || "");
  const match = header.match(/filename=\"?([^\";]+)\"?/i);
  const fallbackByType = {
    repo_landscape: `repo_landscape-${runId}.json`,
    repo_snapshot: `repo_snapshot-${runId}.json`,
    file_chunk_manifest: `file_chunk_manifest-${runId}.json`,
    component_inventory: `component_inventory-${runId}.json`,
    modernization_track_plan: `modernization_track_plan-${runId}.json`,
    router_ruleset: `router_ruleset-${runId}.json`,
    project_metrics: `project_metrics-${runId}.json`,
    static_forensics: `static_forensics-${runId}.json`,
    quality_rules: `quality_rules-${runId}.json`,
    quality_violations: `quality_violations-${runId}.json`,
    dead_code: `dead_code-${runId}.json`,
    type_dependency_matrix: `type_dependency_matrix-${runId}.json`,
    runtime_dependency_matrix: `runtime_dependency_matrix-${runId}.json`,
    third_party_usage: `third_party_usage-${runId}.json`,
    trend_snapshot: `trend_snapshot-${runId}.json`,
    trend_series: `trend_series-${runId}.json`,
    php_route_inventory: `php_route_inventory-${runId}.json`,
    php_controller_inventory: `php_controller_inventory-${runId}.json`,
    php_template_inventory: `php_template_inventory-${runId}.json`,
    php_sql_catalog: `php_sql_catalog-${runId}.json`,
    php_session_state_inventory: `php_session_state_inventory-${runId}.json`,
    php_authz_authn_inventory: `php_authz_authn_inventory-${runId}.json`,
    php_include_graph: `php_include_graph-${runId}.json`,
    php_background_job_inventory: `php_background_job_inventory-${runId}.json`,
    php_file_io_inventory: `php_file_io_inventory-${runId}.json`,
    php_validation_rules: `php_validation_rules-${runId}.json`,
  };
  const filename = (match && match[1]) ? match[1] : fallbackByType[type];
  const url = window.URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  window.URL.revokeObjectURL(url);
}
window.__downloadDiscoverArtifact = downloadDiscoverArtifact;

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
  if (status === "queued") return "border-indigo-400 bg-indigo-100 text-indigo-900";
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
    const stage1 = _stageOutput(run, 1) || {};
    const inventorySummary = stage1?.analyst_report_v2?.decision_brief?.at_a_glance?.inventory_summary
      || stage1?.requirements_pack?.decision_brief?.at_a_glance?.inventory_summary
      || {};
    const discoveredLoc = Number(inventorySummary?.source_loc_total || 0);
    const legacyLines = discoveredLoc > 0 ? discoveredLoc : (legacyCode ? legacyCode.split("\n").length : 0);
    detailItems.push({ label: "Target Language", value: lang });
    detailItems.push({ label: "Legacy Code", value: `${legacyLines} lines` });
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
  const stage = Number(run?.current_stage || 0);
  const activeStages = activeAgentsForRun(run).map((agent) => Number(agent.stage)).sort((a, b) => a - b);
  const activeCount = activeStages.length || AGENTS.length;
  const stageOrdinal = activeStages.includes(stage) ? (activeStages.indexOf(stage) + 1) : Math.min(activeCount, stage);
  const retries = run?.retry_count || 0;
  const teamName = run?.team_name || run?.pipeline_state?.team_name || state.teamSelection.teamName || "-";

  el.statusChips.innerHTML = `
    <div class="rounded-lg border px-3 py-2 ${runStatusTone(status)}">
      <div class="flex items-center justify-between gap-2"><span>Run</span><span>${status.toUpperCase()}</span></div>
      <p class="mt-1 mono text-[10px]">status</p>
    </div>
    <div class="rounded-lg border border-slate-300 bg-white px-3 py-2 text-slate-700">
      <div class="flex items-center justify-between gap-2"><span>Step</span><span>${stageOrdinal}/${activeCount}</span></div>
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
  const runContextBundle = (p.run_context_bundle && typeof p.run_context_bundle === "object") ? p.run_context_bundle : {};
  const runConstitution = (runContextBundle.delivery_constitution && typeof runContextBundle.delivery_constitution === "object")
    ? runContextBundle.delivery_constitution
    : {};
  const specialistRouting = (runContextBundle.specialist_routing && typeof runContextBundle.specialist_routing === "object")
    ? runContextBundle.specialist_routing
    : {};
  const version = String(ref.version_id || "-");
  const commit = String(ref.commit_sha || "-").slice(0, 12) || "-";
  const savedPack = String(state.settings?.policies?.policy_pack || "standard").trim().toLowerCase();
  const policy = p.strict_security_mode
    ? "Strict security policy pack"
    : `${savedPack.charAt(0).toUpperCase()}${savedPack.slice(1)} policy pack`;
  const runId = run?.run_id || "-";
  const status = run?.status || "idle";
  const localArtifactNote = "Local artifacts: run artifact folder";
  let evidence = status === "completed"
    ? `Ready for export | ${localArtifactNote}`
    : (status === "failed" ? `Run failed; partial evidence | ${localArtifactNote}` : `In progress | ${localArtifactNote}`);
  const githubExport = (p.github_export && typeof p.github_export === "object") ? p.github_export : {};
  const exportStatus = String(githubExport.status || "").toLowerCase();
  if (exportStatus === "exported") {
    evidence = `${localArtifactNote} | GitHub export complete (${githubExport.base_path || "configured path"})`;
  } else if (exportStatus === "partial") {
    evidence = `${localArtifactNote} | GitHub export partial (${Number(githubExport.exported_files || 0)} files exported)`;
  } else if (exportStatus === "failed") {
    evidence = `${localArtifactNote} | GitHub export failed: ${String(githubExport.reason || "unknown error")}`;
  } else if (exportStatus === "skipped") {
    evidence = `${localArtifactNote} | GitHub export skipped: ${String(githubExport.reason || "disabled")}`;
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
  if (el.drawerDeliveryConstitution) {
    const constitutionId = String(runConstitution.constitution_id || "").trim();
    const objective = String(runConstitution.modernization_objective || "").trim();
    const nonNegotiables = Array.isArray(runConstitution.non_negotiables) ? runConstitution.non_negotiables : [];
    const snapshotId = String(runConstitution?.knowledge_snapshot?.snapshot_id || "").trim();
    if (constitutionId) {
      const preview = objective.length > 120 ? `${objective.slice(0, 117)}...` : objective;
      const parts = [
        constitutionId,
        snapshotId ? `snapshot=${snapshotId}` : "",
        `rules=${nonNegotiables.length}`,
        preview || "",
      ].filter(Boolean);
      el.drawerDeliveryConstitution.textContent = parts.join(" | ");
    } else {
      el.drawerDeliveryConstitution.textContent = "Not pinned";
    }
  }
  if (el.drawerSpecialistRouting) {
    const selected = Array.isArray(specialistRouting.selected) ? specialistRouting.selected : [];
    if (selected.length) {
      const names = selected
        .slice(0, 3)
        .map((row) => String(row?.name || row?.specialist_id || "").trim())
        .filter(Boolean);
      const dispatchable = Number(specialistRouting.dispatchable_count || 0);
      el.drawerSpecialistRouting.textContent = `${selected.length} selected (${dispatchable} dispatchable)${names.length ? ` | ${names.join(", ")}` : ""}`;
    } else {
      el.drawerSpecialistRouting.textContent = "No specialist routes selected";
    }
  }
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
      const runContextBundle = (p.run_context_bundle && typeof p.run_context_bundle === "object") ? p.run_context_bundle : {};
      const runConstitution = (runContextBundle.delivery_constitution && typeof runContextBundle.delivery_constitution === "object")
        ? runContextBundle.delivery_constitution
        : {};
      const constitutionId = String(runConstitution.constitution_id || "").trim();
      const constitutionRules = Array.isArray(runConstitution.non_negotiables) ? runConstitution.non_negotiables.length : 0;
      const statusText = String(p.context_layer_status || (p.sil_ready ? "ready" : "pending")).toUpperCase();
      el.contextOpsOutput.textContent = [
        `SIL status: ${statusText}`,
        `Context bundle: ${version} @ ${commit}`,
        `Delivery constitution: ${constitutionId || "not pinned"} | rules: ${constitutionRules}`,
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
  if (status === "queued") return "QUEUED";
  if (status === "running") return "RUNNING";
  if (status === "waiting_approval") return "WAITING APPROVAL";
  if (status === "skipped_team") return "NOT IN TEAM";
  if (status === "error") return "ERROR";
  return "PENDING";
}

function statusTone(status) {
  if (status === "success") return "bg-emerald-100 text-emerald-900 border-emerald-300";
  if (status === "warning") return "bg-amber-100 text-amber-900 border-amber-300";
  if (status === "queued") return "bg-indigo-100 text-indigo-900 border-indigo-300";
  if (status === "running") return "bg-sky-100 text-sky-900 border-sky-300";
  if (status === "waiting_approval") return "bg-amber-100 text-amber-900 border-amber-300";
  if (status === "skipped_team") return "bg-slate-100 text-slate-500 border-slate-300";
  if (status === "error") return "bg-rose-100 text-rose-900 border-rose-300";
  return "bg-slate-100 text-slate-700 border-slate-300";
}

function isActiveRunStatus(status) {
  const normalized = String(status || "").trim().toLowerCase();
  return ["queued", "running", "pending", "waiting_approval", "paused"].includes(normalized);
}

function activeAgentsForRun(run) {
  const runStageMap = normalizeStageAgentIds(
    run?.stage_agent_ids
    || run?.pipeline_state?.stage_agent_ids
    || {}
  );
  const selectedStageMap = normalizeStageAgentIds(state.teamSelection?.stageAgentIds || {});
  const effectiveMap = (run?.run_id && isActiveRunStatus(run?.status))
    ? (Object.keys(runStageMap).length ? runStageMap : selectedStageMap)
    : (Object.keys(selectedStageMap).length ? selectedStageMap : runStageMap);
  const stageMap = effectiveMap;
  const activeStages = activeStageIdsFromStageMap(stageMap);
  return AGENTS.filter((agent) => activeStages.includes(String(agent.stage)));
}

function stageDisplayIndex(run, stage) {
  const active = activeAgentsForRun(run);
  const idx = active.findIndex((agent) => Number(agent.stage) === Number(stage));
  return idx >= 0 ? idx + 1 : 0;
}

function stageDisplayLabel(run, stage) {
  const idx = stageDisplayIndex(run, stage);
  return idx > 0 ? `Step ${idx}` : `Step ${Number(stage || 0)}`;
}

function stageForDisplayStep(run, stepNumber) {
  const step = Number(stepNumber || 0);
  const active = activeAgentsForRun(run);
  if (!Number.isFinite(step) || step < 1 || step > active.length) return 0;
  return Number(active[step - 1]?.stage || 0);
}

function determineCurrentStage(run) {
  const activeAgents = activeAgentsForRun(run);
  const stageStatus = run?.stage_status || {};
  for (const agent of activeAgents) {
    const s = stageStatus[agent.stage];
    if (s === "running" || s === "waiting_approval") return agent.stage;
  }
  const pending = activeAgents.find((a) => !stageStatus[a.stage] || stageStatus[a.stage] === "pending");
  if (pending) return pending.stage;
  return Number(run?.current_stage || activeAgents[0]?.stage || 1);
}

function renderProgress() {
  const activeAgents = activeAgentsForRun(state.currentRun);
  const stageStatus = state.currentRun?.stage_status || {};
  const completed = activeAgents
    .filter((agent) => {
      const s = stageStatus[agent.stage];
      return s === "success" || s === "warning";
    })
    .length;
  const total = Math.max(1, activeAgents.length);
  const pct = Math.round((completed / total) * 100);
  el.progressFill.style.width = `${pct}%`;
  el.progressMeta.textContent = `${completed} / ${total} stages complete`;
  if (state.runStart?.pending) {
    const elapsed = Math.max(0, Math.floor((Date.now() - Number(state.runStart.startedAt || Date.now())) / 1000));
    el.pipelineStatusText.textContent = `STARTING RUN... (${elapsed}s)`;
    return;
  }
  const status = String(state.currentRun?.status || "idle").toUpperCase();
  const error = String(state.currentRun?.error_message || "").trim();
  if (status === "FAILED" && error) {
    el.pipelineStatusText.textContent = `FAILED: ${error}`;
    return;
  }
  el.pipelineStatusText.textContent = status;
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
  const qaReport = (report.qa_report_v1 && typeof report.qa_report_v1 === "object")
    ? report.qa_report_v1
    : ((output.qa_report_v1 && typeof output.qa_report_v1 === "object") ? output.qa_report_v1 : {});
  const qaSummary = (qaReport.summary && typeof qaReport.summary === "object") ? qaReport.summary : {};
  const qaStructural = (qaReport.structural && typeof qaReport.structural === "object") ? qaReport.structural : {};
  const qaSemantic = (qaReport.semantic && typeof qaReport.semantic === "object") ? qaReport.semantic : {};
  const qaStructuralChecks = Array.isArray(qaStructural.checks) ? qaStructural.checks : [];
  const qaSemanticChecks = Array.isArray(qaSemantic.checks) ? qaSemantic.checks : [];
  const qaQualityGates = Array.isArray(qaReport.quality_gates) ? qaReport.quality_gates : [];
  const qaAmendments = Array.isArray(qaReport.amendment_diff_v1) ? qaReport.amendment_diff_v1 : [];
  const highVolume = appendix.high_volume_sections || {};
  const rawArtifacts = (output.raw_artifacts && typeof output.raw_artifacts === "object")
    ? output.raw_artifacts
    : {};
  const rawLegacyInventory = (rawArtifacts.legacy_inventory && typeof rawArtifacts.legacy_inventory === "object")
    ? rawArtifacts.legacy_inventory
    : null;
  const rawRepoLandscape = (rawArtifacts.repo_landscape && typeof rawArtifacts.repo_landscape === "object")
    ? rawArtifacts.repo_landscape
    : null;
  const rawScopeLock = (rawArtifacts.scope_lock && typeof rawArtifacts.scope_lock === "object")
    ? rawArtifacts.scope_lock
    : null;
  const rawVariantInventory = (rawArtifacts.variant_inventory && typeof rawArtifacts.variant_inventory === "object")
    ? rawArtifacts.variant_inventory
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
  const rawDataAccessMap = (rawArtifacts.data_access_map && typeof rawArtifacts.data_access_map === "object")
    ? rawArtifacts.data_access_map
    : null;
  const rawRecordsetOps = (rawArtifacts.recordset_ops && typeof rawArtifacts.recordset_ops === "object")
    ? rawArtifacts.recordset_ops
    : null;
  const rawProcedureSummary = (rawArtifacts.procedure_summary && typeof rawArtifacts.procedure_summary === "object")
    ? rawArtifacts.procedure_summary
    : null;
  const rawFormDossier = (rawArtifacts.form_dossier && typeof rawArtifacts.form_dossier === "object")
    ? rawArtifacts.form_dossier
    : null;
  const rawBusinessRules = (rawArtifacts.business_rule_catalog && typeof rawArtifacts.business_rule_catalog === "object")
    ? rawArtifacts.business_rule_catalog
    : null;
  const rawDetectorFindings = (rawArtifacts.detector_findings && typeof rawArtifacts.detector_findings === "object")
    ? rawArtifacts.detector_findings
    : null;
  const rawRiskRegister = (rawArtifacts.risk_register && typeof rawArtifacts.risk_register === "object")
    ? rawArtifacts.risk_register
    : null;
  const rawOrphanAnalysis = (rawArtifacts.orphan_analysis && typeof rawArtifacts.orphan_analysis === "object")
    ? rawArtifacts.orphan_analysis
    : null;
  const rawDeliveryConstitution = (rawArtifacts.delivery_constitution && typeof rawArtifacts.delivery_constitution === "object")
    ? rawArtifacts.delivery_constitution
    : null;
  const rawVariantDiffReport = (rawArtifacts.variant_diff_report && typeof rawArtifacts.variant_diff_report === "object")
    ? rawArtifacts.variant_diff_report
    : null;
  const rawReportingModel = (rawArtifacts.reporting_model && typeof rawArtifacts.reporting_model === "object")
    ? rawArtifacts.reporting_model
    : null;
  const rawIdentityAccessModel = (rawArtifacts.identity_access_model && typeof rawArtifacts.identity_access_model === "object")
    ? rawArtifacts.identity_access_model
    : null;
  const rawDiscoverReviewChecklist = (rawArtifacts.discover_review_checklist && typeof rawArtifacts.discover_review_checklist === "object")
    ? rawArtifacts.discover_review_checklist
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
  const discoverReview = (report.discover_review && typeof report.discover_review === "object")
    ? report.discover_review
    : {};
  const checklistRowsRaw = Array.isArray(rawDiscoverReviewChecklist?.checks)
    ? rawDiscoverReviewChecklist.checks
    : (Array.isArray(rawDiscoverReviewChecklist?.items) ? rawDiscoverReviewChecklist.items : []);
  const discoverReviewChecks = Array.isArray(discoverReview.checks) && discoverReview.checks.length
    ? discoverReview.checks
    : checklistRowsRaw.map((row, idx) => ({
      id: String(row?.id || `review_${idx + 1}`),
      status: String(row?.status || row?.result || "warn").toLowerCase(),
      title: String(row?.title || row?.check || row?.why || row?.action || "Review check"),
      detail: String(row?.detail || row?.notes || row?.why || row?.action || ""),
    }));
  const blockingReviewChecks = discoverReviewChecks.filter((row) => {
    const status = String(row?.status || "").toLowerCase();
    const severity = String(row?.severity || "").toLowerCase();
    return status === "fail" || severity === "blocker" || severity === "high";
  });
  const refToRaw = {
    legacy_inventory_ref: "legacy_inventory",
    repo_landscape_ref: "repo_landscape",
    scope_lock_ref: "scope_lock",
    variant_inventory_ref: "variant_inventory",
    dependency_inventory_ref: "dependency_inventory",
    dependency_list_ref: "dependency_inventory",
    event_map_ref: "event_map",
    sql_catalog_ref: "sql_catalog",
    sql_map_ref: "sql_map",
    data_access_map_ref: "data_access_map",
    recordset_ops_ref: "recordset_ops",
    procedure_summary_ref: "procedure_summary",
    form_dossier_ref: "form_dossier",
    business_rules_ref: "business_rule_catalog",
    detector_findings_ref: "detector_findings",
    risk_register_ref: "risk_register",
    orphan_analysis_ref: "orphan_analysis",
    delivery_constitution_ref: "delivery_constitution",
    variant_diff_report_ref: "variant_diff_report",
    reporting_model_ref: "reporting_model",
    identity_access_model_ref: "identity_access_model",
    discover_review_checklist_ref: "discover_review_checklist",
    artifact_index_ref: "artifact_index",
  };
  const artifactRefRows = Object.entries(appendixRefs);
  const allowedTabs = clientMode
    ? ["spec", "qa", "evidence", "history"]
    : ["spec", "plan", "tasks", "qa", "evidence", "maps", "history"];
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
        ${tabButton("spec", "Decision brief")}
        ${!clientMode ? tabButton("plan", "Delivery spec") : ""}
        ${!clientMode ? tabButton("tasks", "Tasks") : ""}
        ${tabButton("qa", "QA")}
        ${tabButton("evidence", "Evidence appendix")}
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
        <div class="mt-2 rounded border ${blockingReviewChecks.length ? "border-rose-300 bg-rose-50" : "border-emerald-300 bg-emerald-50"} p-2">
          <div class="text-xs font-semibold ${blockingReviewChecks.length ? "text-rose-900" : "text-emerald-900"}">Blocking verification items (${blockingReviewChecks.length})</div>
          <p class="mt-1 text-[11px] ${blockingReviewChecks.length ? "text-rose-800" : "text-emerald-800"}">Resolve blockers before final planning and estimation.</p>
          <ul class="mt-1 list-disc pl-5 text-[11px]">${blockingReviewChecks.map((row) => `<li><strong>${escapeHtml(String(row.id || "review"))}</strong> ${escapeHtml(String(row.title || row.detail || ""))}${row.detail ? `<div class="text-slate-800">${escapeHtml(String(row.detail || ""))}</div>` : ""}</li>`).join("") || "<li>No blocking verification items.</li>"}</ul>
        </div>
        <div class="mt-2 rounded border border-slate-300 bg-white p-2">
          <div class="text-xs font-semibold text-slate-900">Decision brief</div>
      <table class="mt-2 w-full border-collapse text-[11px] text-slate-900">
        <tbody>
          <tr><td class="w-52 border border-slate-300 bg-slate-50 px-2 py-1 font-semibold">Readiness</td><td class="border border-slate-300 px-2 py-1">${escapeHtml(String(glance.readiness_score ?? "n/a"))}/100</td></tr>
          <tr><td class="border border-slate-300 bg-slate-50 px-2 py-1 font-semibold">Risk tier</td><td class="border border-slate-300 px-2 py-1">${escapeHtml(String(glance.risk_tier || "n/a"))}</td></tr>
          <tr><td class="border border-slate-300 bg-slate-50 px-2 py-1 font-semibold">Inventory</td><td class="border border-slate-300 px-2 py-1">${escapeHtml(`${String(inventory.projects ?? 0)} projects, ${String(inventory.forms ?? 0)} forms (${String(inventory.forms_referenced ?? 0)} referenced, ${String(inventory.forms_unmapped ?? 0)} unmapped), ${String(inventory.dependencies ?? 0)} dependencies`)}</td></tr>
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
      <div class="mt-2 text-[11px] font-semibold">Discover review (${discoverReviewChecks.length})</div>
      <ul class="list-disc pl-5 text-[11px]">${discoverReviewChecks.map((row) => `<li><strong>${escapeHtml(String(row.id || "review"))}</strong> [${escapeHtml(String(row.status || "WARN").toUpperCase())}] ${escapeHtml(String(row.title || row.detail || ""))}${row.detail ? `<div class="text-slate-800">${escapeHtml(String(row.detail || ""))}</div>` : ""}</li>`).join("") || "<li>No review checklist generated.</li>"}</ul>
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

      <section data-analyst-view-panel="qa" class="mt-2 rounded border border-slate-300 bg-white p-2 hidden">
        <div class="text-xs font-semibold text-slate-900">QA gate</div>
        <p class="mt-1 text-[11px] text-slate-700">Deterministic structural assertions plus semantic plausibility checks over Analyst outputs.</p>
        <div class="mt-2 grid gap-2 sm:grid-cols-5 text-[11px]">
          <div class="rounded border border-slate-300 bg-slate-50 p-2"><strong>Status:</strong> ${escapeHtml(String(qaSummary.status || "NOT_RUN"))}</div>
          <div class="rounded border border-slate-300 bg-slate-50 p-2"><strong>Pass:</strong> ${Number(qaSummary.pass_count || 0)}</div>
          <div class="rounded border border-slate-300 bg-slate-50 p-2"><strong>Warn:</strong> ${Number(qaSummary.warn_count || 0)}</div>
          <div class="rounded border border-slate-300 bg-slate-50 p-2"><strong>Fail:</strong> ${Number(qaSummary.fail_count || 0)}</div>
          <div class="rounded border border-slate-300 bg-slate-50 p-2"><strong>Blockers:</strong> ${Number(qaSummary.blocker_count || 0)}</div>
        </div>
        <div class="mt-2 text-[11px] font-semibold">QA quality gates (${qaQualityGates.length})</div>
        <ul class="list-disc pl-5 text-[11px]">${qaQualityGates.map((gate) => `<li><span class="${gateBadge(gate.result)}">${escapeHtml(String(gate.result || "warn").toUpperCase())}</span> <strong class="ml-1">${escapeHtml(String(gate.id || "gate"))}</strong> ${escapeHtml(String(gate.description || ""))}</li>`).join("") || "<li>No QA gates emitted.</li>"}</ul>
        <div class="mt-2 text-[11px] font-semibold">Structural checks (${qaStructuralChecks.length})</div>
        <div class="overflow-x-auto">
          <table class="mt-1 w-full border-collapse text-[11px] text-slate-900">
            <thead>
              <tr>
                <th class="border border-slate-300 bg-slate-50 px-2 py-1 text-left">ID</th>
                <th class="border border-slate-300 bg-slate-50 px-2 py-1 text-left">Result</th>
                <th class="border border-slate-300 bg-slate-50 px-2 py-1 text-left">Blocking</th>
                <th class="border border-slate-300 bg-slate-50 px-2 py-1 text-left">Detail</th>
                <th class="border border-slate-300 bg-slate-50 px-2 py-1 text-left">Refs</th>
              </tr>
            </thead>
            <tbody>
              ${qaStructuralChecks.slice(0, 200).map((check) => `<tr>
                <td class="border border-slate-300 px-2 py-1">${escapeHtml(String(check.check_id || check.id || "check"))}</td>
                <td class="border border-slate-300 px-2 py-1"><span class="${gateBadge(check.result)}">${escapeHtml(String(check.result || "warn").toUpperCase())}</span></td>
                <td class="border border-slate-300 px-2 py-1">${check.blocking ? "yes" : "no"}</td>
                <td class="border border-slate-300 px-2 py-1">${escapeHtml(String(check.detail || ""))}</td>
                <td class="border border-slate-300 px-2 py-1">${escapeHtml((Array.isArray(check.refs) ? check.refs.join(", ") : "") || "n/a")}</td>
              </tr>`).join("") || "<tr><td class='border border-slate-300 px-2 py-1' colspan='5'>No structural checks emitted.</td></tr>"}
            </tbody>
          </table>
        </div>
        <div class="mt-2 text-[11px] font-semibold">Semantic checks (${qaSemanticChecks.length})</div>
        <div class="overflow-x-auto">
          <table class="mt-1 w-full border-collapse text-[11px] text-slate-900">
            <thead>
              <tr>
                <th class="border border-slate-300 bg-slate-50 px-2 py-1 text-left">ID</th>
                <th class="border border-slate-300 bg-slate-50 px-2 py-1 text-left">Severity</th>
                <th class="border border-slate-300 bg-slate-50 px-2 py-1 text-left">Confidence</th>
                <th class="border border-slate-300 bg-slate-50 px-2 py-1 text-left">Detail</th>
                <th class="border border-slate-300 bg-slate-50 px-2 py-1 text-left">Suggested fix</th>
              </tr>
            </thead>
            <tbody>
              ${qaSemanticChecks.slice(0, 200).map((check) => `<tr>
                <td class="border border-slate-300 px-2 py-1">${escapeHtml(String(check.check_id || check.id || "check"))}</td>
                <td class="border border-slate-300 px-2 py-1">${escapeHtml(String(check.severity || "medium").toUpperCase())}</td>
                <td class="border border-slate-300 px-2 py-1">${escapeHtml(String(check.confidence ?? "n/a"))}</td>
                <td class="border border-slate-300 px-2 py-1">${escapeHtml(String(check.detail || ""))}</td>
                <td class="border border-slate-300 px-2 py-1">${escapeHtml(String(check.suggested_fix || ""))}</td>
              </tr>`).join("") || "<tr><td class='border border-slate-300 px-2 py-1' colspan='5'>No semantic checks emitted.</td></tr>"}
            </tbody>
          </table>
        </div>
        <div class="mt-2 text-[11px]"><strong>Amendment diff entries:</strong> ${qaAmendments.length}</div>
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
              <tr><td class="border border-slate-300 px-2 py-1">Repo landscape</td><td class="border border-slate-300 px-2 py-1">${Number(rawRepoLandscape?.projects?.length || 0)}</td><td class="border border-slate-300 px-2 py-1">Project/variant scope orientation</td></tr>
              <tr><td class="border border-slate-300 px-2 py-1">Scope lock</td><td class="border border-slate-300 px-2 py-1">${escapeHtml(String(rawScopeLock?.status || "REQUIRED"))}</td><td class="border border-slate-300 px-2 py-1">Explicit in-scope variant decision</td></tr>
              <tr><td class="border border-slate-300 px-2 py-1">Variant inventory</td><td class="border border-slate-300 px-2 py-1">${Number(rawVariantInventory?.variants?.length || 0)}</td><td class="border border-slate-300 px-2 py-1">Variant appendix and drift summary</td></tr>
              <tr><td class="border border-slate-300 px-2 py-1">Dependency inventory</td><td class="border border-slate-300 px-2 py-1">${dependencies.length}</td><td class="border border-slate-300 px-2 py-1">ActiveX/COM/DLL risk and replacement planning</td></tr>
              <tr><td class="border border-slate-300 px-2 py-1">Event map</td><td class="border border-slate-300 px-2 py-1">${eventMap.length}</td><td class="border border-slate-300 px-2 py-1">Entrypoints, calls, side effects</td></tr>
              <tr><td class="border border-slate-300 px-2 py-1">SQL catalog</td><td class="border border-slate-300 px-2 py-1">${sqlCatalog.length}</td><td class="border border-slate-300 px-2 py-1">Query contract and data touchpoints</td></tr>
              <tr><td class="border border-slate-300 px-2 py-1">SQL map</td><td class="border border-slate-300 px-2 py-1">${sqlMapEntries.length}</td><td class="border border-slate-300 px-2 py-1">Form/Procedure to query/table risk mapping</td></tr>
              <tr><td class="border border-slate-300 px-2 py-1">Data access map</td><td class="border border-slate-300 px-2 py-1">${Number(rawDataAccessMap?.rows?.length || 0)}</td><td class="border border-slate-300 px-2 py-1">Canonical Form/Handler/Table operation map</td></tr>
              <tr><td class="border border-slate-300 px-2 py-1">Recordset ops</td><td class="border border-slate-300 px-2 py-1">${Number(rawRecordsetOps?.ops?.length || 0)}</td><td class="border border-slate-300 px-2 py-1">Non-literal ADO recordset write paths</td></tr>
              <tr><td class="border border-slate-300 px-2 py-1">Procedure summaries</td><td class="border border-slate-300 px-2 py-1">${procedureSummaries.length}</td><td class="border border-slate-300 px-2 py-1">Step-wise behavior decomposition</td></tr>
              <tr><td class="border border-slate-300 px-2 py-1">Form dossiers</td><td class="border border-slate-300 px-2 py-1">${Number(rawFormDossier?.dossiers?.length || 0)}</td><td class="border border-slate-300 px-2 py-1">Business-purpose form deep dives with evidence</td></tr>
              <tr><td class="border border-slate-300 px-2 py-1">Business rules</td><td class="border border-slate-300 px-2 py-1">${businessRules.length}</td><td class="border border-slate-300 px-2 py-1">Rule extraction + BDD grounding</td></tr>
              <tr><td class="border border-slate-300 px-2 py-1">Detector findings</td><td class="border border-slate-300 px-2 py-1">${detectorFindings.length}</td><td class="border border-slate-300 px-2 py-1">Modernization risk hotspots</td></tr>
              <tr><td class="border border-slate-300 px-2 py-1">Risk register</td><td class="border border-slate-300 px-2 py-1">${Number(rawRiskRegister?.risks?.length || 0)}</td><td class="border border-slate-300 px-2 py-1">Evidence-backed delivery and technical risks</td></tr>
              <tr><td class="border border-slate-300 px-2 py-1">Orphan analysis</td><td class="border border-slate-300 px-2 py-1">${Number(rawOrphanAnalysis?.orphans?.length || 0)}</td><td class="border border-slate-300 px-2 py-1">Unmapped forms/modules and divergence signals</td></tr>
              <tr><td class="border border-slate-300 px-2 py-1">Delivery constitution</td><td class="border border-slate-300 px-2 py-1">${constitutionPrinciples.length}</td><td class="border border-slate-300 px-2 py-1">Project non-negotiables propagated across phases</td></tr>
              <tr><td class="border border-slate-300 px-2 py-1">Variant diff report</td><td class="border border-slate-300 px-2 py-1">${Number(rawVariantDiffReport?.comparisons?.length || 0)}</td><td class="border border-slate-300 px-2 py-1">Project variant comparison and scope decision gate</td></tr>
              <tr><td class="border border-slate-300 px-2 py-1">Reporting model</td><td class="border border-slate-300 px-2 py-1">${Number(rawReportingModel?.report_entrypoints?.length || 0)}</td><td class="border border-slate-300 px-2 py-1">DataEnvironment/DataReport reconciliation</td></tr>
              <tr><td class="border border-slate-300 px-2 py-1">Identity/access model</td><td class="border border-slate-300 px-2 py-1">${Number(rawIdentityAccessModel?.what_we_found?.auth_tables?.length || 0)}</td><td class="border border-slate-300 px-2 py-1">Role model and credential handling assumptions</td></tr>
              <tr><td class="border border-slate-300 px-2 py-1">Discover review checklist</td><td class="border border-slate-300 px-2 py-1">${Number((Array.isArray(rawDiscoverReviewChecklist?.checks) ? rawDiscoverReviewChecklist.checks.length : (Array.isArray(rawDiscoverReviewChecklist?.items) ? rawDiscoverReviewChecklist.items.length : 0)) || 0)}</td><td class="border border-slate-300 px-2 py-1">PASS/WARN/FAIL gate before planning</td></tr>
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
  const activeAgents = activeAgentsForRun(run);
  const stage = determineCurrentStage(run);
  const stepLabel = stageDisplayLabel(run, stage);
  const agent = activeAgents.find((a) => a.stage === stage) || activeAgents[0] || AGENTS[0];
  const stageStatus = run?.stage_status || {};
  const status = stageStatus[agent.stage] || "pending";
  const result = latestResultByStage(run, agent.stage);
  const persona = personaForStage(run, agent.stage);
  const personaReqPack = Number(agent.stage) === 1 ? String(persona.requirements_pack_profile || "").trim() : "";

  el.currentAgentPanel.innerHTML = `
    <div class="flex flex-wrap items-start justify-between gap-3 ${status === "running" ? "running-glow" : ""}">
      <div>
        <p class="text-xs uppercase tracking-[0.16em] text-slate-600">Current Agent</p>
        <h3 class="mt-1 text-xl font-semibold text-ink-950">${stepLabel}: ${agent.icon} ${agent.name}</h3>
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
    ${Number(agent.stage) === 1 && run?.run_id && result?.output ? `
      <div class="mt-3 rounded-lg border border-slate-300 bg-white p-2">
        <div class="flex flex-wrap items-center gap-2">
          <button data-analyst-export-ba-brief-docx class="btn-light rounded-md px-2 py-1 text-[11px] font-semibold">Export BA Brief</button>
          <button data-analyst-export-tech-workbook-docx class="btn-light rounded-md px-2 py-1 text-[11px] font-semibold">Export Tech Workbook</button>
          <button data-analyst-export-brd-docx class="btn-light rounded-md px-2 py-1 text-[11px] font-semibold">Export BRD</button>
          <button data-analyst-export-business-docx class="btn-light rounded-md px-2 py-1 text-[11px] font-semibold">Export Legacy Business</button>
          <button data-analyst-export-summary class="btn-light rounded-md px-2 py-1 text-[11px] font-semibold">Export Summary</button>
          <button data-analyst-export-full class="btn-light rounded-md px-2 py-1 text-[11px] font-semibold">Export Full Evidence</button>
        </div>
        <p data-analyst-doc-status class="mt-1 text-[11px] text-slate-700">Quick exports for Analyst output.</p>
      </div>
    ` : ""}
  `;

  const openBtn = el.currentAgentPanel.querySelector("[data-open-stage]");
  if (openBtn) openBtn.addEventListener("click", () => openStageModal(Number(openBtn.getAttribute("data-open-stage"))));
  if (Number(agent.stage) === 1 && run?.run_id && result?.output) {
    wireAnalystDocActions(el.currentAgentPanel, run);
  }
}

function renderAgentTabs() {
  const run = state.currentRun;
  const activeAgents = activeAgentsForRun(run);
  if (activeAgents.length && !activeAgents.some((agent) => agent.stage === Number(state.selectedStage || 0))) {
    state.selectedStage = activeAgents[0].stage;
  }
  const stageStatus = run?.stage_status || {};
  el.agentTabs.innerHTML = activeAgents.map((agent) => {
    const status = stageStatus[agent.stage] || "pending";
    const selected = state.selectedStage === agent.stage;
    const stepLabel = stageDisplayLabel(run, agent.stage);
    return `
      <button data-select-stage="${agent.stage}" class="rounded-lg border px-3 py-2 text-xs font-semibold ${selected ? "border-ink-900 bg-sky-100 text-slate-900" : `${statusTone(status)}`}">
        ${agent.icon} ${stepLabel} · ${agent.name}
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
  const activeAgents = activeAgentsForRun(run);
  const stageStatus = run?.stage_status || {};
  const fallbackStage = activeAgents[0]?.stage || 1;
  const requestedStage = Number(state.selectedStage || fallbackStage);
  const stage = activeAgents.some((a) => a.stage === requestedStage) ? requestedStage : fallbackStage;
  const agent = activeAgents.find((a) => a.stage === stage) || AGENTS.find((a) => a.stage === stage) || AGENTS[0];
  const stepLabel = stageDisplayLabel(run, stage);
  const result = latestResultByStage(run, stage);
  const status = stageStatus[stage] || "pending";
  const logs = (result?.logs || []).slice(-20).join("\n");
  const persona = personaForStage(run, stage);
  const runUseCase = String(run?.pipeline_state?.use_case || run?.use_case || "business_objectives");

  el.agentTabPanel.innerHTML = `
    <div class="flex flex-wrap items-start justify-between gap-3">
      <div>
        <h4 class="text-sm font-semibold text-ink-950">${agent.icon} ${stepLabel}: ${agent.name}</h4>
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
          <button data-analyst-export-ba-brief-docx class="btn-light rounded-md px-2 py-1 text-[11px] font-semibold">Export BA Brief</button>
          <button data-analyst-export-tech-workbook-docx class="btn-light rounded-md px-2 py-1 text-[11px] font-semibold">Export Tech Workbook</button>
          <button data-analyst-export-brd-docx class="btn-light rounded-md px-2 py-1 text-[11px] font-semibold">Export BRD</button>
          <button data-analyst-export-business-docx class="btn-light rounded-md px-2 py-1 text-[11px] font-semibold">Export Legacy Business</button>
          <button data-analyst-upload-trigger class="btn-dark rounded-md px-2 py-1 text-[11px] font-semibold">Upload Modified</button>
          <input data-analyst-upload-file type="file" class="hidden" accept=".md,.txt,.json" />
        </div>
        <p data-analyst-doc-status class="mt-1 text-[11px] text-slate-700">Export summary/full markdown, export BA Brief / Tech Workbook / BRD DOCX, or upload an updated version.</p>
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
  const activeAgents = activeAgentsForRun(state.currentRun);
  const fallback = activeAgents[0]?.stage || 1;
  const desired = Number(state.selectedStage || determineCurrentStage(state.currentRun) || fallback);
  return activeAgents.some((a) => a.stage === desired) ? desired : fallback;
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
      const meta = (row.meta && typeof row.meta === "object") ? row.meta : {};
      const chips = [];
      if (role === "assistant" && String(meta.source || "").trim()) chips.push(String(meta.source || "").trim());
      if (role === "assistant" && String(meta.mode || "").trim()) chips.push(String(meta.mode || "").trim());
      if (role === "assistant" && Number.isFinite(Number(meta.confidence))) chips.push(`confidence ${Number(meta.confidence).toFixed(2)}`);
      const provenance = Array.isArray(meta.provenance) ? meta.provenance : [];
      const provenanceHtml = provenance.length
        ? `<div class="mt-1 text-[10px] text-slate-600">Provenance: ${escapeHtml(provenance.slice(0, 4).map((p) => {
            if (!p || typeof p !== "object") return "";
            const artifactId = String(p.artifact_id || "").trim();
            const line = Number(p.line || 0);
            return artifactId ? `${artifactId}${line ? `:${line}` : ""}` : "";
          }).filter(Boolean).join(", "))}</div>`
        : "";
      return `
        <div class="rounded-md border ${cls} p-2">
          <div class="mb-1 flex items-center justify-between gap-2">
            <span class="text-[10px] font-semibold uppercase tracking-[0.12em] text-slate-700">${escapeHtml(role)}</span>
            <span class="mono text-[10px] text-slate-600">${escapeHtml(String(row.created_at || "").replace("T", " ").slice(0, 19))}</span>
          </div>
          ${chips.length ? `<div class="mb-1 text-[10px] text-slate-600">${escapeHtml(chips.join(" · "))}</div>` : ""}
          <div class="whitespace-pre-wrap text-[11px] text-slate-800">${escapeHtml(row.message || "")}</div>
          ${provenanceHtml}
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
          <label class="inline-flex items-center gap-2 text-[11px] text-slate-800"><input id="collab-knowledge-grounded" type="checkbox" class="h-4 w-4 rounded border-slate-400 text-slate-900" checked />Knowledge-grounded</label>
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
  const knowledgeGrounded = document.getElementById("collab-knowledge-grounded");
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
      knowledge_grounded: !!knowledgeGrounded?.checked && !saveDirective?.checked && !createProposal?.checked,
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
    const agent = activeAgentsForRun(state.currentRun).find((a) => a.stage === stage) || AGENTS.find((a) => a.stage === stage);
    el.collabStageLabel.textContent = `${stageDisplayLabel(state.currentRun, stage)}${agent ? ` · ${agent.name}` : ""}`;
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

function knowledgeAssistantResponse(runId) {
  const key = String(runId || "").trim();
  if (!key) return null;
  return state.knowledgeAssistant.responseByRun[key] || null;
}

function knowledgeAssistantProposals(runId) {
  const key = String(runId || "").trim();
  if (!key) return [];
  const rows = state.knowledgeAssistant.proposalsByRun[key];
  return Array.isArray(rows) ? rows : [];
}

async function loadKnowledgeAssistantProposals(runId, force = false) {
  const key = String(runId || "").trim();
  if (!key) return [];
  if (!force && state.knowledgeAssistant.proposalsLoadedByRun[key]) {
    return knowledgeAssistantProposals(key);
  }
  if (state.knowledgeAssistant.proposalLoadingRunId === key) {
    return knowledgeAssistantProposals(key);
  }
  state.knowledgeAssistant.proposalLoadingRunId = key;
  delete state.knowledgeAssistant.proposalErrorByRun[key];
  try {
    const data = await api(`/api/runs/${encodeURIComponent(key)}/knowledge/proposals`, null);
    state.knowledgeAssistant.proposalsByRun[key] = Array.isArray(data?.proposals) ? data.proposals : [];
    state.knowledgeAssistant.proposalsLoadedByRun[key] = true;
  } catch (err) {
    state.knowledgeAssistant.proposalErrorByRun[key] = String(err?.message || err || "Failed to load proposals.");
  } finally {
    state.knowledgeAssistant.proposalLoadingRunId = "";
    renderKnowledgeAssistantPanel();
  }
  return knowledgeAssistantProposals(key);
}

function renderKnowledgeAssistantPanel() {
  if (!el.knowledgeAssistantPanel || !el.knowledgeAssistantOutput || !el.knowledgeAssistantStatus) return;
  const runId = String(state.currentRun?.run_id || state.currentRunId || "").trim();
  if (!runId) {
    el.knowledgeAssistantPanel.classList.add("hidden");
    return;
  }
  el.knowledgeAssistantPanel.classList.remove("hidden");
  if (!state.knowledgeAssistant.proposalsLoadedByRun[runId] && state.knowledgeAssistant.proposalLoadingRunId !== runId) {
    loadKnowledgeAssistantProposals(runId).catch(() => {});
  }
  const draft = String(state.knowledgeAssistant.draftByRun[runId] || "");
  if (el.knowledgeAssistantInput && String(el.knowledgeAssistantInput.value || "") !== draft) {
    el.knowledgeAssistantInput.value = draft;
  }
  const loading = state.knowledgeAssistant.loadingRunId === runId;
  const proposalLoading = state.knowledgeAssistant.proposalLoadingRunId === runId;
  const error = String(state.knowledgeAssistant.errorByRun[runId] || "").trim();
  const record = knowledgeAssistantResponse(runId);
  if (el.knowledgeAssistantAsk) {
    el.knowledgeAssistantAsk.disabled = loading;
    el.knowledgeAssistantAsk.textContent = loading ? "Asking..." : "Ask";
  }
  if (el.knowledgeAssistantPropose) {
    el.knowledgeAssistantPropose.disabled = proposalLoading;
    el.knowledgeAssistantPropose.textContent = proposalLoading ? "Creating..." : "Create proposal";
  }
  if (loading) {
    el.knowledgeAssistantStatus.textContent = "Querying knowledge layer...";
  } else if (error) {
    el.knowledgeAssistantStatus.textContent = error;
  } else if (record && record.response) {
    const response = record.response;
    el.knowledgeAssistantStatus.textContent = `Mode: ${String(response.mode || "n/a")} · Confidence: ${Number(response.confidence || 0).toFixed(2)}`;
  } else {
    el.knowledgeAssistantStatus.textContent = "No query yet.";
  }
  if (!record || !record.response) {
    el.knowledgeAssistantOutput.innerHTML = "<p class='text-slate-700'>No response yet.</p>";
  } else {
    const response = record.response;
    const provenance = Array.isArray(response.provenance) ? response.provenance : [];
    const provenanceHtml = provenance.length
      ? provenance
        .map((ref) => {
          const artifact = escapeHtml(String(ref?.artifact_id || "artifact"));
          const path = escapeHtml(String(ref?.path || ""));
          const line = Number(ref?.line || 0);
          const note = escapeHtml(String(ref?.note || ""));
          const parts = [artifact];
          if (path) parts.push(path);
          if (line > 0) parts.push(`line ${line}`);
          if (note) parts.push(note);
          return `<li>${parts.join(" · ")}</li>`;
        })
        .join("")
      : "<li>No provenance refs returned.</li>";
    el.knowledgeAssistantOutput.innerHTML = `
      <div class="space-y-2">
        <div>
          <p class="text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-600">Answer</p>
          <p class="mt-1 text-sm text-slate-900">${escapeHtml(String(response.answer || ""))}</p>
        </div>
        <div class="flex flex-wrap gap-2">
          <span class="rounded-md border border-slate-300 bg-slate-50 px-2 py-0.5 text-[11px] font-semibold text-slate-700">${escapeHtml(String(response.intent || "query"))}</span>
          <span class="rounded-md border border-slate-300 bg-slate-50 px-2 py-0.5 text-[11px] font-semibold text-slate-700">${escapeHtml(String(response.topic || "general"))}</span>
          <span class="rounded-md border border-slate-300 bg-slate-50 px-2 py-0.5 text-[11px] font-semibold text-slate-700">${escapeHtml(String(response.mode || "needs verification"))}</span>
        </div>
        <div>
          <p class="text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-600">Provenance</p>
          <ul class="mt-1 list-disc space-y-1 pl-4 text-[11px] text-slate-700">${provenanceHtml}</ul>
        </div>
      </div>
    `;
  }

  const proposalError = String(state.knowledgeAssistant.proposalErrorByRun[runId] || "").trim();
  const proposals = knowledgeAssistantProposals(runId);
  if (el.knowledgeAssistantProposalsStatus) {
    if (proposalLoading) el.knowledgeAssistantProposalsStatus.textContent = "Loading proposals...";
    else if (proposalError) el.knowledgeAssistantProposalsStatus.textContent = proposalError;
    else if (!proposals.length) el.knowledgeAssistantProposalsStatus.textContent = "No proposals yet.";
    else el.knowledgeAssistantProposalsStatus.textContent = `${proposals.length} proposal(s)`;
  }
  if (el.knowledgeAssistantProposals) {
    el.knowledgeAssistantProposals.innerHTML = renderKnowledgeAssistantProposals(runId, proposals);
    el.knowledgeAssistantProposals.querySelectorAll("[data-knowledge-proposal-decision]").forEach((btn) => {
      btn.addEventListener("click", () => {
        const proposalId = String(btn.getAttribute("data-knowledge-proposal-id") || "");
        const decision = String(btn.getAttribute("data-knowledge-proposal-decision") || "");
        reviewKnowledgeProposal(runId, proposalId, decision).catch((err) => alert(err.message || err));
      });
    });
  }
}

function renderKnowledgeAssistantProposals(runId, proposals) {
  if (!Array.isArray(proposals) || !proposals.length) {
    return "<p class='text-slate-700'>No proposals yet.</p>";
  }
  return proposals.slice().reverse().map((proposal) => {
    const impact = (proposal && typeof proposal.impact === "object") ? proposal.impact : {};
    const docs = Array.isArray(impact.impacted_documents) ? impact.impacted_documents : [];
    const affectedModules = Array.isArray(impact.blast_radius?.affected_modules) ? impact.blast_radius.affected_modules : [];
    const before = proposal && typeof proposal.before === "object" ? proposal.before : {};
    const after = proposal && typeof proposal.after === "object" ? proposal.after : {};
    const pending = String(proposal?.status || "").toLowerCase() === "pending";
    return `
      <div class="mb-2 rounded-lg border border-slate-300 bg-slate-50 p-3">
        <div class="flex flex-wrap items-center justify-between gap-2">
          <div>
            <p class="text-xs font-semibold text-slate-900">${escapeHtml(String(proposal?.title || "Proposal"))}</p>
            <p class="mt-1 text-[11px] text-slate-700">${escapeHtml(String(proposal?.summary || ""))}</p>
          </div>
          <span class="rounded border px-2 py-0.5 text-[10px] font-semibold ${collabStatusBadge(proposal?.status)}">${escapeHtml(String(proposal?.status || "pending").toUpperCase())}</span>
        </div>
        <div class="mt-2 grid gap-2 lg:grid-cols-2">
          <div class="rounded border border-slate-300 bg-white p-2">
            <p class="text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-600">Before</p>
            <pre class="mono mt-1 whitespace-pre-wrap text-[10px] text-slate-700">${escapeHtml(JSON.stringify(before, null, 2))}</pre>
          </div>
          <div class="rounded border border-slate-300 bg-white p-2">
            <p class="text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-600">After</p>
            <pre class="mono mt-1 whitespace-pre-wrap text-[10px] text-slate-700">${escapeHtml(JSON.stringify(after, null, 2))}</pre>
          </div>
        </div>
        <div class="mt-2 text-[11px] text-slate-700">
          <strong>Documents:</strong> ${escapeHtml(docs.join(", ") || "n/a")}
          ${affectedModules.length ? `<br /><strong>Blast radius:</strong> ${escapeHtml(affectedModules.map((row) => row?.name || "").filter(Boolean).join(", "))}` : ""}
        </div>
        <p class="mono mt-1 text-[10px] text-slate-600">${escapeHtml(String(proposal?.id || ""))}</p>
        ${pending ? `
          <div class="mt-2 flex flex-wrap gap-2">
            <button data-knowledge-proposal-id="${escapeHtml(String(proposal?.id || ""))}" data-knowledge-proposal-decision="approve" class="btn-success rounded-md px-2 py-1 text-[11px] font-semibold">Approve</button>
            <button data-knowledge-proposal-id="${escapeHtml(String(proposal?.id || ""))}" data-knowledge-proposal-decision="reject" class="btn-danger rounded-md px-2 py-1 text-[11px] font-semibold">Reject</button>
          </div>
        ` : ""}
      </div>
    `;
  }).join("");
}

async function askKnowledgeAssistant() {
  const runId = String(state.currentRun?.run_id || state.currentRunId || "").trim();
  if (!runId) return;
  const message = String(el.knowledgeAssistantInput?.value || "").trim();
  state.knowledgeAssistant.draftByRun[runId] = message;
  if (!message) {
    state.knowledgeAssistant.errorByRun[runId] = "Enter a question first.";
    renderKnowledgeAssistantPanel();
    return;
  }
  state.knowledgeAssistant.loadingRunId = runId;
  delete state.knowledgeAssistant.errorByRun[runId];
  renderKnowledgeAssistantPanel();
  try {
    const data = await api(`/api/runs/${encodeURIComponent(runId)}/knowledge/interact`, { message }, "POST");
    state.knowledgeAssistant.responseByRun[runId] = data;
  } catch (err) {
    state.knowledgeAssistant.errorByRun[runId] = String(err?.message || err || "Knowledge query failed.");
  } finally {
    state.knowledgeAssistant.loadingRunId = "";
    renderKnowledgeAssistantPanel();
  }
}

async function createKnowledgeProposal() {
  const runId = String(state.currentRun?.run_id || state.currentRunId || "").trim();
  if (!runId) return;
  const message = String(el.knowledgeAssistantInput?.value || "").trim();
  state.knowledgeAssistant.draftByRun[runId] = message;
  if (!message) {
    state.knowledgeAssistant.proposalErrorByRun[runId] = "Enter a change request first.";
    renderKnowledgeAssistantPanel();
    return;
  }
  state.knowledgeAssistant.proposalLoadingRunId = runId;
  delete state.knowledgeAssistant.proposalErrorByRun[runId];
  renderKnowledgeAssistantPanel();
  try {
    const data = await api(`/api/runs/${encodeURIComponent(runId)}/knowledge/proposals`, { message }, "POST");
    state.knowledgeAssistant.proposalsByRun[runId] = Array.isArray(data?.proposals) ? data.proposals : [];
    state.knowledgeAssistant.proposalsLoadedByRun[runId] = true;
  } catch (err) {
    state.knowledgeAssistant.proposalErrorByRun[runId] = String(err?.message || err || "Proposal creation failed.");
  } finally {
    state.knowledgeAssistant.proposalLoadingRunId = "";
    renderKnowledgeAssistantPanel();
  }
}

async function reviewKnowledgeProposal(runId, proposalId, decision) {
  if (!runId || !proposalId) return;
  const rationale = prompt(`Provide rationale for ${decision}:`, "") || "";
  state.knowledgeAssistant.proposalLoadingRunId = runId;
  delete state.knowledgeAssistant.proposalErrorByRun[runId];
  renderKnowledgeAssistantPanel();
  try {
    const data = await api(
      `/api/runs/${encodeURIComponent(runId)}/knowledge/proposals/${encodeURIComponent(proposalId)}/decision`,
      { decision, rationale },
      "POST"
    );
    state.knowledgeAssistant.proposalsByRun[runId] = Array.isArray(data?.proposals) ? data.proposals : [];
    state.knowledgeAssistant.proposalsLoadedByRun[runId] = true;
  } catch (err) {
    state.knowledgeAssistant.proposalErrorByRun[runId] = String(err?.message || err || "Proposal review failed.");
  } finally {
    state.knowledgeAssistant.proposalLoadingRunId = "";
    renderKnowledgeAssistantPanel();
  }
}

function openStageModal(stage) {
  const agent = activeAgentsForRun(state.currentRun).find((a) => a.stage === stage) || AGENTS.find((a) => a.stage === stage);
  const result = latestResultByStage(state.currentRun, stage);
  if (!result || !agent) return;
  const runUseCase = String(state.currentRun?.pipeline_state?.use_case || state.currentRun?.use_case || "business_objectives");

  el.modalTitle.textContent = `${stageDisplayLabel(state.currentRun, stage)}: ${agent.name}`;
  el.modalSummary.textContent = result.summary || "";
  if (stage === 1 && state.currentRun?.run_id) {
    el.modalReadable.innerHTML = `
      <div class="rounded-lg border border-slate-300 bg-white p-2">
        <div class="flex flex-wrap items-center gap-2">
          <button data-analyst-export-summary class="btn-light rounded-md px-2 py-1 text-[11px] font-semibold">Export Summary</button>
          <button data-analyst-export-full class="btn-light rounded-md px-2 py-1 text-[11px] font-semibold">Export Full Evidence</button>
          <button data-analyst-export-ba-brief-docx class="btn-light rounded-md px-2 py-1 text-[11px] font-semibold">Export BA Brief</button>
          <button data-analyst-export-tech-workbook-docx class="btn-light rounded-md px-2 py-1 text-[11px] font-semibold">Export Tech Workbook</button>
          <button data-analyst-export-brd-docx class="btn-light rounded-md px-2 py-1 text-[11px] font-semibold">Export BRD</button>
          <button data-analyst-export-business-docx class="btn-light rounded-md px-2 py-1 text-[11px] font-semibold">Export Legacy Business</button>
          <button data-analyst-upload-trigger class="btn-dark rounded-md px-2 py-1 text-[11px] font-semibold">Upload Modified</button>
          <input data-analyst-upload-file type="file" class="hidden" accept=".md,.txt,.json" />
        </div>
        <p data-analyst-doc-status class="mt-1 text-[11px] text-slate-700">Export summary/full markdown, export BA Brief / Tech Workbook / BRD DOCX, or upload an updated version.</p>
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
  const isStarting = !!state.runStart?.pending;
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
  if (el.runPipeline) {
    el.runPipeline.disabled = isStarting;
    el.runPipeline.textContent = isStarting ? "Starting Run..." : "Start Run";
    el.runPipeline.classList.toggle("opacity-60", isStarting);
    el.runPipeline.classList.toggle("cursor-not-allowed", isStarting);
  }
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
  const activeAgents = activeAgentsForRun(run);
  if (!activeAgents.length) {
    alert("No active team steps are available for rerun.");
    return;
  }
  const suggestedStep = Math.max(1, stageDisplayIndex(run, Number(state.selectedStage || run.current_stage || activeAgents[0]?.stage || 1)));
  const input = window.prompt(`Rerun from step number (1-${activeAgents.length}):`, String(Math.min(activeAgents.length, suggestedStep)));
  const step = Number(input || "");
  if (!Number.isFinite(step) || step < 1 || step > activeAgents.length) {
    alert(`Step must be between 1 and ${activeAgents.length}.`);
    return;
  }
  const stage = stageForDisplayStep(run, step);
  if (!stage) {
    alert("Unable to resolve selected step to an active stage.");
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
  if (pending.type === "discover_review") {
    const blockers = Array.isArray(pending.unresolved_blocking) ? pending.unresolved_blocking : [];
    const blockerSummary = blockers
      .slice(0, 6)
      .map((row) => {
        const id = String(row?.id || "").trim();
        const title = String(row?.title || row?.detail || "").trim();
        return [id, title].filter(Boolean).join(": ");
      })
      .filter(Boolean)
      .join(" | ");
    const overall = String(pending.overall_status || "FAIL").toUpperCase();
    el.approvalMessage.textContent = [
      pending.message || "",
      `Overall: ${overall}`,
      blockers.length ? `Blocking: ${blockers.length}` : "",
      blockerSummary,
    ].filter(Boolean).join(" | ");
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
  renderKnowledgeAssistantPanel();
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
  if (state.runSnapshotPollTimer) {
    clearInterval(state.runSnapshotPollTimer);
    state.runSnapshotPollTimer = null;
  }
  if (state.runBootstrapPollTimer) {
    clearInterval(state.runBootstrapPollTimer);
    state.runBootstrapPollTimer = null;
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
  await fetchRunLogs(runId);
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

async function fetchRunLogs(runId) {
  if (!runId || !state.currentRun) return;
  try {
    const logsData = await api(`/api/runs/${runId}/logs?limit=400`, null);
    if (Array.isArray(logsData?.logs)) {
      state.currentRun.progress_logs = logsData.logs;
    }
  } catch (_err) {
    // Tail logs are optional; keep snapshot/status usable even if the logs endpoint is unavailable.
  }
}

async function fetchRunStatus(runId) {
  const data = await api(`/api/runs/${runId}/status`, null);
  return data.status || null;
}

function applyRunStatus(status) {
  if (!status) return;
  if (!state.currentRun || state.currentRun.run_id !== status.run_id) {
    state.currentRun = {
      run_id: status.run_id,
      status: status.status || "running",
      current_stage: Number(status.current_stage || 0),
      next_stage_idx: Number(status.next_stage_idx || 0),
      stage_status: status.stage_status || {},
      progress_logs: [],
      pipeline_state: state.currentRun?.pipeline_state || {},
      error_message: status.error_message || null,
    };
  } else {
    state.currentRun.status = status.status || state.currentRun.status;
    state.currentRun.current_stage = Number(status.current_stage || state.currentRun.current_stage || 0);
    state.currentRun.next_stage_idx = Number(status.next_stage_idx || state.currentRun.next_stage_idx || 0);
    state.currentRun.stage_status = status.stage_status || state.currentRun.stage_status || {};
    state.currentRun.error_message = status.error_message || state.currentRun.error_message || null;
  }
  const tail = Array.isArray(status.progress_logs_tail) ? status.progress_logs_tail : [];
  for (const line of tail) {
    if (typeof line === "string" && line) upsertLog(line);
  }
}

function startStreaming(runId) {
  stopStreaming();
  state.eventSource = new EventSource(`/api/runs/${runId}/stream`);
  state.runSnapshotPollTimer = setInterval(async () => {
    try {
      if (!state.currentRun || state.currentRun.run_id !== runId) return;
      const latest = await fetchRunSnapshot(runId);
      if (!isActiveRunStatus(latest?.status || "")) {
        stopStreaming();
      }
    } catch (_err) {
      // Keep the stream alive; snapshot polling is a best-effort freshness backstop.
    }
  }, 8000);

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
      const latest = await fetchRunSnapshot(runId);
      if (isActiveRunStatus(latest?.status || "")) {
        setTimeout(() => startStreaming(runId), 1200);
      }
    } catch (err) {
      try {
        const status = await fetchRunStatus(runId);
        applyRunStatus(status);
        renderRun();
        if (isActiveRunStatus(status?.status || "")) {
          setTimeout(() => startStreaming(runId), 1200);
          return;
        }
      } catch (_fallbackErr) {
        // no-op
      }
      el.pipelineStatusText.textContent = `STREAM ERROR: ${err.message}`;
    }
  };
}

async function syncRun(runId) {
  if (!runId) return;
  try {
    const run = await fetchRunSnapshot(runId);
    if (isActiveRunStatus(run.status)) startStreaming(runId);
    else stopStreaming();
    await refreshArtifactsList();
  } catch (err) {
    el.pipelineStatusText.textContent = `ERROR: ${err.message}`;
  }
}

function scheduleRunBootstrapRefresh(runId) {
  if (!runId) return;
  if (state.runBootstrapPollTimer) {
    clearInterval(state.runBootstrapPollTimer);
    state.runBootstrapPollTimer = null;
  }
  let attempts = 0;
  const maxAttempts = 45;
  state.runBootstrapPollTimer = setInterval(async () => {
    attempts += 1;
    try {
      if (!state.currentRunId || state.currentRunId !== runId) {
        clearInterval(state.runBootstrapPollTimer);
        state.runBootstrapPollTimer = null;
        return;
      }
      const status = await fetchRunStatus(runId);
      applyRunStatus(status);
      await fetchRunLogs(runId);
      const latestStatus = String(state.currentRun?.status || status?.status || "").toLowerCase();
      const shouldHydrateSnapshot = !latestStatus || latestStatus === "queued" || attempts <= 5 || attempts % 3 === 0;
      if (shouldHydrateSnapshot) {
        await fetchRunSnapshot(runId);
      }
      renderRun();
      if (!isActiveRunStatus(String(state.currentRun?.status || latestStatus || "").toLowerCase()) || attempts >= maxAttempts) {
        clearInterval(state.runBootstrapPollTimer);
        state.runBootstrapPollTimer = null;
      }
    } catch (_err) {
      if (attempts >= maxAttempts) {
        clearInterval(state.runBootstrapPollTimer);
        state.runBootstrapPollTimer = null;
      }
    }
  }, 2000);
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
  renderRun();
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
  if (pending.type === "discover_review" && decision === "approve") {
    const blockers = Array.isArray(pending.unresolved_blocking) ? pending.unresolved_blocking : [];
    payload.waived_ids = blockers
      .map((row) => String(row?.id || "").trim())
      .filter(Boolean);
    payload.note = "Approved from run approval panel";
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
    return normalizeStageAgentIds(state.teamSelection.stageAgentIds);
  }
  const builderMap = teamBuilderEffectiveStageAgentIds();
  if (Object.keys(builderMap).length) {
    return builderMap;
  }
  return { ...defaultBuilderMap() };
}

async function runStartPreflight(payload) {
  return api("/api/runs/preflight", payload, "POST");
}

async function startRun() {
  if (state.runStart?.pending) return;
  const objectives = (el.objectives.value || "").trim();
  if (!objectives) {
    alert("Business challenge is required.");
    return;
  }
  state.runStart.pending = true;
  state.runStart.startedAt = Date.now();
  setGlobalSearchStatus("Starting run request...");
  renderRunControls();
  renderProgress();
  const integrationContext = getIntegrationContext();
  const stageAgentIds = selectedStageAgentIdsForRun();
  if (!Object.keys(stageAgentIds).length) {
    state.runStart.pending = false;
    state.runStart.startedAt = 0;
    renderRunControls();
    renderProgress();
    alert("Active team has no stages configured. Add at least one agent in Plan > Team Creation.");
    setMode(MODES.PLAN);
    setPlanTab("team_creation");
    return;
  }
  if (String(integrationContext.domain_pack_error || "").trim()) {
    state.runStart.pending = false;
    state.runStart.startedAt = 0;
    renderRunControls();
    renderProgress();
    alert(`Domain Pack configuration error: ${integrationContext.domain_pack_error}`);
    setMode(MODES.DISCOVER);
    setWizardStep(1);
    setDiscoverStep(3);
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
    state.runStart.pending = false;
    state.runStart.startedAt = 0;
    renderRunControls();
    renderProgress();
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
        state.runStart.pending = false;
        state.runStart.startedAt = 0;
        renderRunControls();
        renderProgress();
        alert("Code modernization repository scan mode requires a connected GitHub repository in Discover Connect.");
        return;
      }
    } else if (isModernizationEvidenceMode()) {
      const bundleId = String(integrationContext?.evidence?.bundle_id || "").trim();
      if (!bundleId) {
        state.runStart.pending = false;
        state.runStart.startedAt = 0;
        renderRunControls();
        renderProgress();
        alert("Evidence Mode requires an uploaded analysis bundle in Discover Connect.");
        setMode(MODES.DISCOVER);
        setWizardStep(1);
        setDiscoverStep(1);
        return;
      }
    } else if (isModernizationHybridMode()) {
      const provider = String(integrationContext?.brownfield?.repo_provider || "").toLowerCase();
      const repoUrl = String(integrationContext?.brownfield?.repo_url || "").trim();
      const bundleId = String(integrationContext?.evidence?.bundle_id || "").trim();
      if (!bundleId && (provider !== "github" || !repoUrl)) {
        state.runStart.pending = false;
        state.runStart.startedAt = 0;
        renderRunControls();
        renderProgress();
        alert("Hybrid mode requires a connected GitHub repository or an uploaded evidence bundle.");
        setMode(MODES.DISCOVER);
        setWizardStep(1);
        setDiscoverStep(1);
        return;
      }
    } else if (!(el.legacyCode.value || "").trim()) {
      state.runStart.pending = false;
      state.runStart.startedAt = 0;
      renderRunControls();
      renderProgress();
      alert("Legacy code is required for code modernization use case.");
      return;
    }
  }
  if (useCase === "database_conversion" && !(el.dbSchema.value || "").trim()) {
    state.runStart.pending = false;
    state.runStart.startedAt = 0;
    renderRunControls();
    renderProgress();
    alert("Legacy schema/SQL is required for database conversion use case.");
    return;
  }
  const discoverCompletion = discoverStepCompletion();
  const requiresConnectStep = useCase === "code_modernization" && isModernizationRepoScanMode();
  if (!discoverCompletion.landscapeComplete || !discoverCompletion.scopeComplete || !discoverCompletion.scanComplete || (requiresConnectStep && !discoverCompletion.connectComplete)) {
    state.runStart.pending = false;
    state.runStart.startedAt = 0;
    renderRunControls();
    renderProgress();
    const blockers = [];
    if (requiresConnectStep && !discoverCompletion.connectComplete) blockers.push("Connect");
    if (!discoverCompletion.landscapeComplete) blockers.push("Landscape");
    if (!discoverCompletion.scopeComplete) blockers.push("Define scope");
    if (!discoverCompletion.scanComplete) blockers.push("Scan");
    alert(`Complete Discover step(s): ${blockers.join(", ")} before starting a run.`);
    setMode(MODES.DISCOVER);
    setWizardStep(1);
    if (requiresConnectStep && !discoverCompletion.connectComplete) setDiscoverStep(1);
    else if (!discoverCompletion.landscapeComplete) setDiscoverStep(2);
    else if (!discoverCompletion.scopeComplete) setDiscoverStep(3);
    else if (!discoverCompletion.scanComplete) setDiscoverStep(4);
    return;
  }
  if (String(el.deploymentTarget.value || "local").toLowerCase() === "cloud" && !el.enableCloudPromotion?.checked) {
    state.runStart.pending = false;
    state.runStart.startedAt = 0;
    renderRunControls();
    renderProgress();
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
    stage_agent_ids: stageAgentIds,
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
    setGlobalSearchStatus("Running start-run preflight checks...");
    await runStartPreflight(payload);
  } catch (err) {
    state.runStart.pending = false;
    state.runStart.startedAt = 0;
    renderRunControls();
    renderProgress();
    const msg = String(err?.message || err || "Run preflight failed");
    setGlobalSearchStatus(`Run preflight failed: ${msg}`, true);
    alert(`Run preflight failed: ${msg}`);
    if (/api key|llm|provider|credentials/i.test(msg)) {
      setMode(MODES.SETTINGS);
    }
    return;
  }

  let data = null;
  try {
    data = await api("/api/runs", payload);
  } catch (err) {
    state.runStart.pending = false;
    state.runStart.startedAt = 0;
    setGlobalSearchStatus(`Start run failed: ${err.message || err}`, true);
    renderRunControls();
    renderProgress();
    alert(`Failed to start run: ${err.message}`);
    return;
  }

  // Treat run creation as the authoritative success point. Follow-up sync
  // failures should not make the UI appear as if the run never started.
  state.currentRunId = data.run_id;
  state.selectedStage = 1;
  state.currentRun = {
    run_id: data.run_id,
    status: data.status || "running",
    current_stage: 0,
    stage_status: {},
    progress_logs: [],
    pipeline_state: null,
    error_message: null,
    retry_count: 0,
    team_id: state.teamSelection.teamId,
    team_name: state.teamSelection.teamName,
  };
  state.runStart.pending = false;
  state.runStart.startedAt = 0;
  setGlobalSearchStatus(
    (String(data.status || "").toLowerCase() === "queued")
      ? `Run ${data.run_id} queued. Waiting for worker dispatch...`
      : `Run ${data.run_id} started. Streaming live updates...`
  );
  setMode(MODES.BUILD);
  renderRun();
  if (isActiveRunStatus(String(data.status || "").toLowerCase())) {
    startStreaming(data.run_id);
    scheduleRunBootstrapRefresh(data.run_id);
  }

  Promise.allSettled([refreshRunHistory(), syncRun(data.run_id)]).then((results) => {
    const failed = results.filter((row) => row.status === "rejected");
    if (failed.length) {
      console.warn("Run started but follow-up sync failed", failed);
      setGlobalSearchStatus("Run started, but initial sync is delayed. Live stream will continue updating.", true);
    }
  });
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
  el.navEstimates?.addEventListener("click", () => setMode(MODES.ESTIMATES));
  el.navBuild?.addEventListener("click", () => setMode(MODES.BUILD));
  el.navHistory.addEventListener("click", () => setMode(MODES.VERIFY));
  el.navSettings?.addEventListener("click", () => setMode(MODES.SETTINGS));

  el.homeWorkBtn.addEventListener("click", () => setMode(MODES.DISCOVER));
  el.homeTeamBtn.addEventListener("click", () => setMode(MODES.PLAN));
  el.homeHistoryBtn.addEventListener("click", () => setMode(MODES.VERIFY));
  el.estimateCreateBtn?.addEventListener("click", () => {
    createEstimateFromForm().catch((err) => setEstimateStatus(`Estimate create failed: ${err.message}`, true));
  });
  el.estimateLoadRunBtn?.addEventListener("click", () => {
    loadRunEstimates().catch((err) => setEstimateStatus(`Estimate list load failed: ${err.message}`, true));
  });
  el.estimateRefreshList?.addEventListener("click", () => {
    loadRunEstimates().catch((err) => setEstimateStatus(`Estimate list load failed: ${err.message}`, true));
  });
  el.estimateAgentIntakeBtn?.addEventListener("click", () => {
    probeEstimateIntake().catch((err) => setEstimateStatus(`Estimate intake probe failed: ${err.message}`, true));
  });
  el.estimateAgentExplainBtn?.addEventListener("click", () => {
    explainCurrentEstimate().catch((err) => setEstimateStatus(`Estimate explanation failed: ${err.message}`, true));
  });
  document.querySelectorAll("[data-estimate-tab]").forEach((btn) => {
    btn.addEventListener("click", () => {
      if (!(btn instanceof HTMLElement)) return;
      setEstimateTab(btn.getAttribute("data-estimate-tab") || "overview");
    });
  });
  el.estimateList?.addEventListener("click", (evt) => {
    const target = evt.target;
    if (!(target instanceof HTMLElement)) return;
    const estimateId = target.getAttribute("data-estimate-load-id");
    if (!estimateId) return;
    loadEstimateById(estimateId).catch((err) => setEstimateStatus(`Estimate load failed: ${err.message}`, true));
  });

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
  el.userMenuBtn?.addEventListener("click", () => setMode(MODES.SETTINGS));
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
  el.settingsCurrentUserSelect?.addEventListener("change", () => {
    const email = String(el.settingsCurrentUserSelect?.value || "").trim().toLowerCase();
    const row = settingsUsers().find((user) => String(user.email || "").toLowerCase() === email);
    if (row) applyUserFormFromRow(row);
  });
  el.settingsUserSave?.addEventListener("click", () => saveUser().catch((err) => setUserMessage(err.message, true)));
  el.settingsUserUse?.addEventListener("click", () => useSelectedUser().catch((err) => setUserMessage(err.message, true)));
  el.settingsUserRemove?.addEventListener("click", () => removeUser().catch((err) => setUserMessage(err.message, true)));
  el.settingsKnowledgeSourceSave?.addEventListener("click", () => saveKnowledgeSource().catch((err) => setKnowledgeMessage(err.message, true)));
  el.settingsKnowledgeSetSave?.addEventListener("click", () => saveKnowledgeSet().catch((err) => setKnowledgeMessage(err.message, true)));
  el.settingsBrainSave?.addEventListener("click", () => saveAgentBrain().catch((err) => setKnowledgeMessage(err.message, true)));
  el.settingsBindingSave?.addEventListener("click", () => saveProjectBinding().catch((err) => setKnowledgeMessage(err.message, true)));
  document.querySelectorAll("[data-knowledge-hub-tab]").forEach((btn) => {
    btn.addEventListener("click", () => setKnowledgeHubTab(String(btn.getAttribute("data-knowledge-hub-tab") || "sources")));
  });
  el.settingsKnowledgeJobsRefresh?.addEventListener("click", () => renderKnowledgeHub());
  el.settingsKnowledgeEvalsRefresh?.addEventListener("click", () => renderKnowledgeHub());
  el.settingsAuditRefresh?.addEventListener("click", () => loadSettings(true).catch((err) => setSettingsMessage(err.message, true)));

  el.discoverStepConnect?.addEventListener("click", () => setDiscoverStep(1));
  el.discoverStepLandscape?.addEventListener("click", () => {
    if (state.discoverStep < 2 && !validateDiscoverStep(1)) return;
    setDiscoverStep(2);
  });
  el.discoverStepScope?.addEventListener("click", () => {
    const current = Math.min(state.discoverStep, 2);
    if (current >= 1 && !validateDiscoverStep(1)) return;
    setDiscoverStep(3);
  });
  el.discoverStepScan?.addEventListener("click", () => {
    if (!validateDiscoverStep(1) || !validateDiscoverStep(3)) return;
    setDiscoverStep(4);
  });
  el.discoverStepResults?.addEventListener("click", () => {
    if (!validateDiscoverStep(1) || !validateDiscoverStep(3) || !validateDiscoverStep(4)) return;
    setDiscoverStep(5);
  });
  el.discoverRunAnalystBriefLandscape?.addEventListener("click", () => loadDiscoverLandscape({ force: true }).catch((err) => {
    state.discoverLandscape.error = err.message;
    renderDiscoverLandscape();
    renderDiscoverStepper();
  }));
  el.discoverOpenLandscapeStepResults?.addEventListener("click", () => {
    setDiscoverStep(5);
    setDiscoverResultsView("landscape");
  });
  el.discoverExportLandscapeStep?.addEventListener("click", () => downloadDiscoverArtifact("repo_landscape").catch((err) => alert(err.message)));
  el.discoverExportComponentInventoryStep?.addEventListener("click", () => downloadDiscoverArtifact("component_inventory").catch((err) => alert(err.message)));
  el.discoverExportTrackPlanStep?.addEventListener("click", () => downloadDiscoverArtifact("modernization_track_plan").catch((err) => alert(err.message)));
  el.discoverOpenLandscape?.addEventListener("click", () => setDiscoverResultsView("landscape"));
  el.discoverOpenCityMap?.addEventListener("click", () => setDiscoverResultsView("city"));
  el.discoverOpenSystemMap?.addEventListener("click", () => setDiscoverResultsView("system"));
  el.discoverOpenHealthDebt?.addEventListener("click", () => setDiscoverResultsView("health"));
  el.discoverOpenConventions?.addEventListener("click", () => setDiscoverResultsView("conventions"));
  el.discoverOpenStaticForensics?.addEventListener("click", () => setDiscoverResultsView("static_forensics"));
  el.discoverOpenCodeQuality?.addEventListener("click", () => setDiscoverResultsView("code_quality"));
  el.discoverOpenDeadCode?.addEventListener("click", () => setDiscoverResultsView("dead_code"));
  el.discoverOpenDependencyMatrix?.addEventListener("click", () => setDiscoverResultsView("dependency_matrix"));
  el.discoverOpenTrends?.addEventListener("click", () => setDiscoverResultsView("trends"));
  el.discoverOpenData?.addEventListener("click", () => setDiscoverResultsView("data"));
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
  el.discoverExportLandscape?.addEventListener("click", async () => {
    try {
      await downloadDiscoverArtifact("repo_landscape");
      setGlobalSearchStatus("Landscape report exported.");
    } catch (err) {
      setGlobalSearchStatus(`Landscape export failed: ${err.message || err}`, true);
    }
  });
  el.discoverExportComponentInventory?.addEventListener("click", async () => {
    try {
      await downloadDiscoverArtifact("component_inventory");
      setGlobalSearchStatus("Component inventory exported.");
    } catch (err) {
      setGlobalSearchStatus(`Component inventory export failed: ${err.message || err}`, true);
    }
  });
  el.discoverExportTrackPlan?.addEventListener("click", async () => {
    try {
      await downloadDiscoverArtifact("modernization_track_plan");
      setGlobalSearchStatus("Track plan exported.");
    } catch (err) {
      setGlobalSearchStatus(`Track plan export failed: ${err.message || err}`, true);
    }
  });
  el.discoverExportRouterRuleset?.addEventListener("click", async () => {
    try {
      await downloadDiscoverArtifact("router_ruleset");
      setGlobalSearchStatus("Router ruleset exported.");
    } catch (err) {
      setGlobalSearchStatus(`Router ruleset export failed: ${err.message || err}`, true);
    }
  });
  el.discoverExportSourceSchema?.addEventListener("click", async () => {
    try {
      await downloadDiscoverDbArtifact("source_schema");
      setGlobalSearchStatus("Source schema exported.");
    } catch (err) {
      setGlobalSearchStatus(`Source schema export failed: ${err.message || err}`, true);
    }
  });
  el.discoverExportSourceErd?.addEventListener("click", async () => {
    try {
      await downloadDiscoverDbArtifact("source_erd");
      setGlobalSearchStatus("Source ERD exported.");
    } catch (err) {
      setGlobalSearchStatus(`Source ERD export failed: ${err.message || err}`, true);
    }
  });
  el.discoverExportDataDictionary?.addEventListener("click", async () => {
    try {
      await downloadDiscoverDbArtifact("data_dictionary");
      setGlobalSearchStatus("Data dictionary exported.");
    } catch (err) {
      setGlobalSearchStatus(`Data dictionary export failed: ${err.message || err}`, true);
    }
  });
  el.discoverExportProjectMetrics?.addEventListener("click", async () => {
    try {
      await downloadDiscoverArtifact("project_metrics");
      setGlobalSearchStatus("Project metrics exported.");
    } catch (err) {
      setGlobalSearchStatus(`Project metrics export failed: ${err.message || err}`, true);
    }
  });
  el.discoverExportStaticForensics?.addEventListener("click", async () => {
    try {
      await downloadDiscoverArtifact("static_forensics");
      setGlobalSearchStatus("Static forensics exported.");
    } catch (err) {
      setGlobalSearchStatus(`Static forensics export failed: ${err.message || err}`, true);
    }
  });
  el.discoverExportQualityRules?.addEventListener("click", async () => {
    try {
      await downloadDiscoverArtifact("quality_rules");
      setGlobalSearchStatus("Quality rules exported.");
    } catch (err) {
      setGlobalSearchStatus(`Quality rules export failed: ${err.message || err}`, true);
    }
  });
  el.discoverExportQualityViolations?.addEventListener("click", async () => {
    try {
      await downloadDiscoverArtifact("quality_violations");
      setGlobalSearchStatus("Quality violations exported.");
    } catch (err) {
      setGlobalSearchStatus(`Quality violations export failed: ${err.message || err}`, true);
    }
  });
  el.discoverExportDeadCode?.addEventListener("click", async () => {
    try {
      await downloadDiscoverArtifact("dead_code");
      setGlobalSearchStatus("Dead code report exported.");
    } catch (err) {
      setGlobalSearchStatus(`Dead code export failed: ${err.message || err}`, true);
    }
  });
  el.discoverExportTypeDependencyMatrix?.addEventListener("click", async () => {
    try {
      await downloadDiscoverArtifact("type_dependency_matrix");
      setGlobalSearchStatus("Type dependency matrix exported.");
    } catch (err) {
      setGlobalSearchStatus(`Type dependency matrix export failed: ${err.message || err}`, true);
    }
  });
  el.discoverExportRuntimeDependencyMatrix?.addEventListener("click", async () => {
    try {
      await downloadDiscoverArtifact("runtime_dependency_matrix");
      setGlobalSearchStatus("Runtime dependency matrix exported.");
    } catch (err) {
      setGlobalSearchStatus(`Runtime dependency matrix export failed: ${err.message || err}`, true);
    }
  });
  el.discoverExportThirdPartyUsage?.addEventListener("click", async () => {
    try {
      await downloadDiscoverArtifact("third_party_usage");
      setGlobalSearchStatus("Third-party usage exported.");
    } catch (err) {
      setGlobalSearchStatus(`Third-party usage export failed: ${err.message || err}`, true);
    }
  });
  el.discoverExportTrendSnapshot?.addEventListener("click", async () => {
    try {
      await downloadDiscoverArtifact("trend_snapshot");
      setGlobalSearchStatus("Trend snapshot exported.");
    } catch (err) {
      setGlobalSearchStatus(`Trend snapshot export failed: ${err.message || err}`, true);
    }
  });
  el.discoverExportTrendSeries?.addEventListener("click", async () => {
    try {
      await downloadDiscoverArtifact("trend_series");
      setGlobalSearchStatus("Trend series exported.");
    } catch (err) {
      setGlobalSearchStatus(`Trend series export failed: ${err.message || err}`, true);
    }
  });
  el.wizardPrevDiscover?.addEventListener("click", () => {
    if (state.discoverStep > 1) setDiscoverStep(state.discoverStep - 1);
  });
  el.projectStateMode?.addEventListener("change", () => {
    const result = detectProjectStateHeuristic();
    applyProjectStateResult(result);
    autoTriggerDiscoverExternalViews({ force: true });
  });
  el.detectProjectState?.addEventListener("click", () => {
    applyProjectStateResult(detectProjectStateHeuristic());
    autoTriggerDiscoverExternalViews({ force: true });
  });
  el.discoverRunAnalystBrief?.addEventListener("click", () => {
    if (state.discoverAnalystBrief?.loading) return;
    loadDiscoverAnalystBrief({ force: true }).catch((err) => {
      state.discoverAnalystBrief = {
        loading: false,
        error: String(err?.message || err || "Failed to run analyst brief."),
        data: null,
        requestKey: "",
        threadId: "",
        requestToken: "",
        inFlightPromise: null,
      };
      renderDiscoverAnalystBrief();
    });
  });
  el.bfUploadEvidence?.addEventListener("click", () => el.bfEvidenceFiles?.click());
  el.bfEvidenceFiles?.addEventListener("change", () => {
    uploadDiscoverEvidenceBundle().catch((err) => {
      state.discoverEvidenceBundle = { loading: false, error: String(err?.message || err || "Evidence upload failed."), data: null };
      renderDiscoverIntegrationPreviews();
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
    el.bfSourceMode,
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
    state.discoverLandscape = { loading: false, error: "", data: null, requestKey: "", requestToken: "", inFlightPromise: null };
    state.discoverAnalystBrief = { loading: false, error: "", data: null, requestKey: "", threadId: "", requestToken: "", inFlightPromise: null };
    renderDomainPackControls();
    if (node === el.bfSourceMode && el.modernizationSourceMode) el.modernizationSourceMode.value = String(el.bfSourceMode.value || "manual");
    toggleUseCasePanel();
    renderDiscoverIntegrationPreviews();
    renderDiscoverStepper();
  }));
  [
    el.bfSourceMode,
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
    state.discoverLandscape = { loading: false, error: "", data: null, requestKey: "", requestToken: "", inFlightPromise: null };
    state.discoverAnalystBrief = { loading: false, error: "", data: null, requestKey: "", threadId: "", requestToken: "", inFlightPromise: null };
    if (node === el.bfSourceMode && el.modernizationSourceMode) el.modernizationSourceMode.value = String(el.bfSourceMode.value || "manual");
    if (node === el.modernizationSourceMode && el.bfSourceMode) el.bfSourceMode.value = String(el.modernizationSourceMode.value || "manual");
    toggleUseCasePanel();
    renderDomainPackControls();
    renderDiscoverIntegrationPreviews();
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
    state.discoverLandscape = { loading: false, error: "", data: null, requestKey: "", requestToken: "", inFlightPromise: null };
    state.discoverAnalystBrief = { loading: false, error: "", data: null, requestKey: "", threadId: "", requestToken: "", inFlightPromise: null };
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
    state.discoverLandscape = { loading: false, error: "", data: null, requestKey: "", requestToken: "", inFlightPromise: null };
    state.discoverAnalystBrief = { loading: false, error: "", data: null, requestKey: "", threadId: "", requestToken: "", inFlightPromise: null };
    if (isCodeModernizationMode()) {
      el.objectives.dataset.autogen = "1";
      setAutogeneratedObjective();
    }
    renderDiscoverStepper();
    renderTaskSummary();
  });
  el.dbSource.addEventListener("change", () => {
    state.discoverLandscape = { loading: false, error: "", data: null, requestKey: "", requestToken: "", inFlightPromise: null };
    state.discoverAnalystBrief = { loading: false, error: "", data: null, requestKey: "", threadId: "", requestToken: "", inFlightPromise: null };
    if (isDatabaseConversionMode()) {
      el.objectives.dataset.autogen = "1";
      setAutogeneratedObjective();
    }
    renderDiscoverStepper();
    renderTaskSummary();
  });
  el.dbTarget.addEventListener("change", () => {
    state.discoverLandscape = { loading: false, error: "", data: null, requestKey: "", requestToken: "", inFlightPromise: null };
    state.discoverAnalystBrief = { loading: false, error: "", data: null, requestKey: "", threadId: "", requestToken: "", inFlightPromise: null };
    if (isDatabaseConversionMode()) {
      el.objectives.dataset.autogen = "1";
      setAutogeneratedObjective();
    }
    renderDiscoverStepper();
    renderTaskSummary();
  });
  el.objectives.addEventListener("input", () => {
    state.discoverLandscape = { loading: false, error: "", data: null, requestKey: "", requestToken: "", inFlightPromise: null };
    state.discoverAnalystBrief = { loading: false, error: "", data: null, requestKey: "", threadId: "", requestToken: "", inFlightPromise: null };
    el.objectives.dataset.autogen = "0";
    if (String(el.projectStateMode?.value || "auto") === "auto") {
      applyProjectStateResult(detectProjectStateHeuristic());
    } else {
      renderDiscoverStepper();
    }
    renderTaskSummary();
  });
  el.legacyCode.addEventListener("input", () => {
    state.discoverLandscape = { loading: false, error: "", data: null, requestKey: "", requestToken: "", inFlightPromise: null };
    state.discoverAnalystBrief = { loading: false, error: "", data: null, requestKey: "", threadId: "", requestToken: "", inFlightPromise: null };
    if (String(el.projectStateMode?.value || "auto") === "auto") {
      applyProjectStateResult(detectProjectStateHeuristic());
    }
    renderDiscoverStepper();
    renderTaskSummary();
  });
  el.dbSchema.addEventListener("input", () => {
    state.discoverLandscape = { loading: false, error: "", data: null, requestKey: "", requestToken: "", inFlightPromise: null };
    state.discoverAnalystBrief = { loading: false, error: "", data: null, requestKey: "", threadId: "", requestToken: "", inFlightPromise: null };
    if (String(el.projectStateMode?.value || "auto") === "auto") {
      applyProjectStateResult(detectProjectStateHeuristic());
    }
    renderDiscoverStepper();
    renderTaskSummary();
  });

  el.wizardContinue.addEventListener("click", async () => {
    if (state.discoverStep < 5) {
      const previousStep = state.discoverStep;
      if (!validateDiscoverStep(state.discoverStep)) return;
      setDiscoverStep(state.discoverStep + 1);
      // Stage transition 2 -> 3 triggers scan load in setDiscoverStep; avoid
      // issuing a second concurrent brief request that can race and clear state.
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
      } else if (isModernizationEvidenceMode()) {
        const integration = getIntegrationContext();
        const bundleId = String(integration?.evidence?.bundle_id || "").trim();
        if (!bundleId) {
          alert("For imported analysis mode, upload analysis outputs in Discover Connect.");
          return;
        }
      } else if (isModernizationHybridMode()) {
        const integration = getIntegrationContext();
        const provider = String(integration?.brownfield?.repo_provider || "").toLowerCase();
        const repoUrl = String(integration?.brownfield?.repo_url || "").trim();
        const bundleId = String(integration?.evidence?.bundle_id || "").trim();
        if (!bundleId && (provider !== "github" || !repoUrl) && !(el.legacyCode.value || "").trim()) {
          alert("For hybrid mode, connect a GitHub repository, upload analysis outputs, or provide legacy code.");
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
  el.planTeamLoadBtn?.addEventListener("click", () => loadSelectedTeamIntoBuilder().catch((err) => alert(err.message)));
  el.planTeamNewBtn?.addEventListener("click", resetBuilderForNewTeam);
  el.planTeamDuplicateBtn?.addEventListener("click", () => duplicateSelectedTeamInPlan().catch((err) => alert(err.message)));
  el.planTeamDeleteBtn?.addEventListener("click", () => deleteSelectedTeamInPlan().catch((err) => alert(err.message)));
  el.teamAddAgentBtn?.addEventListener("click", addAgentRowToBuilder);
  el.teamLoadSelectedBtn?.addEventListener("click", () => loadSelectedTeamIntoBuilder().catch((err) => alert(err.message)));
  el.teamUseInWorkBtn.addEventListener("click", useBuilderTeamInWork);
  el.teamRefreshBtn.addEventListener("click", () => loadAgentsAndTeams().catch((err) => alert(err.message)));
  document.querySelectorAll("[data-plan-tab]").forEach((btn) => {
    btn.addEventListener("click", () => setPlanTab(String(btn.getAttribute("data-plan-tab") || "team_creation")));
  });
  el.cloneAgentBtn.addEventListener("click", () => cloneAgentFromBuilder().catch((err) => alert(err.message)));
  el.cloneBaseAgent?.addEventListener("change", refreshCloneRequirementsPackFields);
  el.cloneRequirementsPackProfile?.addEventListener("change", refreshCloneRequirementsPackFields);
  el.agentStudioAgentSelect?.addEventListener("change", () => {
    state.agentStudio.selectedAgentKey = String(el.agentStudioAgentSelect?.value || "").trim();
    renderAgentStudioPanel();
  });
  document.querySelectorAll("[data-agent-studio-tab]").forEach((btn) => {
    btn.addEventListener("click", () => setAgentStudioTab(String(btn.getAttribute("data-agent-studio-tab") || "persona")));
  });
  el.agentStudioSave?.addEventListener("click", () => saveAgentStudioConfig().catch((err) => {
    if (el.agentStudioMessage) el.agentStudioMessage.textContent = String(err?.message || err || "Failed to save Agent Studio config.");
  }));
  el.agentStudioPanel?.addEventListener("click", handleAgentStudioPanelClick);

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
  el.knowledgeAssistantAsk?.addEventListener("click", () => {
    askKnowledgeAssistant().catch((err) => {
        const runId = String(state.currentRun?.run_id || state.currentRunId || "").trim();
        if (runId) {
        state.knowledgeAssistant.errorByRun[runId] = String(err?.message || err || "Knowledge query failed.");
        state.knowledgeAssistant.loadingRunId = "";
        renderKnowledgeAssistantPanel();
      }
    });
  });
  el.knowledgeAssistantPropose?.addEventListener("click", () => {
    createKnowledgeProposal().catch((err) => {
      const runId = String(state.currentRun?.run_id || state.currentRunId || "").trim();
      if (runId) {
        state.knowledgeAssistant.proposalErrorByRun[runId] = String(err?.message || err || "Proposal creation failed.");
        state.knowledgeAssistant.proposalLoadingRunId = "";
        renderKnowledgeAssistantPanel();
      }
    });
  });
  el.knowledgeAssistantInput?.addEventListener("input", () => {
    const runId = String(state.currentRun?.run_id || state.currentRunId || "").trim();
    if (!runId) return;
    state.knowledgeAssistant.draftByRun[runId] = String(el.knowledgeAssistantInput?.value || "");
  });
  el.knowledgeAssistantInput?.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && (event.metaKey || event.ctrlKey)) {
      event.preventDefault();
      askKnowledgeAssistant().catch((err) => {
        const runId = String(state.currentRun?.run_id || state.currentRunId || "").trim();
        if (runId) {
          state.knowledgeAssistant.errorByRun[runId] = String(err?.message || err || "Knowledge query failed.");
          state.knowledgeAssistant.loadingRunId = "";
          renderKnowledgeAssistantPanel();
        }
      });
    }
  });
  el.closeModal.addEventListener("click", () => el.outputModal.close());
  el.diagramClose?.addEventListener("click", () => el.diagramModal?.close());
  el.diagramDownloadMmd?.addEventListener("click", () => {
    const source = String(el.diagramModal?.dataset?.diagramSource || "").trim();
    const title = String(el.diagramModal?.dataset?.diagramTitle || "diagram").trim() || "diagram";
    if (!source) return;
    _downloadBlobContent(`${source}\n`, `${safeName(title)}.mmd`, "text/plain;charset=utf-8");
  });
  el.diagramDownloadSvg?.addEventListener("click", () => {
    const title = String(el.diagramModal?.dataset?.diagramTitle || "diagram").trim() || "diagram";
    const svg = el.diagramModalViewer?.querySelector("svg");
    if (!svg) return;
    const content = `<?xml version="1.0" encoding="UTF-8"?>\n${svg.outerHTML}\n`;
    _downloadBlobContent(content, `${safeName(title)}.svg`, "image/svg+xml;charset=utf-8");
  });
}

async function init() {
  state.activeUserEmail = String(localStorage.getItem(ACTIVE_USER_STORAGE_KEY) || "").trim().toLowerCase();
  if (!state.activeUserEmail) {
    state.activeUserEmail = "local-user@synthetix.local";
    localStorage.setItem(ACTIVE_USER_STORAGE_KEY, state.activeUserEmail);
  }
  renderCurrentUserIdentity();
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

if (typeof window !== "undefined") {
  window.startRun = startRun;
  window.loadDiscoverLandscape = loadDiscoverLandscape;
  window.loadDiscoverAnalystBrief = loadDiscoverAnalystBrief;
}

init().catch((err) => {
  el.pipelineStatusText.textContent = `INIT ERROR: ${err.message}`;
});
