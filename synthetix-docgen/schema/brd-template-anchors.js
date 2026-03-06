'use strict';

const DEFAULT_ANCHOR_MAP = {
  id: 'synthetix_default_v1',
  family: 'default',
  section_titles: {
    cover: 'Business Requirements Document (BRD)',
    version_history: 'Version History',
    module_inventory: 'Module Inventory',
    context: 'Introduction and Context',
    project_description: 'Project Description',
    general_requirements: 'General Requirements',
    modules: 'Module Details',
    appendices: 'Appendices',
  },
  table_headers: {
    version_history: ['Version', 'Date', 'Author', 'Summary', 'Status'],
    module_inventory: ['Module ID', 'Business Name', 'State Key', 'Kind', 'Description', 'Confidence', 'Source Forms'],
    business_rules: ['Feature ID', 'Rule ID', 'Title', 'Statement', 'Error Message', 'Rationale', 'Priority'],
    display_requirements: ['Feature ID', 'Display ID', 'Title', 'Requirement'],
    field_definitions: ['Feature ID', 'Field ID', 'Label', 'Business Meaning', 'Required', 'Validation', 'Source'],
    user_stories: ['Feature ID', 'Story ID', 'As a', 'I want', 'So that'],
    acceptance_criteria: ['Feature ID', 'AC ID', 'Statement', 'Linked Story'],
  },
};

const JHA_OPENANYWHERE_ANCHOR_MAP = {
  id: 'jha_openanywhere_v1',
  family: 'JHA_OpenAnywhere_v1',
  section_titles: {
    cover: 'Business Requirements Document (BRD)',
    version_history: 'Version History [Software]',
    module_inventory: 'Module Name from Code',
    context: 'Introduction',
    project_description: 'Project Description',
    general_requirements: 'General Requirements',
    modules: 'Workflow Modules',
    appendices: 'System Requirements / Software Requirements / Other Code Files to Rewrite',
  },
  table_headers: {
    version_history: ['Srl. No.', 'Version No.', 'Author / Owner', 'Date of Publishing', 'Description of Release/Change', 'Approved By'],
    module_inventory: ['Module Name from Code', 'Module ID', 'STATE KEY NAME', 'Module Description (Why & How?)'],
    business_rules: ['Feature ID', 'Business Rule ID', 'Rule Title', 'Business Rule Statement', 'Error Message', 'Rationale', 'Priority'],
    display_requirements: ['Feature ID', 'Display ID', 'Display Requirement', 'Details'],
    field_definitions: ['Feature ID', 'Field ID', 'Field Label', 'Business Meaning', 'Required', 'Validation Rule', 'Source Refs'],
    user_stories: ['Feature ID', 'Story ID', 'As a', 'I want', 'So that'],
    acceptance_criteria: ['Feature ID', 'Acceptance ID', 'Statement', 'Linked Story'],
  },
};

function normalizeFamily(value) {
  return String(value || '').trim().toLowerCase().replace(/[^a-z0-9]+/g, '_');
}

function getTemplateAnchorMap(templateFamily) {
  const f = normalizeFamily(templateFamily);
  if (!f) return DEFAULT_ANCHOR_MAP;
  if (f.includes('jha') || f.includes('openanywhere')) return JHA_OPENANYWHERE_ANCHOR_MAP;
  return DEFAULT_ANCHOR_MAP;
}

module.exports = {
  DEFAULT_ANCHOR_MAP,
  JHA_OPENANYWHERE_ANCHOR_MAP,
  getTemplateAnchorMap,
};
