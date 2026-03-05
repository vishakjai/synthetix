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
    business_rules: ['Rule ID', 'Title', 'Statement', 'Rationale', 'Priority'],
    field_definitions: ['Field ID', 'Label', 'Business Meaning', 'Required', 'Validation', 'Source'],
    user_stories: ['Story ID', 'As a', 'I want', 'So that'],
    acceptance_criteria: ['AC ID', 'Statement', 'Linked Story'],
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
    business_rules: ['Business Rule ID', 'Rule Title', 'Business Rule Statement', 'Rationale', 'Priority'],
    field_definitions: ['Field ID', 'Field Label', 'Business Meaning', 'Required', 'Validation Rule', 'Source Refs'],
    user_stories: ['Story ID', 'As a', 'I want', 'So that'],
    acceptance_criteria: ['Acceptance ID', 'Statement', 'Linked Story'],
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
