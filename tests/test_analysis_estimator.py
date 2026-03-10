import unittest

from utils.analysis_estimator import build_analysis_plan_v1


class AnalysisEstimatorTests(unittest.TestCase):
    def test_standard_mode_uses_bundle_summary(self):
        plan = build_analysis_plan_v1(
            snapshot_id='snap1',
            repo_snapshot={
                'analysis_mode': 'standard',
                'analysis_mode_reasons': ['small repo'],
                'selected_file_count': 10,
                'fetched_file_count': 10,
                'failed_fetch_count': 0,
                'bundle_summary': {'bundle_chars': 4000},
            },
            component_inventory={'components': [{'id': 'c1'}]},
            chunk_manifest={'chunks': [{'id': 'k1'}]},
            provider='openai',
            model='gpt-5',
            max_output_tokens=6000,
        )
        self.assertEqual(plan['analysis_mode'], 'standard')
        self.assertEqual(plan['llm_strategy'], 'standard_bundle')
        self.assertGreater(plan['estimated_total_tokens'], 0)
        self.assertEqual(plan['selected_file_count'], 10)
        self.assertEqual(plan['chunk_count'], 0)

    def test_large_repo_uses_chunk_context(self):
        plan = build_analysis_plan_v1(
            snapshot_id='snap2',
            repo_snapshot={
                'analysis_mode': 'large_repo',
                'analysis_mode_reasons': ['vb6 forms > 80'],
                'selected_file_count': 220,
                'fetched_file_count': 210,
                'failed_fetch_count': 10,
            },
            component_inventory={'components': [{'id': 'c1'}, {'id': 'c2'}]},
            chunk_manifest={
                'chunk_count': 3,
                'chunks': [
                    {'id': 'k1', 'estimated_chars': 20000},
                    {'id': 'k2', 'estimated_chars': 32000},
                    {'id': 'k3', 'estimated_chars': 10000},
                ],
            },
            large_repo_context={
                'context_text': 'x' * 12000,
                'included_chunk_count': 2,
                'omitted_chunk_count': 1,
                'included_file_count': 180,
                'omitted_file_count': 40,
            },
            provider='anthropic',
            model='claude-sonnet-4-20250514',
            max_output_tokens=6000,
        )
        self.assertEqual(plan['analysis_mode'], 'large_repo')
        self.assertEqual(plan['llm_strategy'], 'bounded_chunk_synthesis')
        self.assertEqual(plan['included_chunk_count'], 2)
        self.assertEqual(plan['omitted_chunk_count'], 1)
        self.assertEqual(plan['largest_chunk_estimated_tokens'], 8000)
        self.assertEqual(plan['chunk_count'], 3)
        self.assertIn(plan['llm_rejection_risk'], {'low', 'medium', 'high'})


if __name__ == '__main__':
    unittest.main()
