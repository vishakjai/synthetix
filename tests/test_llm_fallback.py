import unittest

from config import LLMProvider, PipelineConfig
from utils.llm import LLMClient, LLMResponse


class LLMFallbackTests(unittest.TestCase):
    def test_anthropic_billing_error_falls_back_to_openai(self):
        cfg = PipelineConfig(
            provider=LLMProvider.ANTHROPIC,
            anthropic_api_key="sk-ant-test",
            anthropic_model="claude-sonnet-4-20250514",
            openai_api_key="sk-test-openai",
            openai_model="gpt-4o",
        )
        client = LLMClient(cfg)

        def boom(_system_prompt, _user_message):
            raise RuntimeError("Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits.")

        def ok(_system_prompt, _user_message):
            return LLMResponse(
                content="fallback-ok",
                model="gpt-4o",
                provider="openai",
                input_tokens=10,
                output_tokens=5,
                latency_ms=0,
            )

        client._invoke_anthropic = boom  # type: ignore[method-assign]
        client._fallback_invoke_openai = ok  # type: ignore[method-assign]

        response = client.invoke("sys", "user")
        self.assertEqual(response.provider, "openai")
        self.assertEqual(response.content, "fallback-ok")


if __name__ == "__main__":
    unittest.main()
