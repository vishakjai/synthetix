import unittest
from types import SimpleNamespace

from utils.llm import LLMClient


class OpenAIMessageTextTest(unittest.TestCase):
    def test_prefers_string_content(self):
        message = SimpleNamespace(content='{"ok":true}', refusal=None)
        self.assertEqual(LLMClient._openai_message_text(message), '{"ok":true}')

    def test_flattens_block_content(self):
        message = SimpleNamespace(
            content=[
                {"type": "text", "text": "first"},
                SimpleNamespace(text="second"),
            ],
            refusal=None,
        )
        self.assertEqual(LLMClient._openai_message_text(message), "first\nsecond")

    def test_falls_back_to_refusal_text(self):
        message = SimpleNamespace(content=None, refusal="safety refusal")
        self.assertEqual(LLMClient._openai_message_text(message), "safety refusal")


if __name__ == "__main__":
    unittest.main()
