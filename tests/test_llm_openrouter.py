import os
import unittest
from unittest.mock import patch

from helpers.llm_openrouter import _extract_bedrock_text, _parse_usage, load_llm_config


class LLMConfigTests(unittest.TestCase):
    def test_load_llm_config_defaults_to_openrouter(self):
        with patch.dict(os.environ, {"OPENROUTER_API_KEY": "test-key"}, clear=True):
            config = load_llm_config()

        self.assertEqual("openrouter", config.provider)
        self.assertEqual("test-key", config.api_key)
        self.assertEqual("https://openrouter.ai/api/v1", config.base_url)

    def test_load_llm_config_supports_bedrock(self):
        with patch.dict(
            os.environ,
            {
                "LLM_MODE": "bedrock_only",
                "BEDROCK_REGION": "us-east-1",
                "BEDROCK_MODEL_SUMMARY": "amazon.nova-lite-v1:0",
            },
            clear=True,
        ):
            config = load_llm_config(target="summary")

        self.assertEqual("bedrock", config.provider)
        self.assertEqual("us-east-1", config.region)
        self.assertEqual("amazon.nova-lite-v1:0", config.model)

    def test_load_llm_config_supports_openrouter_only_mode(self):
        with patch.dict(
            os.environ,
            {
                "LLM_MODE": "openrouter_only",
                "OPENROUTER_API_KEY": "test-key",
                "OPENROUTER_MODEL_EXTRACT": "openrouter/extract-model",
            },
            clear=True,
        ):
            config = load_llm_config(target="extract")

        self.assertEqual("openrouter", config.provider)
        self.assertEqual("openrouter/extract-model", config.model)

    def test_load_llm_config_supports_bedrock_only_mode(self):
        with patch.dict(
            os.environ,
            {
                "LLM_MODE": "bedrock_only",
                "BEDROCK_REGION": "us-east-1",
                "BEDROCK_MODEL_SUMMARY": "amazon.nova-lite-v1:0",
            },
            clear=True,
        ):
            config = load_llm_config(target="summary")

        self.assertEqual("bedrock", config.provider)
        self.assertEqual("amazon.nova-lite-v1:0", config.model)

    def test_load_llm_config_supports_mixed_mode(self):
        with patch.dict(
            os.environ,
            {
                "LLM_MODE": "mixed",
                "MIXED_MODE_PROVIDER_EXTRACT": "openrouter",
                "MIXED_MODE_PROVIDER_SUMMARY": "bedrock",
                "OPENROUTER_API_KEY": "test-key",
                "OPENROUTER_MODEL_EXTRACT": "openrouter/extract-model",
                "BEDROCK_REGION": "us-east-1",
                "BEDROCK_MODEL_SUMMARY": "amazon.nova-pro-v1:0",
            },
            clear=True,
        ):
            extract_config = load_llm_config(target="extract")
            summary_config = load_llm_config(target="summary")

        self.assertEqual("openrouter", extract_config.provider)
        self.assertEqual("openrouter/extract-model", extract_config.model)
        self.assertEqual("bedrock", summary_config.provider)
        self.assertEqual("amazon.nova-pro-v1:0", summary_config.model)

    def test_load_llm_config_requires_task_specific_bedrock_model_for_target(self):
        with patch.dict(
            os.environ,
            {
                "LLM_MODE": "bedrock_only",
                "BEDROCK_REGION": "us-east-1",
            },
            clear=True,
        ):
            with self.assertRaises(RuntimeError):
                load_llm_config(target="summary")

    def test_parse_usage_supports_bedrock_usage_shape(self):
        usage = _parse_usage({"usage": {"inputTokens": 11, "outputTokens": 7, "totalTokens": 18}})

        self.assertEqual(11, usage.input_tokens)
        self.assertEqual(7, usage.output_tokens)
        self.assertEqual(18, usage.total_tokens)

    def test_extract_bedrock_text_collects_text_blocks(self):
        text = _extract_bedrock_text(
            {
                "output": {
                    "message": {
                        "content": [
                            {"text": "Hello"},
                            {"citationsContent": {"content": [{"text": "World"}]}},
                        ]
                    }
                }
            }
        )

        self.assertEqual("Hello\nWorld", text)


if __name__ == "__main__":
    unittest.main()
