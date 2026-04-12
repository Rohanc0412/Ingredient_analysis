import json
import tempfile
import unittest
from pathlib import Path

from populator_ingredient_matrix.cli import load_records, normalize_key


class MatrixPopulatorTests(unittest.TestCase):
    def test_load_records_extracts_supported_field_shapes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = {
                "ingredient": "Example Ingredient",
                "fields": {
                    "Ingredient Category (Botanical, Vitamin, Mineral, Probiotic, Peptide, Fiber, etc.)": {
                        "category": "- Botanical",
                        "sources": [],
                    },
                    "Evidence of GLP-1 Interaction (Yes/No/Indirect)": {
                        "category": "- Indirect",
                        "sources": [],
                    },
                    "Clinical Evidence Level (Human RCT, Human Observational, Animal, In-vitro)": {
                        "category": "- Human RCT",
                        "sources": [],
                    },
                    "Number of Clinical Studies": {
                        "summary": "- More than 12 studies reported",
                        "study_count": 12,
                        "sources": [],
                    },
                    "Key Clinical Outcomes (Weight Loss %, Appetite Reduction, HbA1c, etc.)": {
                        "summary": "- Weight loss improved",
                        "study_count": 4,
                        "sources": [],
                    },
                    "Formulation Format (Capsule, Powder, Drink, Gummy, Sachet)": {
                        "category": "- Capsule, powder",
                        "sources": [],
                    },
                    "Average Effective Dose": {
                        "value": 1500,
                        "unit": "mg/day",
                        "context": "- Common regimen is split across meals",
                        "sources": [],
                    },
                    "Consumer Perceived Naturalness (High/Medium/Low)": {
                        "category": "- High",
                        "sources": [],
                    },
                    "Gender Skew": {
                        "category": "- Neutral",
                        "sources": [],
                    },
                    "Synergy Ingredients Commonly Used": {
                        "items": ["- Fiber", "- Chromium"],
                        "sources": [],
                    },
                },
            }
            (root / "example.json").write_text(json.dumps(payload), encoding="utf-8")

            records = load_records(root)

            self.assertEqual(1, len(records))
            record = records[0]
            self.assertEqual("Example Ingredient", record[normalize_key("Ingredient")])
            self.assertEqual(
                "- Botanical",
                record[normalize_key("Ingredient Category (Botanical, Vitamin, Mineral, Probiotic, Peptide, Fiber, etc.)")],
            )
            self.assertEqual(
                "- Indirect",
                record[normalize_key("Evidence of GLP-1 Interaction (Yes/No/Indirect)")],
            )
            self.assertEqual(
                "- Human RCT",
                record[normalize_key("Clinical Evidence Level (Human RCT, Human Observational, Animal, In-vitro)")],
            )
            self.assertEqual("12", record[normalize_key("Number of Clinical Studies")])
            self.assertEqual(
                "- Weight loss improved",
                record[normalize_key("Key Clinical Outcomes (Weight Loss %, Appetite Reduction, HbA1c, etc.)")],
            )
            self.assertEqual(
                "- Capsule, powder",
                record[normalize_key("Formulation Format (Capsule, Powder, Drink, Gummy, Sachet)")],
            )
            self.assertEqual(
                "1500 mg/day\n- Common regimen is split across meals",
                record[normalize_key("Average Effective Dose")],
            )
            self.assertEqual(
                "- High",
                record[normalize_key("Consumer Perceived Naturalness (High/Medium/Low)")],
            )
            self.assertEqual("- Neutral", record[normalize_key("Gender Skew")])
            self.assertEqual(
                "- Fiber\n- Chromium",
                record[normalize_key("Synergy Ingredients Commonly Used")],
            )


if __name__ == "__main__":
    unittest.main()
