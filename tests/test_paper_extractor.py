import tempfile
import unittest
from pathlib import Path

from helpers.pdf_metadata import write_pdf_metadata
from paper_extractor.cli import derive_pdf_source_label
from paper_extractor.schema import NOT_AVAILABLE, flatten_llm_to_excel


class PaperExtractorTests(unittest.TestCase):
    def test_derive_pdf_source_label_uses_sidecar_metadata_when_source_folder_is_disabled(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "pdfs"
            pdf_path = root / "ashwagandha" / "paper.pdf"
            pdf_path.parent.mkdir(parents=True, exist_ok=True)
            pdf_path.write_bytes(b"%PDF-1.4")
            write_pdf_metadata(pdf_root=root, pdf_path=pdf_path, payload={"source": "google_scholar"})

            label = derive_pdf_source_label(root, pdf_path)

            self.assertEqual("google scholar", label)

    def test_derive_pdf_source_label_does_not_mistake_filename_for_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "pdfs"
            pdf_path = root / "ashwagandha" / "paper.pdf"
            pdf_path.parent.mkdir(parents=True, exist_ok=True)
            pdf_path.write_bytes(b"%PDF-1.4")

            label = derive_pdf_source_label(root, pdf_path)

            self.assertEqual(NOT_AVAILABLE, label)

    def test_flatten_llm_to_excel_uses_flat_template_keys(self):
        headers = [
            "Ingredient",
            "Ingredient Category",
            "Year",
            "Title",
            "Journal / Source",
            "Population",
        ]

        row = flatten_llm_to_excel(
            headers,
            {
                "Ingredient": "Resveratrol",
                "Ingredient Category": "Polyphenol",
                "Year": "2024",
                "Title": "Example Title",
                "Journal / Source": "Foods",
                "Population": "Adults",
            },
        )

        self.assertEqual("Resveratrol", row["Ingredient"])
        self.assertEqual("Polyphenol", row["Ingredient Category"])
        self.assertEqual("2024", row["Year"])
        self.assertEqual("Example Title", row["Title"])
        self.assertEqual("Foods", row["Journal / Source"])
        self.assertEqual("Adults", row["Population"])

    def test_flatten_llm_to_excel_supports_primary_ingredient_alias(self):
        # LLM key "Primary Ingredient" should fuzzy-match to template header "Ingredient"
        headers = ["Ingredient", "Title"]

        row = flatten_llm_to_excel(
            headers,
            {
                "Primary Ingredient": "Urolithin A",
                "Title": "Example Title",
            },
        )

        self.assertEqual("Urolithin A", row["Ingredient"])
        self.assertEqual("Example Title", row["Title"])

    def test_flatten_llm_to_excel_supports_nested_keys_without_hardcoded_mapping(self):
        headers = ["Year", "Journal / Source", "Population"]

        row = flatten_llm_to_excel(
            headers,
            {
                "paper_identification": {
                    "year": "2024",
                    "journal_source": "Foods",
                },
                "study_design": {
                    "population": "Adults",
                },
            },
        )

        self.assertEqual("2024", row["Year"])
        self.assertEqual("Foods", row["Journal / Source"])
        self.assertEqual("Adults", row["Population"])


if __name__ == "__main__":
    unittest.main()
