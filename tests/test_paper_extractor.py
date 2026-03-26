import tempfile
import unittest
from pathlib import Path

from helpers.pdf_metadata import write_pdf_metadata
from paper_extractor.cli import derive_pdf_source_label
from paper_extractor.schema import NOT_AVAILABLE


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


if __name__ == "__main__":
    unittest.main()
