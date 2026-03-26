import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from helpers.pdf_metadata import pdf_metadata_path
from paper_downloader.downloader import resolve_output_dir, should_include_source_dir, write_download_metadata


class PaperDownloaderTests(unittest.TestCase):
    def test_should_include_source_dir_defaults_to_true(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertTrue(should_include_source_dir())

    def test_should_include_source_dir_reads_false_from_env(self):
        with patch.dict(os.environ, {"PAPER_DOWNLOADER_INCLUDE_SOURCE_DIR": "false"}, clear=True):
            self.assertFalse(should_include_source_dir())

    def test_resolve_output_dir_includes_source_when_enabled(self):
        out_dir = resolve_output_dir(
            out_root=Path("input") / "pdfs",
            ingredient="ashwagandha",
            output_source="pubmed",
            include_source_dir=True,
        )

        self.assertEqual(Path("input") / "pdfs" / "ashwagandha" / "pubmed", out_dir)

    def test_resolve_output_dir_omits_source_when_disabled(self):
        out_dir = resolve_output_dir(
            out_root=Path("input") / "pdfs",
            ingredient="ashwagandha",
            output_source="pubmed",
            include_source_dir=False,
        )

        self.assertEqual(Path("input") / "pdfs" / "ashwagandha", out_dir)

    def test_write_download_metadata_stores_files_under_pdf_metadata_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            pdf_root = workspace / "input" / "pdfs"
            pdf_path = pdf_root / "ashwagandha" / "paper.pdf"
            pdf_path.parent.mkdir(parents=True, exist_ok=True)
            pdf_path.write_bytes(b"%PDF-1.4")

            metadata_path = write_download_metadata(
                pdf_path,
                pdf_root=pdf_root,
                ingredient="ashwagandha",
                output_source="pubmed",
                source_preference="pubmed",
                source_url=None,
                row={"doi": "10.1000/test", "pdf_url": "https://example.com/paper.pdf"},
            )

            self.assertEqual(workspace / "input" / "pdf_metadata" / "ashwagandha" / "paper.pdf.metadata.json", metadata_path)
            self.assertFalse((pdf_path.parent / "paper.pdf.metadata.json").exists())
            self.assertTrue(pdf_metadata_path(pdf_root=pdf_root, pdf_path=pdf_path).exists())


if __name__ == "__main__":
    unittest.main()
