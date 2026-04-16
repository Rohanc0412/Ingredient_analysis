import json
import tempfile
import unittest
from pathlib import Path

from helpers.pdf_metadata import pdf_metadata_path, resolve_metadata_root, write_pdf_metadata
from paper_deduper.cli import build_report, build_session_name, find_duplicate_groups, main


class PaperDeduperTests(unittest.TestCase):
    def test_duplicate_keeps_alphabetically_first(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "pdfs"
            first = root / "Ingredient" / "same (2).pdf"
            second = root / "Ingredient" / "same.pdf"
            first.parent.mkdir(parents=True, exist_ok=True)
            first.write_bytes(b"duplicate-pdf-content")
            second.write_bytes(b"duplicate-pdf-content")

            groups = find_duplicate_groups(root, [first, second])

            self.assertEqual(1, len(groups))
            self.assertEqual(first, groups[0].kept_path)
            self.assertEqual((second,), groups[0].duplicate_paths)

    def test_same_folder_duplicate_keeps_lexicographically_smallest(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "pdfs"
            a = root / "Ingredient" / "paper (2).pdf"
            b = root / "Ingredient" / "paper.pdf"
            a.parent.mkdir(parents=True, exist_ok=True)
            a.write_bytes(b"same")
            b.write_bytes(b"same")

            groups = find_duplicate_groups(root, [a, b])

            self.assertEqual(1, len(groups))
            self.assertEqual(a if str(a.relative_to(root)).replace("\\", "/").lower() < str(b.relative_to(root)).replace("\\", "/").lower() else b, groups[0].kept_path)

    def test_dry_run_writes_report_without_moving_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            pdf_root = workspace / "input" / "pdfs"
            quarantine_root = workspace / "output" / "quarantine"
            report_json = workspace / "output" / "report.json"
            keep = pdf_root / "Ingredient" / "paper.pdf"
            dup = pdf_root / "Ingredient" / "paper (2).pdf"
            keep.parent.mkdir(parents=True, exist_ok=True)
            keep.write_bytes(b"same-bytes")
            dup.write_bytes(b"same-bytes")

            exit_code = main(
                [
                    "--pdf-root",
                    str(pdf_root),
                    "--quarantine-root",
                    str(quarantine_root),
                    "--report-json",
                    str(report_json),
                    "--dry-run",
                ]
            )

            self.assertEqual(0, exit_code)
            self.assertTrue(keep.exists())
            self.assertTrue(dup.exists())
            self.assertTrue(report_json.exists())
            payload = json.loads(report_json.read_text(encoding="utf-8"))
            self.assertTrue(payload["run"]["dry_run"])
            self.assertEqual(1, payload["totals"]["duplicate_groups"])
            self.assertEqual(1, payload["totals"]["moved_files"])

    def test_real_run_moves_duplicate_into_quarantine(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            pdf_root = workspace / "input" / "pdfs"
            quarantine_root = workspace / "output" / "quarantine"
            report_json = workspace / "output" / "report.json"
            keep = pdf_root / "Ingredient" / "paper.pdf"
            dup = pdf_root / "Ingredient" / "paper (2).pdf"
            keep.parent.mkdir(parents=True, exist_ok=True)
            keep.write_bytes(b"same-bytes")
            dup.write_bytes(b"same-bytes")

            exit_code = main(
                [
                    "--pdf-root",
                    str(pdf_root),
                    "--quarantine-root",
                    str(quarantine_root),
                    "--report-json",
                    str(report_json),
                ]
            )

            self.assertEqual(0, exit_code)
            self.assertTrue(keep.exists())
            self.assertFalse(dup.exists())
            payload = json.loads(report_json.read_text(encoding="utf-8"))
            session_dir = Path(payload["run"]["quarantine_dir"])
            moved_path = session_dir / "Ingredient" / "paper (2).pdf"
            self.assertTrue(moved_path.exists())

    def test_non_duplicates_are_ignored(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "pdfs"
            a = root / "IngredientA" / "paper.pdf"
            b = root / "IngredientB" / "paper.pdf"
            a.parent.mkdir(parents=True, exist_ok=True)
            b.parent.mkdir(parents=True, exist_ok=True)
            a.write_bytes(b"aaa")
            b.write_bytes(b"bbb")

            groups = find_duplicate_groups(root, [a, b])

            self.assertEqual([], groups)

    def test_real_run_moves_duplicate_metadata_into_quarantine(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            pdf_root = workspace / "input" / "pdfs"
            quarantine_root = workspace / "output" / "quarantine"
            report_json = workspace / "output" / "report.json"
            keep = pdf_root / "Ingredient" / "same.pdf"
            dup = pdf_root / "Ingredient" / "same (2).pdf"
            keep.parent.mkdir(parents=True, exist_ok=True)
            keep.write_bytes(b"same-bytes")
            dup.write_bytes(b"same-bytes")
            write_pdf_metadata(pdf_root=pdf_root, pdf_path=keep, payload={"source": "pubmed"})
            write_pdf_metadata(pdf_root=pdf_root, pdf_path=dup, payload={"source": "google_scholar"})

            exit_code = main(
                [
                    "--pdf-root",
                    str(pdf_root),
                    "--quarantine-root",
                    str(quarantine_root),
                    "--report-json",
                    str(report_json),
                ]
            )

            self.assertEqual(0, exit_code)
            self.assertFalse(pdf_metadata_path(pdf_root=pdf_root, pdf_path=dup).exists())
            payload = json.loads(report_json.read_text(encoding="utf-8"))
            session_dir = Path(payload["run"]["quarantine_dir"])
            quarantine_metadata_root = resolve_metadata_root(session_dir, include_storage_name=True)
            moved_meta_path = pdf_metadata_path(
                pdf_root=session_dir,
                pdf_path=session_dir / "Ingredient" / "same (2).pdf",
                metadata_root=quarantine_metadata_root,
            )
            self.assertTrue(moved_meta_path.exists())


if __name__ == "__main__":
    unittest.main()
