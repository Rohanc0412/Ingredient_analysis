import json
import tempfile
import unittest
from pathlib import Path

from paper_deduper.cli import build_report, build_session_name, find_duplicate_groups, main


class PaperDeduperTests(unittest.TestCase):
    def test_prefers_pubmed_over_google_scholar(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "pdfs"
            pubmed = root / "Ingredient" / "pubmed" / "same.pdf"
            scholar = root / "Ingredient" / "google_scholar" / "same.pdf"
            pubmed.parent.mkdir(parents=True, exist_ok=True)
            scholar.parent.mkdir(parents=True, exist_ok=True)
            payload = b"duplicate-pdf-content"
            pubmed.write_bytes(payload)
            scholar.write_bytes(payload)

            groups = find_duplicate_groups(root, [pubmed, scholar])

            self.assertEqual(1, len(groups))
            self.assertEqual(pubmed, groups[0].kept_path)
            self.assertEqual((scholar,), groups[0].duplicate_paths)

    def test_same_folder_duplicate_keeps_lexicographically_smallest(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "pdfs"
            a = root / "Ingredient" / "china" / "paper (2).pdf"
            b = root / "Ingredient" / "china" / "paper.pdf"
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
            keep = pdf_root / "Ingredient" / "pubmed" / "paper.pdf"
            dup = pdf_root / "Ingredient" / "google_scholar" / "paper.pdf"
            keep.parent.mkdir(parents=True, exist_ok=True)
            dup.parent.mkdir(parents=True, exist_ok=True)
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
            keep = pdf_root / "Ingredient" / "pubmed" / "paper.pdf"
            dup = pdf_root / "Ingredient" / "google_scholar" / "paper.pdf"
            keep.parent.mkdir(parents=True, exist_ok=True)
            dup.parent.mkdir(parents=True, exist_ok=True)
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
            moved_path = session_dir / "Ingredient" / "google_scholar" / "paper.pdf"
            self.assertTrue(moved_path.exists())

    def test_non_duplicates_are_ignored(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "pdfs"
            a = root / "IngredientA" / "pubmed" / "paper.pdf"
            b = root / "IngredientB" / "google_scholar" / "paper.pdf"
            a.parent.mkdir(parents=True, exist_ok=True)
            b.parent.mkdir(parents=True, exist_ok=True)
            a.write_bytes(b"aaa")
            b.write_bytes(b"bbb")

            groups = find_duplicate_groups(root, [a, b])

            self.assertEqual([], groups)


if __name__ == "__main__":
    unittest.main()
