import tempfile
import unittest
from pathlib import Path

from helpers.file_discovery import sorted_glob_files, sorted_rglob_files


class FileDiscoveryTests(unittest.TestCase):
    def test_sorted_glob_files_ignores_gitkeep(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / ".gitkeep").write_text("", encoding="utf-8")
            (root / "b.json").write_text("{}", encoding="utf-8")
            (root / "a.json").write_text("{}", encoding="utf-8")

            paths = sorted_glob_files(root, "*.json")

            self.assertEqual([root / "a.json", root / "b.json"], paths)

    def test_sorted_rglob_files_ignores_gitkeep(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            subdir = root / "nested"
            subdir.mkdir(parents=True, exist_ok=True)
            (subdir / ".gitkeep").write_text("", encoding="utf-8")
            (subdir / "paper.pdf").write_bytes(b"%PDF-1.4")

            paths = sorted_rglob_files(root, "*.pdf")

            self.assertEqual([subdir / "paper.pdf"], paths)


if __name__ == "__main__":
    unittest.main()
