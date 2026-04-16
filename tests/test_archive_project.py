import unittest
from pathlib import Path

from archive_project import clear_cache_dir, clear_tree


class FakePath:
    def __init__(self, name: str, *, is_dir: bool, parent: "FakePath | None" = None):
        self.name = name
        self._is_dir = is_dir
        self.parent = parent
        self.children: list[FakePath] = []
        self._exists = True
        self.unlinked = False
        self.removed = False
        if parent is not None:
            parent.children.append(self)

    def __repr__(self) -> str:
        return f"FakePath({self.as_posix()})"

    def __lt__(self, other: "FakePath") -> bool:
        return self.as_posix() < other.as_posix()

    def as_posix(self) -> str:
        if self.parent is None:
            return self.name
        return f"{self.parent.as_posix()}/{self.name}"

    def exists(self) -> bool:
        return self._exists

    def is_dir(self) -> bool:
        return self._is_dir

    def is_file(self) -> bool:
        return not self._is_dir

    def rglob(self, pattern: str):
        items: list[FakePath] = []
        for child in self.children:
            items.append(child)
            if child.is_dir():
                items.extend(child.rglob(pattern))
        return items

    def relative_to(self, other: "FakePath") -> Path:
        self_parts = self.as_posix().split("/")
        other_parts = other.as_posix().split("/")
        rel_parts = self_parts[len(other_parts):]
        return Path(*rel_parts) if rel_parts else Path(".")

    def iterdir(self):
        return iter(self.children)

    def unlink(self) -> None:
        if self.parent is not None:
            self.parent.children.remove(self)
        self._exists = False
        self.unlinked = True

    def rmdir(self) -> None:
        if self.parent is not None:
            self.parent.children.remove(self)
        self._exists = False
        self.removed = True


class ArchiveProjectTests(unittest.TestCase):
    def test_clear_tree_prunes_empty_subdirectories_but_keeps_gitkeep_anchors(self):
        root = FakePath("input", is_dir=True)

        pdfs_dir = FakePath("pdfs", is_dir=True, parent=root)
        FakePath(".gitkeep", is_dir=False, parent=pdfs_dir)
        pdfs_nested = FakePath("B. infantis", is_dir=True, parent=pdfs_dir)
        pdf_file = FakePath("paper.pdf", is_dir=False, parent=pdfs_nested)

        metadata_dir = FakePath("pdf_metadata", is_dir=True, parent=root)
        FakePath(".gitkeep", is_dir=False, parent=metadata_dir)
        metadata_nested = FakePath("HMOs", is_dir=True, parent=metadata_dir)
        metadata_file = FakePath("metadata.json", is_dir=False, parent=metadata_nested)

        deleted_files, deleted_dirs = clear_tree(root, dry_run=False)

        self.assertEqual(2, deleted_files)
        self.assertEqual(2, deleted_dirs)
        self.assertTrue(pdf_file.unlinked)
        self.assertTrue(metadata_file.unlinked)
        self.assertTrue(pdfs_nested.removed)
        self.assertTrue(metadata_nested.removed)
        self.assertEqual([".gitkeep"], [child.name for child in pdfs_dir.children])
        self.assertEqual([".gitkeep"], [child.name for child in metadata_dir.children])

    def test_clear_cache_dir_removes_root_for_ephemeral_cache(self):
        root = FakePath("repo", is_dir=True)
        cache_dir = FakePath(".pytest_cache", is_dir=True, parent=root)
        nested = FakePath("v", is_dir=True, parent=cache_dir)
        cache_file = FakePath("nodeids", is_dir=False, parent=nested)

        deleted_files, deleted_dirs, removed_root = clear_cache_dir(
            cache_dir,
            dry_run=False,
            remove_root_when_empty=True,
        )

        self.assertEqual(1, deleted_files)
        self.assertEqual(1, deleted_dirs)
        self.assertTrue(cache_file.unlinked)
        self.assertTrue(nested.removed)
        self.assertTrue(cache_dir.removed)
        self.assertTrue(removed_root)


if __name__ == "__main__":
    unittest.main()
