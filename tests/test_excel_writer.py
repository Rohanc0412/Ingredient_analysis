import tempfile
import unittest
from pathlib import Path

from openpyxl import Workbook

from helpers.excel_writer import LIT_REVIEW_ALL_SHEET, ensure_review_sheets, load_workbook_context


class ExcelWriterTests(unittest.TestCase):
    def test_accepts_generic_single_sheet_template_with_extractor_headers(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "template.xlsx"
            wb = Workbook()
            ws = wb.active
            ws.title = "Sheet1"
            ws.append(
                [
                    "SL No",
                    "Year",
                    "Authors",
                    "Title",
                    "Journal / Source",
                    "Primary Ingredient",
                ]
            )
            wb.save(path)

            ctx = load_workbook_context(path)

            self.assertIn(LIT_REVIEW_ALL_SHEET, ctx.review_sheets)
            self.assertEqual("Ref #", ctx.headers[0])

    def test_ensure_review_sheets_renames_generic_template_sheet(self):
        wb = Workbook()
        ws = wb.active
        ws.title = "Sheet1"
        ws.append(
            [
                "SL No",
                "Title",
                "Journal / Source",
                "Primary Ingredient",
            ]
        )

        review_sheets = ensure_review_sheets(wb)

        self.assertIn(LIT_REVIEW_ALL_SHEET, review_sheets)


if __name__ == "__main__":
    unittest.main()
