#!/usr/bin/env python3
"""Unit tests for TCK report sanitization (no Maven required)."""
import unittest

from render_tck_job_summary import (
    apply_report_sort,
    assign_report_numbers,
    compact_java_exception_detail,
    sanitize_failure_detail,
    sort_results_by_display_path,
    sort_results_by_index,
)


class SortResultsTest(unittest.TestCase):
    def test_apply_report_sort_defaults_to_path(self) -> None:
        import os

        rows = [
            {"index": 1, "display_path": "Z » ex_9 » ex_9", "status": "PASS", "detail": ""},
            {"index": 2, "display_path": "A » ex_2 » ex_2", "status": "PASS", "detail": ""},
        ]
        old = os.environ.pop("TCK_REPORT_SORT", None)
        try:
            apply_report_sort(rows)
        finally:
            if old is not None:
                os.environ["TCK_REPORT_SORT"] = old
        self.assertEqual([r["display_path"] for r in rows], ["A » ex_2 » ex_2", "Z » ex_9 » ex_9"])

    def test_assign_report_numbers_after_path_sort(self) -> None:
        rows = [
            {"index": 165, "display_path": "Agg » ex_1 » ex_1", "status": "PASS", "detail": ""},
            {"index": 164, "display_path": "Agg » ex_2 » ex_2", "status": "PASS", "detail": ""},
        ]
        sort_results_by_display_path(rows)
        assign_report_numbers(rows)
        self.assertEqual([r["report_index"] for r in rows], [1, 2])
        self.assertEqual([r["index"] for r in rows], [165, 164])

    def test_sorts_by_test_index(self) -> None:
        rows = [
            {"index": 165, "display_path": "Agg » ex_1 » ex_1", "status": "PASS", "detail": ""},
            {"index": 164, "display_path": "Agg » ex_2 » ex_2", "status": "PASS", "detail": ""},
            {"index": 1, "display_path": "Z » ex_9 » ex_9", "status": "PASS", "detail": ""},
        ]
        sort_results_by_index(rows)
        self.assertEqual([r["index"] for r in rows], [1, 164, 165])

    def test_sorts_by_display_path_natural_order(self) -> None:
        rows = [
            {"index": 3, "display_path": "A » ex_10 » ex_10", "status": "PASS", "detail": ""},
            {"index": 1, "display_path": "A » ex_2 » ex_2", "status": "PASS", "detail": ""},
            {"index": 2, "display_path": "B » ex_1 » ex_1", "status": "PASS", "detail": ""},
        ]
        sort_results_by_display_path(rows)
        paths = [r["display_path"] for r in rows]
        self.assertEqual(
            paths,
            ["A » ex_2 » ex_2", "A » ex_10 » ex_10", "B » ex_1 » ex_1"],
        )


class CompactExceptionTest(unittest.TestCase):
    def test_collapses_bare_stack_to_summary(self) -> None:
        raw = """java.util.NoSuchElementException
\tat fr.insee.vtl.spark.SparkDataset.get(SparkDataset.java:42)
\tat fr.insee.vtl.engine.visitors.ExpressionVisitor.visit(ExpressionVisitor.java:100)
\tat java.base/java.lang.reflect.Method.invoke(Method.java:568)
\tat org.apache.spark.sql.Dataset.collect(Dataset.java:1)
"""
        out = compact_java_exception_detail(raw)
        self.assertIn("NoSuchElementException", out)
        self.assertIn("fr.insee.vtl.spark.SparkDataset", out)
        self.assertNotIn("org.apache.spark", out)

    def test_skips_structured_tck_failure(self) -> None:
        raw = "[path] output `DS_r` — row data differs\n\nTrevas (actual):\n"
        self.assertEqual(raw, compact_java_exception_detail(raw))

    def test_sanitize_keeps_numbered_trevas_stack(self) -> None:
        raw = """java.lang.AssertionError:

[Conditional » ex_1] script execution failed
Exception: java.util.NoSuchElementException
Trevas stack (file:line):
  1. fr.insee.vtl.spark.SparkProcessingEngine.eval — SparkProcessingEngine.java:512
  2. fr.insee.vtl.engine.visitors.ConditionalVisitor.visit — ConditionalVisitor.java:88

--- inputs ---
« DS_1 »
| a |
| --- |
| 1 |
"""
        out = sanitize_failure_detail(raw)
        self.assertIn("script execution failed", out)
        self.assertIn("SparkProcessingEngine.java:512", out)
        self.assertIn("ConditionalVisitor.java:88", out)
        self.assertNotIn("java.lang.AssertionError", out)
        self.assertNotIn("VTL script:", out)


if __name__ == "__main__":
    unittest.main()
