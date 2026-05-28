#!/usr/bin/env python3
"""Unit tests for TCK report sanitization (no Maven required)."""
import unittest

from render_tck_job_summary import compact_java_exception_detail, sanitize_failure_detail


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
