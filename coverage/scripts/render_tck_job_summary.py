#!/usr/bin/env python3
from __future__ import annotations

import os
from pathlib import Path

FULL_REPORT_PATH = Path("coverage/target/tck-scripts-report.md")
MAX_SUMMARY_CHARS = 120000


def main() -> None:
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not FULL_REPORT_PATH.exists():
        return

    if not summary_path:
        return

    full_markdown = FULL_REPORT_PATH.read_text(encoding="utf-8", errors="replace")
    preview = full_markdown[:MAX_SUMMARY_CHARS]
    truncated = len(full_markdown) > MAX_SUMMARY_CHARS

    with open(summary_path, "a", encoding="utf-8") as out:
        out.write("## TCK scripts output (preview)\n\n")
        out.write("Full report is exported as artifact `tck-scripts-report`.\n\n")
        out.write(preview)
        if truncated:
            out.write(
                "\n\n_... Preview truncated in job summary due to GitHub size limits. "
                "Download artifact `tck-scripts-report` for the full content._\n"
            )


if __name__ == "__main__":
    main()
