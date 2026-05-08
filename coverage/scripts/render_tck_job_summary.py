#!/usr/bin/env python3
from __future__ import annotations

import os
import re
from pathlib import Path

FULL_REPORT_PATH = Path("coverage/target/tck-scripts-report.md")
MAX_SUMMARY_CASES = 40


def extract_cases(text: str) -> list[dict[str, str]]:
    lines = text.splitlines()
    cases: list[dict[str, str]] = []
    i = 0
    while i < len(lines):
        m = re.match(r"^(✅|❌)\s+Test\s+(\d+)\s*$", lines[i].strip())
        if not m:
            i += 1
            continue
        status = "PASS" if m.group(1) == "✅" else "FAIL"
        index = m.group(2)
        script = ""
        path = ""
        cursor = i + 1
        if cursor < len(lines):
            path = lines[cursor].strip()
            cursor += 1

        # Parse full script block emitted by TCKTest markers.
        if cursor < len(lines) and lines[cursor].strip() == ">>>VTL_SCRIPT_START<<<":
            cursor += 1
            script_lines: list[str] = []
            while cursor < len(lines):
                if lines[cursor].strip() == ">>>VTL_SCRIPT_END<<<":
                    cursor += 1
                    break
                script_lines.append(lines[cursor])
                cursor += 1
            script = "\n".join(script_lines)

        cases.append({"status": status, "index": index, "script": script, "path": path})
        i = cursor if cursor > i else i + 1
    return cases


def main() -> None:
    report = Path("coverage/target/surefire-reports/fr.insee.vtl.coverage.TCKTest.txt")
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not report.exists():
        return

    text = report.read_text(encoding="utf-8", errors="replace")
    cases = extract_cases(text)
    FULL_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with open(FULL_REPORT_PATH, "w", encoding="utf-8") as full_out:
        full_out.write("# TCK scripts output (full)\n\n")
        if not cases:
            full_out.write("_No TCK test case lines found in surefire report._\n")
        else:
            full_out.write(f"Parsed {len(cases)} case(s) from surefire report.\n\n")
            for c in cases:
                icon = "✅" if c["status"] == "PASS" else "❌"
                full_out.write(f"## {icon} Test {c['index']}\n\n")
                if c["path"]:
                    full_out.write(f"`{c['path']}`\n\n")
                full_out.write("```vtl\n")
                full_out.write((c["script"] or "(empty)").replace("\r", ""))
                full_out.write("\n```\n\n")

    if not summary_path:
        return

    with open(summary_path, "a", encoding="utf-8") as out:
        out.write("## TCK scripts output (preview)\n\n")
        if not cases:
            out.write("_No TCK test case lines found in surefire report._\n")
            return
        out.write(
            f"Parsed {len(cases)} case(s). Showing first {min(len(cases), MAX_SUMMARY_CASES)} "
            "in summary; full report is exported as artifact `tck-scripts-report`.\n\n"
        )
        for c in cases[:MAX_SUMMARY_CASES]:
            icon = "✅" if c["status"] == "PASS" else "❌"
            out.write(f"### {icon} Test {c['index']}\n\n")
            if c["path"]:
                out.write(f"`{c['path']}`\n\n")
            out.write("```vtl\n")
            out.write((c["script"] or "(empty)").replace("\r", ""))
            out.write("\n```\n\n")


if __name__ == "__main__":
    main()
