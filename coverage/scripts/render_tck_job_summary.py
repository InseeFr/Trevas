#!/usr/bin/env python3
from __future__ import annotations

import os
import re
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

FULL_REPORT_PATH = Path("coverage/target/tck-scripts-report.md")
MAX_SUMMARY_CHARS = 120000
TCK_ZIP_CANDIDATES = [
    Path("coverage/src/main/resources/v2.1.zip"),
    Path("vtl/tck/v2.1.zip"),
    Path("coverage/target/classes/v2.1.zip"),
]
SUREFIRE_XML_PATH = Path("coverage/target/surefire-reports/TEST-fr.insee.vtl.coverage.TCKTest.xml")


def resolve_tck_zip() -> Path | None:
    for path in TCK_ZIP_CANDIDATES:
        if path.exists():
            return path
    return None


def parse_test_index(testcase_name: str) -> int | None:
    m = re.search(r"\[(\d+)\]\s*$", testcase_name)
    if m:
        return int(m.group(1))
    m = re.search(r"\bTest\s+(\d+)\b", testcase_name)
    if m:
        return int(m.group(1))
    return None


def read_execution_results() -> dict[int, dict[str, str]]:
    if not SUREFIRE_XML_PATH.exists():
        return {}
    tree = ET.parse(SUREFIRE_XML_PATH)
    root = tree.getroot()
    results: dict[int, dict[str, str]] = {}
    for tc in root.findall(".//testcase"):
        name = tc.attrib.get("name", "")
        idx = parse_test_index(name)
        if idx is None:
            continue
        failure = tc.find("failure")
        error = tc.find("error")
        if failure is not None or error is not None:
            node = error if error is not None else failure
            message = (node.attrib.get("message", "") if node is not None else "").strip()
            results[idx] = {
                "status": "FAIL",
                "label": name,
                "error": message or "Unknown failure",
            }
        else:
            results[idx] = {"status": "PASS", "label": name, "error": ""}
    return results


def main() -> None:
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    zip_path = resolve_tck_zip()

    FULL_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    if zip_path is None:
        FULL_REPORT_PATH.write_text(
            "# TCK scripts output\n\n"
            "_Unable to locate `v2.1.zip` in expected paths:_\n\n"
            + "\n".join(f"- `{p}`" for p in TCK_ZIP_CANDIDATES)
            + "\n",
            encoding="utf-8",
        )
        return

    cases: list[tuple[str, str]] = []
    with zipfile.ZipFile(zip_path) as zf:
        for name in sorted(zf.namelist()):
            if not name.endswith("transformation.vtl"):
                continue
            script = zf.read(name).decode("utf-8", errors="replace").replace("\r", "")
            display_path = name[: -len("transformation.vtl")].rstrip("/")
            cases.append((display_path, script))

    execution = read_execution_results()

    with open(FULL_REPORT_PATH, "w", encoding="utf-8") as full_out:
        full_out.write("# TCK scripts output\n\n")
        full_out.write(f"Source zip: `{zip_path}`\n\n")
        full_out.write(f"Total cases: {len(cases)}\n\n")
        for i, (display_path, script) in enumerate(cases, start=1):
            full_out.write(f"## Test {i}\n\n")
            full_out.write(f"{display_path}\n\n")
            full_out.write("```vtl\n")
            full_out.write(script if script else "(empty)")
            full_out.write("\n```\n\n")
            result = execution.get(i)
            if result is None:
                full_out.write("Result: ⏳ Test not executed or result unavailable\n\n")
            elif result["status"] == "PASS":
                full_out.write(f"✅ Test {i}\n")
                full_out.write(f"\t{result['label']}\n\n")
            else:
                full_out.write(f"❌ Test {i}\n")
                full_out.write(f"\t{result['label']}\n")
                full_out.write(f"\t{result['error']}\n\n")

    if not summary_path:
        return

    full_markdown = FULL_REPORT_PATH.read_text(encoding="utf-8", errors="replace")
    preview = full_markdown[:MAX_SUMMARY_CHARS]
    truncated = len(full_markdown) > MAX_SUMMARY_CHARS

    with open(summary_path, "a", encoding="utf-8") as out:
        out.write("## TCK scripts output\n\n")
        out.write("Report is exported as artifact `tck-scripts-report`.\n\n")
        out.write(preview)
        if truncated:
            out.write(
                "\n\n_... Output truncated in job summary due to GitHub size limits. "
                "Download artifact `tck-scripts-report` for the full content._\n"
            )


if __name__ == "__main__":
    main()
