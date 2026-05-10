#!/usr/bin/env python3
"""
Build a single markdown report: for each TCK case in JUnit execution order, path + script (from zip)
then pass/fail line(s) from Surefire XML.

Zip entry paths use '/'; display paths in tests use ' » ' (see TckPaths.SEGMENT_SEP).
"""
from __future__ import annotations

import os
import re
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

FULL_REPORT_PATH = Path("coverage/target/tck-scripts-report.md")
TCK_ZIP_CANDIDATES = [
    Path("coverage/src/main/resources/v2.1.zip"),
    Path("vtl/tck/v2.1.zip"),
    Path("coverage/target/classes/v2.1.zip"),
]
SUREFIRE_XML_PATH = Path("coverage/target/surefire-reports/TEST-fr.insee.vtl.coverage.TCKTest.xml")

# Matches Java TckPaths.SEGMENT_SEP = " \u00bb "
_DISPLAY_SEP = " \u00bb "


def resolve_tck_zip() -> Path | None:
    for path in TCK_ZIP_CANDIDATES:
        if path.exists():
            return path
    return None


def decode_xml_entities(text: str) -> str:
    out = text
    for _ in range(4):
        nxt = (
            out.replace("&quot;", '"')
            .replace("&#34;", '"')
            .replace("&apos;", "'")
            .replace("&#39;", "'")
            .replace("&lt;", "<")
            .replace("&#60;", "<")
            .replace("&gt;", ">")
            .replace("&#62;", ">")
            .replace("&amp;", "&")
            .replace("&#38;", "&")
            .replace("&#10;", "\n")
            .replace("&#13;", "\r")
            .replace("&#9;", "\t")
        )
        if nxt == out:
            break
        out = nxt
    return out


def split_testcase_name(name_attr: str) -> tuple[int, str] | None:
    """Extract (index, display_path) from Surefire testcase name (before or after prettify)."""
    s = decode_xml_entities(name_attr).strip()
    # Prettified: "Test 1\n\tConditional operators » ..."
    if "\n" in s:
        lines = [ln.strip() for ln in s.split("\n") if ln.strip()]
        if len(lines) >= 2:
            m0 = re.match(r"^Test\s+(\d+)\s*$", lines[0])
            if m0:
                path_line = lines[1].lstrip("\t").strip()
                return int(m0.group(1)), path_line
    # Original phrased: ... Test 1 — path
    m = re.search(r"Test\s+(\d+)\s+—\s+(.+)$", s)
    if m:
        return int(m.group(1)), m.group(2).strip()
    return None


def display_path_to_zip_key(display_path: str) -> str:
    return display_path.replace(_DISPLAY_SEP, "/")


def load_scripts_from_zip(zip_path: Path) -> dict[str, str]:
    scripts: dict[str, str] = {}
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            if not name.endswith("transformation.vtl"):
                continue
            key = name[: -len("transformation.vtl")].rstrip("/")
            body = zf.read(name).decode("utf-8", errors="replace").replace("\r", "")
            scripts[key] = body
    return scripts


def parse_ordered_results(xml_path: Path) -> list[dict[str, str]]:
    """Surefire testcase document order matches parameterized test order."""
    if not xml_path.exists():
        return []
    tree = ET.parse(xml_path)
    root = tree.getroot()
    out: list[dict[str, str]] = []
    for tc in root.findall(".//testcase"):
        name = tc.attrib.get("name", "")
        parsed = split_testcase_name(name)
        if parsed is None:
            continue
        idx, display_path = parsed
        failure = tc.find("failure")
        error = tc.find("error")
        if failure is not None or error is not None:
            node = error if error is not None else failure
            msg = (node.attrib.get("message", "") if node is not None else "").strip()
            body = (node.text or "").strip() if node is not None else ""
            detail = msg
            if body and msg not in body:
                detail = f"{msg}\n{body}" if msg else body
            out.append(
                {
                    "index": idx,
                    "display_path": display_path,
                    "status": "FAIL",
                    "detail": detail or "Unknown failure",
                }
            )
        else:
            out.append(
                {
                    "index": idx,
                    "display_path": display_path,
                    "status": "PASS",
                    "detail": name,
                }
            )
    return out


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
        if summary_path:
            with open(summary_path, "a", encoding="utf-8") as out:
                out.write(
                    "## TCK report\n\n"
                    "_Zip not found — could not build report._\n"
                )
        return

    scripts = load_scripts_from_zip(zip_path)
    results = parse_ordered_results(SUREFIRE_XML_PATH)

    lines: list[str] = []
    lines.append("# TCK scripts output\n")
    lines.append(f"\n_Source zip:_ `{zip_path}`  \n")
    lines.append(f"_Cases (from Surefire):_ {len(results)}\n")

    for row in results:
        i = row["index"]
        display_path = row["display_path"]
        zip_key = display_path_to_zip_key(display_path)
        script = scripts.get(zip_key, "(script not found in zip for this path)")
        lines.append(f"\n## Test {i}\n\n")
        lines.append(f"{display_path}\n\n")
        lines.append("```vtl\n")
        lines.append(script if script else "(empty)")
        lines.append("\n```\n\n")
        if row["status"] == "PASS":
            lines.append(f"✅ Test {i}\n")
            lines.append(f"\t{display_path}\n")
        else:
            lines.append(f"❌ Test {i}\n")
            lines.append(f"\t{display_path}\n")
            detail = row["detail"]
            for ln in detail.split("\n"):
                lines.append(f"\t{ln}\n")

    FULL_REPORT_PATH.write_text("".join(lines), encoding="utf-8")

    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as out:
            out.write(
                "## TCK report\n\n"
                "Unified scripts + results (same order as tests): download artifact "
                "**`tck-scripts-report`** (`tck-scripts-report.md`).\n"
            )


if __name__ == "__main__":
    main()
