#!/usr/bin/env python3
"""
Build a readable markdown report: for each TCK case in JUnit execution order —
path (slash style), script from zip, then pass/fail with full failure body (tables preserved).

Zip keys use '/'; display paths in Surefire use ' » ' (TckPaths.SEGMENT_SEP).
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
    if "\n" in s:
        lines = [ln.strip() for ln in s.split("\n") if ln.strip()]
        if len(lines) >= 2:
            m0 = re.match(r"^Test\s+(\d+)\s*$", lines[0])
            if m0:
                path_line = lines[1].lstrip("\t").strip()
                return int(m0.group(1)), path_line
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


def failure_detail_text(message: str, body: str) -> str:
    """Single block for markdown: message + body without awkward duplication."""
    message = message.strip()
    body = (body or "").strip()
    if not body:
        return message or "Unknown failure"
    if not message:
        return body
    if message in body:
        return body
    if body in message:
        return message
    return f"{message}\n\n{body}"


def fenced_lang(content: str, lang: str) -> str:
    inner = content.rstrip("\n")
    fence = "```"
    return f"{fence}{lang}\n{inner}\n{fence}\n"


def parse_ordered_results(xml_path: Path) -> list[dict[str, str]]:
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
            detail = failure_detail_text(msg, body)
            out.append(
                {
                    "index": idx,
                    "display_path": display_path,
                    "status": "FAIL",
                    "detail": detail,
                }
            )
        else:
            out.append(
                {
                    "index": idx,
                    "display_path": display_path,
                    "status": "PASS",
                    "detail": "",
                }
            )
    return out


def render_case_md(i: int, display_path: str, path_slash: str, script: str, row: dict[str, str]) -> str:
    parts: list[str] = []
    parts.append(f"### Test {i}\n\n")
    parts.append(f"{path_slash}\n\n")
    scr = script.strip() if script else "(empty)"
    parts.append(fenced_lang(scr, "vtl"))
    parts.append("\n")
    if row["status"] == "PASS":
        parts.append(f"✅ **Test {i}**\n\n")
    else:
        parts.append(f"❌ **Test {i}**\n\n")
        parts.append(f"Test {i}\n\n")
        parts.append(f"{display_path}\n\n")
        detail = row["detail"].strip()
        parts.append(fenced_lang(detail, "text"))
        parts.append("\n")
    return "".join(parts)


def main() -> None:
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    zip_path = resolve_tck_zip()

    FULL_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    if zip_path is None:
        fallback = (
            "# Trevas - TCK VTL 2.1\n\n"
            "## TCK scripts output\n\n"
            "Report is exported as artifact `tck-scripts-report`.\n\n"
            "TCK scripts output\n"
            "_Unable to locate `v2.1.zip` in expected paths:_\n\n"
            + "\n".join(f"- `{p}`" for p in TCK_ZIP_CANDIDATES)
            + "\n"
        )
        FULL_REPORT_PATH.write_text(fallback, encoding="utf-8")
        if summary_path:
            with open(summary_path, "a", encoding="utf-8") as out:
                out.write("\n\n---\n\n")
                out.write(fallback)
        return

    scripts = load_scripts_from_zip(zip_path)
    results = parse_ordered_results(SUREFIRE_XML_PATH)

    chunks: list[str] = []
    chunks.append("# Trevas - TCK VTL 2.1\n\n")
    chunks.append("## TCK scripts output\n\n")
    chunks.append("Report is exported as artifact `tck-scripts-report`.\n\n")
    chunks.append("TCK scripts output\n")
    chunks.append(f"Source zip: `{zip_path}`\n\n")
    chunks.append(f"Total cases: {len(results)}\n\n")

    for row in results:
        i = row["index"]
        display_path = row["display_path"]
        zip_key = display_path_to_zip_key(display_path)
        path_slash = zip_key
        script = scripts.get(zip_key, "(script not found in zip for this path)")
        chunks.append(render_case_md(i, display_path, path_slash, script, row))

    report = "".join(chunks)
    FULL_REPORT_PATH.write_text(report, encoding="utf-8")

    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as out:
            out.write("\n\n---\n\n")
            out.write(report)


if __name__ == "__main__":
    main()
