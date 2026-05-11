#!/usr/bin/env python3
"""
After `mvn test` (+ optional prettify), build a plain-text-friendly Markdown report:

  TCK scripts output
  Source zip: …
  Total cases: …

  Per test (same shape for pass / skip / fail):
    ✅ Test N
    folder » with » chevrons » ex_k

    <script body from zip>

  Failures: one-line header `✅ Test N - path » …`; script in ```vtl`; detail
  without Java stacks or `--- inputs ---`; subtitles bold (datasets / Trevas / TCK);
  ASCII tables → GFM; `[path] output …` → **Cause: output …**.

Inputs:
  - Surefire XML (test order = execution order; names carry Test N + path).
  - v2.1.zip (transformation.vtl per folder path).

Writes: coverage/target/tck-scripts-report.md
Appends to GITHUB_STEP_SUMMARY when set (after existing summary from test-reporter).
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


def zip_path_for_report(zip_path: Path) -> str:
    try:
        return str(zip_path.relative_to(Path.cwd()))
    except ValueError:
        return str(zip_path)


def decode_xml_entities(text: str) -> str:
    out = text
    for _ in range(5):
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
    s = decode_xml_entities(name_attr).strip()
    if "\n" in s:
        lines = [ln.strip() for ln in s.split("\n") if ln.strip()]
        if len(lines) >= 2:
            m0 = re.match(r"^Test\s+(\d+)\s*$", lines[0])
            if m0:
                return int(m0.group(1)), lines[1].lstrip("\t").strip()
    m = re.search(r"Test\s+(\d+)\s+—\s+(.+)$", s)
    if m:
        return int(m.group(1)), m.group(2).strip()
    return None


def display_path_to_zip_key(display_path: str) -> str:
    return display_path.replace(_DISPLAY_SEP, "/")


def lookup_script(scripts: dict[str, str], display_path: str) -> str:
    """Resolve transformation.vtl folder key in zip (handles trailing duplicate segment, e.g. ex_1/ex_1)."""
    key = display_path_to_zip_key(display_path)
    if key in scripts:
        return scripts[key]
    parts = key.split("/")
    if len(parts) >= 2 and parts[-1] == parts[-2]:
        parent = "/".join(parts[:-1])
        if parent in scripts:
            return scripts[parent]
    for end in range(len(parts), 0, -1):
        prefix = "/".join(parts[:end])
        if prefix in scripts:
            return scripts[prefix]
    return "(script not found in zip for this path)"


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


def local_tag(elem: ET.Element) -> str:
    if elem.tag.startswith("{"):
        return elem.tag.split("}", 1)[1]
    return elem.tag


_FAILURE_OR_ERROR_TAGS = frozenset(
    {"failure", "error", "rerunFailure", "rerunError", "flakyFailure"}
)


def find_failure_or_error_node(tc: ET.Element) -> ET.Element | None:
    """JUnit5Xml30 / Surefire 3.x may nest failure/error under wrappers, not only direct children."""
    for el in tc.iter():
        if el is tc:
            continue
        if local_tag(el) in _FAILURE_OR_ERROR_TAGS:
            return el
    return None


def iter_surefire_testcases(root: ET.Element) -> list[ET.Element]:
    """Ordered testcase elements (JUnit5 may wrap testsuite(s))."""
    found: list[ET.Element] = []
    for parent in root.iter():
        if local_tag(parent) != "testsuite":
            continue
        for child in parent:
            if local_tag(child) == "testcase":
                found.append(child)
    if found:
        return found
    # Fallback: any testcase in document order (namespaced or alternate layout).
    return [e for e in root.iter() if local_tag(e) == "testcase"]


_ASSERTJ_MULTI = re.compile(r"^Multiple Failures\s*\([^)]+\)\s*$", re.IGNORECASE)
_ASSERTJ_FAILURE_HDR = re.compile(r"^--\s*failure\s+\d+\s*--\s*$", re.IGNORECASE)
_ASSERTJ_CLASS_HDR = re.compile(
    r"^\s*org\.assertj\..*AssertJMultipleFailuresError:\s*$", re.IGNORECASE
)


def strip_assertj_noise(text: str) -> str:
    """Drop AssertJMultipleFailures boilerplate lines."""
    lines = text.splitlines()
    kept: list[str] = []
    for line in lines:
        s = line.strip()
        if (
            _ASSERTJ_MULTI.match(s)
            or _ASSERTJ_FAILURE_HDR.match(s)
            or _ASSERTJ_CLASS_HDR.match(s)
        ):
            continue
        kept.append(line)
    return "\n".join(kept).strip()


def strip_embedded_tck_script_block(text: str) -> str:
    """Remove TckScriptText.appendFull section (already printed above from zip)."""
    key_vtl = "VTL script:"
    key_sep = "--- transformation.vtl ---"
    markers = ("--- inputs ---", "Trevas (actual):")
    i = text.find(key_vtl)
    if i == -1:
        return text
    start_cut = i
    if start_cut > 0 and text[start_cut - 1] == "\n":
        start_cut -= 1
        if start_cut > 0 and text[start_cut - 1] == "\r":
            start_cut -= 1

    j = text.find(key_sep, i)
    if j == -1:
        line_end = text.find("\n", i)
        if line_end == -1:
            return text[:start_cut].rstrip()
        tail = text[line_end + 1 :].lstrip("\n\r")
        return (text[:start_cut].rstrip() + ("\n\n" + tail if tail else "")).strip()

    after_sep = j + len(key_sep)
    while after_sep < len(text) and text[after_sep] in "\r\n":
        after_sep += 1

    next_pos: list[int] = []
    for mk in markers:
        k = text.find(mk, after_sep)
        if k != -1:
            next_pos.append(k)
    if not next_pos:
        head = text[:start_cut].rstrip()
        return head

    k = min(next_pos)
    tail = text[k:].lstrip("\n\r")
    head = text[:start_cut].rstrip()
    return (head + ("\n\n" + tail if tail else "")).strip()


def _split_ascii_pipe_row(line: str) -> list[str]:
    parts = [p.strip() for p in line.split("|")]
    while parts and parts[0] == "":
        parts.pop(0)
    while parts and parts[-1] == "":
        parts.pop()
    return parts


def _is_ascii_separator_row(line: str) -> bool:
    """Java renderTable separator is -----+-----+----- (no '|'); single-column is just dashes."""
    s = line.strip()
    if not s or "-" not in s or "|" in s:
        return False
    if "+" in s:
        return bool(re.fullmatch(r"[\s\-+]+", s))
    return bool(re.fullmatch(r"-+", s))


def _ascii_table_block_to_markdown(header_line: str, data_lines: list[str]) -> str | None:
    headers = _split_ascii_pipe_row(header_line)
    n = len(headers)
    if n < 2:
        return None

    def esc_cell(c: str) -> str:
        return c.replace("|", "\\|").replace("\n", " ").strip()

    rows_md = [
        "| " + " | ".join(esc_cell(h) for h in headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for raw in data_lines:
        cells = _split_ascii_pipe_row(raw)
        if len(cells) < n:
            cells = cells + [""] * (n - len(cells))
        cells = cells[:n]
        rows_md.append("| " + " | ".join(esc_cell(c) for c in cells) + " |")
    return "\n".join(rows_md)


def convert_ascii_tables_to_markdown(text: str) -> str:
    """Turn TCK ASCII pipe tables into GFM tables (renders nicely on GitHub)."""
    lines = text.splitlines()
    out: list[str] = []
    i = 0
    while i < len(lines):
        header = lines[i]
        if (
            "|" in header
            and not header.strip().startswith("|")
            and not _is_ascii_separator_row(header)
            and i + 1 < len(lines)
            and _is_ascii_separator_row(lines[i + 1])
        ):
            j = i + 2
            data: list[str] = []
            while j < len(lines):
                ln = lines[j]
                if not ln.strip():
                    break
                if "|" not in ln or _is_ascii_separator_row(ln):
                    break
                data.append(ln)
                j += 1
            md = _ascii_table_block_to_markdown(header, data)
            if md:
                out.append(md)
                out.append("")
                i = j
                continue
        out.append(lines[i])
        i += 1
    return "\n".join(out).strip()


_CAUSE_LINE = re.compile(r"^\s*\[[^\]]+\]\s+(.*)$")


_STACK_FRAME = re.compile(r"^\s*at\s+.+")
_STACK_MORE = re.compile(r"^\s*\.\.\.\s*\d+\s+more\b")
_SUPPRESSED_HDR = re.compile(r"^\s*Suppressed:\s*")


def strip_java_stack_trace(text: str) -> str:
    """Drop Java stack frames (lines starting with `at …`), Suppressed headers, '… more'."""
    lines = text.splitlines()
    out: list[str] = []
    for ln in lines:
        if (
            _SUPPRESSED_HDR.match(ln)
            or _STACK_FRAME.match(ln)
            or _STACK_MORE.match(ln)
            or ln.strip() == "... <omitted />"
        ):
            continue
        out.append(ln)
    return "\n".join(out)


def tidy_failure_section_headers(text: str) -> str:
    """Remove --- inputs ---; bold compact subtitles for datasets / Trevas / TCK."""
    t = re.sub(r"^--- inputs ---\s*\n", "", text, flags=re.MULTILINE | re.IGNORECASE)
    t = re.sub(r"^« ([^»]+) »\s*$", r"**« \1 »**", t, flags=re.MULTILINE)
    t = re.sub(r"^Trevas \(actual\):\s*$", "**Trevas (actual)**", t, flags=re.MULTILINE)
    t = re.sub(r"^TCK \(expected\):\s*$", "**TCK (expected)**", t, flags=re.MULTILINE)
    return t


def rewrite_failure_cause_summary(text: str) -> str:
    """[path] output `DS_r` — … → **Cause: output `DS_r` — …** (path already shown above)."""
    out_lines: list[str] = []
    for line in text.splitlines():
        m = _CAUSE_LINE.match(line)
        if m:
            rest = m.group(1).strip()
            if rest.startswith("output"):
                line = f"**Cause: {rest}**"
        out_lines.append(line)
    return "\n".join(out_lines)


def sanitize_failure_detail(detail: str) -> str:
    """Remove duplicated script and AssertJ noise; keep inputs/tables."""
    t = strip_assertj_noise(detail)
    # Some Surefire variants duplicate the payload (message + body).
    # Strip embedded script sections repeatedly until stable.
    while True:
        nxt = strip_embedded_tck_script_block(t)
        if nxt == t:
            break
        t = nxt
    t = strip_java_stack_trace(t)
    t = convert_ascii_tables_to_markdown(t)
    t = tidy_failure_section_headers(t)
    t = rewrite_failure_cause_summary(t)
    return re.sub(r"\n{3,}", "\n\n", t).strip()


def failure_or_error_text(node: ET.Element | None) -> str:
    if node is None:
        return ""
    msg = (node.attrib.get("message") or "").strip()
    body = "".join(node.itertext()).strip()
    typ = (node.attrib.get("type") or "").strip()
    # Prefer body: in Surefire/JUnit5 it usually contains the full useful failure payload.
    if body:
        return body
    if msg:
        return msg
    if typ:
        return typ
    return ""


def parse_ordered_results(xml_path: Path) -> list[dict[str, str]]:
    if not xml_path.exists():
        return []
    tree = ET.parse(xml_path)
    root = tree.getroot()
    out: list[dict[str, str]] = []
    for tc in iter_surefire_testcases(root):
        name = tc.attrib.get("name", "")
        parsed = split_testcase_name(name)
        if parsed is None:
            continue
        idx, display_path = parsed
        node = find_failure_or_error_node(tc)
        if node is not None:
            detail = failure_or_error_text(node)
            out.append(
                {
                    "index": idx,
                    "display_path": display_path,
                    "status": "FAIL",
                    "detail": detail or "Unknown failure",
                }
            )
        else:
            skipped = any(
                local_tag(el) == "skipped" for el in tc.iter() if el is not tc
            )
            out.append(
                {
                    "index": idx,
                    "display_path": display_path,
                    "status": "SKIP" if skipped else "PASS",
                    "detail": "",
                }
            )
    return out


def fenced_vtl(script: str) -> str:
    body = (script.strip() if script else "(empty)").rstrip()
    return "```vtl\n" + body + "\n```"


def render_case_plain(i: int, display_path: str, script: str, row: dict[str, str]) -> str:
    if row["status"] == "PASS":
        mark = "✅"
    elif row["status"] == "SKIP":
        mark = "⏭️"
    else:
        mark = "❌"
    lines: list[str] = [f"{mark} Test {i} - {display_path}", "", fenced_vtl(script)]
    if row["status"] == "FAIL":
        cleaned = sanitize_failure_detail(row["detail"].strip())
        if cleaned:
            lines.extend(["", cleaned])
    lines.extend(["", ""])
    return "\n".join(lines)


def main() -> None:
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    zip_path = resolve_tck_zip()

    FULL_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)

    if zip_path is None:
        msg = (
            "TCK scripts output\n\n"
            "Source zip: (not found)\n\n"
            "Expected one of:\n"
            + "\n".join(f"  - {p}" for p in TCK_ZIP_CANDIDATES)
            + "\n"
        )
        FULL_REPORT_PATH.write_text(msg, encoding="utf-8")
        if summary_path:
            with open(summary_path, "a", encoding="utf-8") as out:
                out.write("\n\n---\n\n")
                out.write(msg)
        return

    scripts = load_scripts_from_zip(zip_path)
    results = parse_ordered_results(SUREFIRE_XML_PATH)

    chunks: list[str] = []
    chunks.append("TCK scripts output\n")
    chunks.append("")
    chunks.append(f"Source zip: {zip_path_for_report(zip_path)}\n")
    chunks.append("")
    chunks.append(f"Total cases: {len(results)}\n")
    chunks.append("")

    for row in results:
        i = row["index"]
        display_path = row["display_path"]
        script = lookup_script(scripts, display_path)
        chunks.append(render_case_plain(i, display_path, script, row))

    report = "".join(chunks).rstrip() + "\n"

    FULL_REPORT_PATH.write_text(report, encoding="utf-8")

    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as out:
            out.write("\n\n---\n\n")
            out.write(report)


if __name__ == "__main__":
    main()
