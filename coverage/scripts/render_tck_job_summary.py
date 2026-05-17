#!/usr/bin/env python3
"""
After `mvn test` (+ optional prettify), build a plain-text-friendly Markdown report:

  TCK scripts output
  Source zip: …
  Total cases: …

  Per test (same shape for pass / skip / fail):
    ✅ Test N
    folder » with » chevrons » ex_k

  N is 1..total in report order (default: natural alphanumeric on the path).
  JUnit parameterized index is not shown (see Surefire XML / CI for Test 162, …).

    <script body from zip>

  Failures: one-line header `✅ Test N - path » …`; script in ```vtl`; detail
  without Java stacks or `--- inputs ---`; subtitles bold (datasets / Trevas / TCK);
  ASCII tables → GFM; `[path] output …` → **Cause: output …**.

Inputs:
  - Surefire XML (names carry Test N + path).
  - v2.1.zip (transformation.vtl per folder path).

Report order: natural alphanumeric sort on the case path (folder » … » ex_k).
Case labels Test 1, Test 2, … follow that order (not the JUnit zip-tree index).
Override with env TCK_REPORT_SORT=index to sort and number by JUnit Test N instead.

Writes: coverage/target/tck-scripts-report.md
Appends to GITHUB_STEP_SUMMARY when set (after existing summary from test-reporter).

Optional env TCK_REPORT_TITLE: prepended as Markdown H1 (e.g. TCK VTL v2.1 (Spark 4)).
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


def natural_sort_key(text: str) -> list:
    """Case-insensitive natural sort key (ex_2 before ex_10)."""
    return [int(part) if part.isdigit() else part.lower() for part in re.split(r"(\d+)", text)]


def sort_results_by_index(results: list[dict]) -> None:
    """Sort cases by Trevas test number (JUnit parameterized order from the TCK zip tree)."""
    results.sort(key=lambda row: row["index"])


def sort_results_by_display_path(results: list[dict]) -> None:
    """Sort cases by label path (e.g. General purpose operators » … » ex_5)."""
    results.sort(
        key=lambda row: (natural_sort_key(row["display_path"]), row["index"])
    )


def apply_report_sort(results: list[dict]) -> None:
    mode = os.environ.get("TCK_REPORT_SORT", "path").strip().lower()
    if mode in ("index", "test", "number"):
        sort_results_by_index(results)
    elif mode in ("path", "alpha", "alphanumeric", "natural"):
        sort_results_by_display_path(results)
    else:
        raise ValueError(
            f"unsupported TCK_REPORT_SORT={mode!r} (use path or index)"
        )


def assign_report_numbers(results: list[dict]) -> None:
    """Set report_index 1..n in current list order (after apply_report_sort)."""
    for n, row in enumerate(results, start=1):
        row["report_index"] = n


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
_EXCEPTION_HEAD = re.compile(
    r"^([a-zA-Z][\w$]*(?:\.[a-zA-Z][\w$]*)*(?:Exception|Error))(?:\s*:\s*(.*))?$"
)
_CAUSED_BY = re.compile(r"^Caused by:\s*(.*)$", re.IGNORECASE)
_FRAME_TREVAS = re.compile(r"^\s*at\s+(?:fr\.insee\.vtl\.|javax\.script\.)")
_FRAME_OMITTED = re.compile(r"^\s*…\s*\(.*omitted.*\)\s*$", re.IGNORECASE)
_NUMBERED_TREVAS_FRAME = re.compile(
    r"^\s*\d+\.\s+fr\.insee\.vtl\.[\w$.]+\.[\w$]+(?:\s*<[^>]+>)?\s+—\s+.+$",
)
_TREVAS_STACK_HDR = re.compile(
    r"^Trevas stack(?: \(file:line\))?:", re.IGNORECASE
)
_ASSERTION_ERROR_HDR = re.compile(r"^java\.lang\.AssertionError:\s*$")


def _already_structured_failure(text: str) -> bool:
    return (
        "output `" in text
        or "script execution failed" in text
        or "row data differs" in text
        or "data structure differs" in text
    )


def compact_java_exception_detail(text: str, max_frames: int = 5) -> str:
    """
    Collapse raw Java stack dumps to type, message, cause chain, and Trevas/script frames.
    Skips when TckFailureText already formatted the failure.
    """
    if not text.strip() or _already_structured_failure(text):
        return text

    lines = text.splitlines()
    blocks: list[str] = []
    current_exc: list[str] = []
    frames: list[str] = []

    def flush_block() -> None:
        nonlocal current_exc, frames
        if not current_exc and not frames:
            return
        block_lines = list(current_exc)
        block_lines.extend(frames[:max_frames])
        if len(frames) > max_frames:
            block_lines.append(f"    … ({len(frames) - max_frames} more frames omitted)")
        blocks.append("\n".join(block_lines))
        current_exc = []
        frames = []

    for ln in lines:
        s = ln.strip()
        if not s:
            continue
        if _SUPPRESSED_HDR.match(ln) or _STACK_MORE.match(ln) or s == "... <omitted />":
            continue
        m_cb = _CAUSED_BY.match(ln)
        if m_cb:
            flush_block()
            current_exc.append(f"Caused by: {m_cb.group(1).strip()}")
            continue
        m_ex = _EXCEPTION_HEAD.match(s)
        if m_ex:
            flush_block()
            head = m_ex.group(1)
            msg = (m_ex.group(2) or "").strip()
            current_exc.append(f"{head}: {msg}" if msg else head)
            continue
        if _FRAME_TREVAS.match(ln) or _FRAME_OMITTED.match(ln):
            frames.append(ln.rstrip())
            continue
        if _STACK_FRAME.match(ln):
            continue
        if s.startswith("org.assertj.") and "Error" in s:
            continue
        if current_exc or frames:
            flush_block()
        blocks.append(ln.rstrip())

    flush_block()
    if not blocks:
        return text

    summary = "\n\n".join(blocks)
    if len(summary.strip()) < 12 and text.strip():
        return text
    return "**Error**\n\n```text\n" + summary.strip() + "\n```"


def _keep_stack_line(ln: str) -> bool:
    """Keep Trevas locations and our numbered stack; drop generic Java/Spark noise."""
    if _NUMBERED_TREVAS_FRAME.match(ln):
        return True
    if _TREVAS_STACK_HDR.match(ln):
        return True
    if _FRAME_TREVAS.match(ln):
        return True
    if _FRAME_OMITTED.match(ln):
        return True
    if ln.strip().startswith("Exception:") or ln.strip().startswith("Root cause:"):
        return True
    return False


def strip_java_stack_trace(text: str) -> str:
    """Drop Java stack frames unless they are Trevas locations we want in the report."""
    lines = text.splitlines()
    out: list[str] = []
    for ln in lines:
        if (
            _SUPPRESSED_HDR.match(ln)
            or _STACK_MORE.match(ln)
            or ln.strip() == "... <omitted />"
        ):
            continue
        if _STACK_FRAME.match(ln):
            if _keep_stack_line(ln):
                out.append(ln.rstrip())
            continue
        out.append(ln)
    return "\n".join(out)


def peel_assertion_error_wrapper(text: str) -> str:
    """Surefire often prefixes AssertionError before our formatted execution failure."""
    lines = text.splitlines()
    out: list[str] = []
    seen_execution_failed = False
    for ln in lines:
        if _ASSERTION_ERROR_HDR.match(ln.strip()):
            continue
        if "script execution failed" in ln:
            seen_execution_failed = True
        if (
            seen_execution_failed
            and ln.strip().startswith("Caused by:")
            and "Exception" in ln
        ):
            continue
        out.append(ln)
    return "\n".join(out).strip()


def highlight_trevas_stack_headers(text: str) -> str:
    return re.sub(
        r"^(Trevas stack(?: \(file:line\))?:)\s*$",
        r"**\1**",
        text,
        flags=re.MULTILINE,
    )


def tidy_failure_section_headers(text: str) -> str:
    """Remove --- inputs ---; bold compact subtitles for datasets / Trevas / TCK."""
    t = re.sub(
        r"^Exception:\s*$",
        "**Exception**",
        text,
        flags=re.MULTILINE,
    )
    t = re.sub(
        r"^Root cause:\s*",
        "**Root cause:** ",
        t,
        flags=re.MULTILINE,
    )
    t = re.sub(
        r"^\[([^\]]+)\] script execution failed\s*$",
        r"**\1 — script execution failed**",
        t,
        flags=re.MULTILINE,
    )
    t = re.sub(r"^--- inputs ---\s*\n", "", t, flags=re.MULTILINE | re.IGNORECASE)
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
    t = peel_assertion_error_wrapper(detail)
    t = strip_assertj_noise(t)
    # Some Surefire variants duplicate the payload (message + body).
    # Strip embedded script sections repeatedly until stable.
    while True:
        nxt = strip_embedded_tck_script_block(t)
        if nxt == t:
            break
        t = nxt
    t = compact_java_exception_detail(t)
    t = strip_java_stack_trace(t)
    t = highlight_trevas_stack_headers(t)
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
            if cleaned.startswith("**") or cleaned.startswith("```"):
                lines.extend(["", cleaned])
            else:
                lines.extend(["", "```text", cleaned, "```"])
    lines.extend(["", ""])
    return "\n".join(lines)


def main() -> None:
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    zip_path = resolve_tck_zip()
    report_title = os.environ.get("TCK_REPORT_TITLE", "").strip()

    FULL_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)

    if zip_path is None:
        msg = ""
        if report_title:
            msg += f"# {report_title}\n\n"
        msg += (
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
    apply_report_sort(results)
    assign_report_numbers(results)

    chunks: list[str] = []
    if report_title:
        chunks.append(f"# {report_title}\n\n")
    chunks.append("TCK scripts output\n")
    chunks.append("")
    chunks.append(f"Source zip: {zip_path_for_report(zip_path)}\n")
    chunks.append("")
    chunks.append(f"Total cases: {len(results)}\n")
    chunks.append("")

    for row in results:
        display_path = row["display_path"]
        script = lookup_script(scripts, display_path)
        chunks.append(
            render_case_plain(row["report_index"], display_path, script, row)
        )

    report = "".join(chunks).rstrip() + "\n"

    FULL_REPORT_PATH.write_text(report, encoding="utf-8")

    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as out:
            out.write("\n\n---\n\n")
            out.write(report)


if __name__ == "__main__":
    main()
