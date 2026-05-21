#!/usr/bin/env python3
"""
Rewrite Surefire TEST-*.xml testcase names for TCK so GitHub's java-junit reporter shows:

  Test 76
  \tClause operators » ...

instead of a single technical line like tckCase(TckCase) Test 76 — …

Also reorders <testcase> elements by natural alphanumeric order of the display path
(same order as coverage/target/tck-scripts-report.md).

Run after mvn test, before dorny/test-reporter. Safe no-op if reports are missing.
"""

from __future__ import annotations

import glob
import os
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

from render_tck_job_summary import (  # noqa: E402
    natural_sort_key,
    split_testcase_name,
)


def escape_xml_attr_value(value: str) -> str:
    s = value.replace("&", "&amp;").replace('"', "&quot;").replace("<", "&lt;")
    return s.replace("\n", "&#10;").replace("\r", "&#13;").replace("\t", "&#9;")


def prettify_testcase_name(name: str) -> str | None:
    # Current Trevas (phrased Surefire): method(TckCase) Test N — path
    m = re.match(r"^(\w+)\(TckCase\)\s+(Test\s+\d+)\s+—\s+(.+)$", name)
    if m:
        _method, idx, path = m.groups()
        return f"{idx}\n\t{path}"
    # Older / alternate phrasing still carrying "Test N — path"
    m = re.match(r"^(\w+)\([^)]*\)\s+(Test\s+\d+)\s+—\s+(.+)$", name)
    if m:
        _method, idx, path = m.groups()
        return f"{idx}\n\t{path}"
    return None


def _local_tag(elem: ET.Element) -> str:
    if elem.tag.startswith("{"):
        return elem.tag.split("}", 1)[1]
    return elem.tag


def _testcase_sort_key(tc: ET.Element) -> list:
    name = tc.attrib.get("name", "")
    parsed = split_testcase_name(name)
    if parsed is not None:
        return natural_sort_key(parsed[1])
    return natural_sort_key(name)


def sort_testcases_in_testsuite(parent: ET.Element) -> bool:
    children = list(parent)
    testcases = [c for c in children if _local_tag(c) == "testcase"]
    if len(testcases) < 2:
        return False
    sorted_testcases = sorted(testcases, key=_testcase_sort_key)
    if sorted_testcases == testcases:
        return False
    tc_iter = iter(sorted_testcases)
    new_children: list[ET.Element] = []
    for child in children:
        if _local_tag(child) == "testcase":
            new_children.append(next(tc_iter))
        else:
            new_children.append(child)
    for child in children:
        parent.remove(child)
    for child in new_children:
        parent.append(child)
    return True


def sort_testcases_in_file(path: str) -> bool:
    tree = ET.parse(path)
    root = tree.getroot()
    changed = False
    for parent in root.iter():
        if _local_tag(parent) == "testsuite" and sort_testcases_in_testsuite(parent):
            changed = True
    if changed:
        tree.write(path, encoding="utf-8", xml_declaration=True)
    return changed


def rewrite_file(path: str) -> bool:
    with open(path, encoding="utf-8") as f:
        text = f.read()

    def repl(match: re.Match[str]) -> str:
        original = match.group(1)
        new = prettify_testcase_name(original)
        if new is None:
            return match.group(0)
        return f'<testcase name="{escape_xml_attr_value(new)}"'

    new_text, n = re.subn(
        r'<testcase name="([^"]*)"',
        repl,
        text,
        flags=re.MULTILINE,
    )
    if n == 0:
        return False
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(new_text)
    return True


def main() -> None:
    base = "coverage/target/surefire-reports"
    if not os.path.isdir(base):
        return
    for path in sorted(glob.glob(os.path.join(base, "TEST-*.xml"))):
        if "TCKTest" not in os.path.basename(path):
            continue
        rewrite_file(path)
        sort_testcases_in_file(path)


if __name__ == "__main__":
    main()
