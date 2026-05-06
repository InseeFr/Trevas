#!/usr/bin/env python3
"""
Rewrite Surefire TEST-*.xml testcase names for TCK so GitHub's java-junit reporter shows:

  Test 76
  \tClause operators » ...

instead of a single technical line like tckCase(TckCase) Test 76 — …

Run after mvn test, before dorny/test-reporter. Safe no-op if reports are missing.
"""

from __future__ import annotations

import glob
import os
import re


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


if __name__ == "__main__":
    main()
