#!/usr/bin/env python3
"""Assemble the self-contained objectKV architecture tracker."""

from __future__ import annotations

import html
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[2]
BODY = ROOT / "body-objectkv-architecture.html"
CSS = ROOT / "_css.html"
OUTPUT = ROOT / "objectkv-architecture.html"

H2_RE = re.compile(r'<h2 id="([^"]+)" data-toc>(.*?)</h2>', re.DOTALL)
TAG_RE = re.compile(r"<[^>]+>")


def git(*args: str) -> str:
    return subprocess.check_output(
        ["git", *args], cwd=REPO, text=True, stderr=subprocess.DEVNULL
    ).strip()


def add_toc(body: str) -> tuple[str, str]:
    entries: list[str] = []
    counter = 0

    def replace(match: re.Match[str]) -> str:
        nonlocal counter
        counter += 1
        section_id, raw_title = match.groups()
        title = html.unescape(TAG_RE.sub("", raw_title)).strip()
        label = f"{counter:02d}"
        entries.append(
            f'<a href="#{section_id}"><span>{label}</span>{html.escape(title)}</a>'
        )
        return f'<h2 id="{section_id}"><span class="section-id">[{label}]</span>{raw_title}</h2>'

    return H2_RE.sub(replace, body), "\n".join(entries)


def render() -> str:
    body = BODY.read_text(encoding="utf-8")
    body = body.replace("{{REVISION}}", html.escape(git("rev-parse", "--short", "HEAD")))
    body = body.replace("{{BRANCH}}", html.escape(git("branch", "--show-current")))
    body = body.replace(
        "{{GENERATED_AT}}",
        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    )
    body, toc = add_toc(body)
    body = body.replace("{{TOC}}", toc)
    css = CSS.read_text(encoding="utf-8")
    document = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="description" content="Living objectKV implementation architecture and infrastructure evidence boundary">
  <title>objectKV Architecture</title>
  {css}
</head>
<body>
  {body}
</body>
</html>
"""
    required = [
        "Architecture today",
        "Read pipeline",
        "Write and publication pipeline",
        "Infrastructure evidence",
        "Real workload admission",
        "Target cell",
    ]
    if len(document) < 28_000 or any(item not in document for item in required):
        raise RuntimeError("assembled architecture tracker failed content validation")
    return document


if __name__ == "__main__":
    output = render()
    OUTPUT.write_text(output, encoding="utf-8")
    print(f"wrote {OUTPUT} ({len(output):,} characters)")
