#!/usr/bin/env python3
"""Render the canonical objectKV product spec Markdown as a standalone HTML report."""

from __future__ import annotations

import html
import re
from pathlib import Path

import markdown


ROOT = Path(__file__).resolve().parent
SOURCE = ROOT.parents[1] / "PRODUCT-SPEC.md"
BODY_TEMPLATE = ROOT / "body-objectkv-product-spec.html"
CSS = ROOT / "_css.html"
OUTPUT = ROOT / "objectkv-product-spec.html"

H2_RE = re.compile(r'<h2 id="([^"]+)">(.*?)</h2>', re.DOTALL)
TAG_RE = re.compile(r"<[^>]+>")


DIAGRAMS = {
    "product-boundary": """
<div class="architecture-diagram" role="img" aria-label="objectKV product boundary from consumers through transactional ranges to object storage and analytical serving">
  <div class="flow-stack">
    <div class="flow-node">PostgreSQL · Redis · Search · custom applications<small>Consumer semantics</small></div>
    <div class="flow-arrow">↓</div>
    <div class="flow-node">Ordered transactional KV API<small>Keys · opaque values · snapshots · bounded transactions</small></div>
    <div class="flow-arrow">↓</div>
    <div class="flow-node">Cell transaction layer<small>Start version · validation · commit decision</small></div>
    <div class="flow-arrow">↓</div>
    <div class="node-grid">
      <div class="flow-node">Range group A<small>Raft + RocksDB serving image</small></div>
      <div class="flow-node">Range group B<small>Raft + RocksDB serving image</small></div>
    </div>
    <div class="flow-arrow">↓</div>
    <div class="flow-node">Asynchronous objectification<small>Immutable open object row bases</small></div>
    <div class="flow-arrow">↓</div>
    <div class="node-grid">
      <div class="flow-node">Row serving and recovery</div>
      <div class="flow-node">Table change projection → Parquet / Vortex → DataFusion</div>
    </div>
  </div>
</div>
""",
    "commit-pipeline": """
<div class="architecture-diagram" role="img" aria-label="Proposed objectKV commit pipeline">
  <ol class="flow-steps">
    <li>Acquire start and read version <code>S</code>.</li>
    <li>Read keys and declare read and write conflict ranges.</li>
    <li>Route mutations to affected range groups.</li>
    <li>Validate and quorum-persist each prewrite or intent.</li>
    <li>Choose cell commit version <code>C</code>.</li>
    <li>Durably record one crash-resolvable transaction decision.</li>
    <li>Expose committed versions through <code>C</code>.</li>
    <li>Asynchronously advance row-base watermarks.</li>
  </ol>
</div>
""",
    "point-read": """
<div class="architecture-diagram" role="img" aria-label="Point-read path across routing, RAM, RocksDB, object cache, and object storage">
  <div class="flow-node">Logical key + exact snapshot <code>T</code> → tenant and range router → transaction-local write overlay</div>
  <div class="flow-arrow">↓</div>
  <div class="tier-grid">
    <div class="tier"><span class="tier-num">01</span><h4>RAM</h4><p>Row and decoded block cache.</p></div>
    <div class="tier"><span class="tier-num">02</span><h4>RocksDB serving image</h4><p>Newest visible local value or tombstone at <code>T</code>.</p></div>
    <div class="tier"><span class="tier-num">03</span><h4>NVMe object cache</h4><p>Manifest-selected immutable block.</p></div>
    <div class="tier"><span class="tier-num">04</span><h4>Object range GET</h4><p>Explicit elastic miss path, never disguised as local latency.</p></div>
  </div>
  <p class="diagram-caption">A complete resident image can make a RocksDB miss authoritative. Elastic ranges continue to the selected object row base.</p>
</div>
""",
    "dual-history": """
<div class="architecture-diagram" role="img" aria-label="One commit history projected into separate row and columnar serving paths">
  <div class="flow-stack">
    <div class="flow-node">One commit history at target version <code>T</code></div>
    <div class="flow-arrow">↓</div>
    <div class="node-grid">
      <div class="flow-node">OLTP row path<small>Row object base at <code>Oᵣ</code> + row overlay <code>(Oᵣ, T]</code></small></div>
      <div class="flow-node">OLAP columnar path<small>Columnar base at <code>O꜀</code> + analytical tail <code>(O꜀, T]</code></small></div>
    </div>
  </div>
</div>
""",
}


def replace_diagram_blocks(source: str) -> str:
    blocks = [
        ("PostgreSQL / Redis / Search / custom applications", "product-boundary"),
        ("client transaction", "commit-pipeline"),
        ("logical key + T", "point-read"),
        ("commit history", "dual-history"),
    ]
    for marker, name in blocks:
        pattern = re.compile(r"```text\n([\s\S]*?)\n```", re.MULTILINE)
        match = next((candidate for candidate in pattern.finditer(source) if marker in candidate.group(1)), None)
        if match is None:
            raise RuntimeError(f"diagram block not found: {marker}")
        source = source[: match.start()] + f"\n@@DIAGRAM:{name}@@\n" + source[match.end() :]
    return source


def strip_title_and_lede(rendered: str) -> str:
    rendered = re.sub(r"\A<h1[^>]*>.*?</h1>\s*", "", rendered, count=1, flags=re.DOTALL)
    rendered = re.sub(r"\A<p>.*?</p>\s*", "", rendered, count=1, flags=re.DOTALL)
    return rendered


def add_section_anchors(rendered: str) -> tuple[str, str]:
    entries: list[str] = []
    counter = 0

    def replace(match: re.Match[str]) -> str:
        nonlocal counter
        counter += 1
        section_id, raw_title = match.groups()
        clean_title = html.unescape(TAG_RE.sub("", raw_title)).strip()
        label = f"{counter:02d}"
        entries.append(
            f'<a href="#{section_id}"><span class="num">{label}</span><span>{html.escape(clean_title)}</span></a>'
        )
        return f'<h2 id="{section_id}"><span class="section-anchor">[{label}]</span>{raw_title}</h2>'

    return H2_RE.sub(replace, rendered), "\n".join(entries)


def decorate_statuses(rendered: str) -> str:
    classes = {
        "CODE-COMPLETE": "state-code-complete",
        "VERIFIED": "state-verified",
        "EVALUATING": "state-evaluating",
        "PROPOSED": "state-proposed",
        "FUTURE": "state-future",
    }
    for state, class_name in classes.items():
        rendered = rendered.replace(
            f"<code>[{state}]</code>",
            f'<span class="state-token {class_name}">{state}</span>',
        )
    rendered = re.sub(
        r"<tr>\s*<td>(D\d+)</td>\s*<td>(audited|unaudited)</td>",
        lambda match: (
            f'<tr id="{match.group(1)}"><td>{match.group(1)}</td>'
            f'<td><span class="audit-status audit-{match.group(2)}">{match.group(2)}</span></td>'
        ),
        rendered,
    )
    return rendered


def render() -> str:
    source = SOURCE.read_text(encoding="utf-8")
    if len(source) < 15_000:
        raise RuntimeError("canonical product spec is unexpectedly short")

    title_match = re.search(r"^#\s+(.+)$", source, re.MULTILINE)
    lede_match = re.search(r"^#\s+[^\n]+\n\n(.+?)(?=\n\n)", source, re.MULTILINE | re.DOTALL)
    if not title_match or not lede_match:
        raise RuntimeError("title or lede missing from canonical product spec")

    title = title_match.group(1).strip()
    lede = " ".join(line.strip() for line in lede_match.group(1).splitlines())
    source = replace_diagram_blocks(source)
    rendered = markdown.markdown(
        source,
        extensions=["tables", "fenced_code", "toc", "sane_lists"],
        output_format="html5",
    )
    rendered = strip_title_and_lede(rendered)

    for name, diagram in DIAGRAMS.items():
        rendered = rendered.replace(f"<p>@@DIAGRAM:{name}@@</p>", diagram.strip())

    rendered = decorate_statuses(rendered)
    rendered, toc = add_section_anchors(rendered)
    rendered = rendered.replace("<table>", '<div class="table-shell"><table>')
    rendered = rendered.replace("</table>", "</table></div>")

    template = BODY_TEMPLATE.read_text(encoding="utf-8")
    body = (
        template.replace("{{TITLE}}", html.escape(title))
        .replace("{{LEDE}}", html.escape(lede))
        .replace("{{TOC}}", toc)
        .replace("{{CONTENT}}", rendered)
    )
    css = CSS.read_text(encoding="utf-8")
    document = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="description" content="objectKV product specification and architecture review">
  <title>{html.escape(title)}</title>
  {css}
</head>
<body>
  <main>{body}</main>
</body>
</html>
"""
    if len(document) < 30_000 or "Current architectural call" not in document:
        raise RuntimeError("assembled HTML failed the content sanity check")
    return document


if __name__ == "__main__":
    document = render()
    OUTPUT.write_text(document, encoding="utf-8")
    print(f"wrote {OUTPUT} ({len(document):,} bytes)")
