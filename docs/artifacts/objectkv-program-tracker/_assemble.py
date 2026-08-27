#!/usr/bin/env python3
"""Assemble the living objectKV program tracker from its body and experiment ledger."""

from __future__ import annotations

import html
import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[2]
BODY = ROOT / "body-objectkv-program-tracker.html"
CSS = ROOT / "_css.html"
OUTPUT = ROOT / "objectkv-program-tracker.html"
LEDGER = REPO / "experiments" / "ledger.jsonl"
GOLDEN_SCENARIO = REPO / "evals" / "scenarios" / "objectkv-golden-path-v1.toml"
GOLDEN_PROGRAM = REPO / "evals" / "programs" / "objectkv-golden-path-v1.toml"

H2_RE = re.compile(r'<h2 id="([^"]+)" data-toc>(.*?)</h2>', re.DOTALL)
TAG_RE = re.compile(r"<[^>]+>")
TABLE_BLOCK_RE = re.compile(r"(?ms)^\[\[([^]]+)\]\]\s*(.*?)(?=^\[\[|\Z)")


def git(*args: str) -> str:
    return subprocess.check_output(
        ["git", *args], cwd=REPO, text=True, stderr=subprocess.DEVNULL
    ).strip()


def ledger_rows(limit: int = 10) -> str:
    records: list[dict[str, object]] = []
    for line in LEDGER.read_text(encoding="utf-8").splitlines():
        if line.strip():
            records.append(json.loads(line))

    rows: list[str] = []
    for record in reversed(records[-limit:]):
        primary = record.get("primary_metric") or {}
        metric_name = primary.get("name", "recorded result") if isinstance(primary, dict) else "recorded result"
        median = primary.get("median") if isinstance(primary, dict) else None
        selected = primary.get("value") if isinstance(primary, dict) else None
        statistic = primary.get("statistic") if isinstance(primary, dict) else None
        metric = metric_name
        if selected is not None and statistic is not None:
            metric = f"{metric_name}.{statistic} = {selected:g}"
        elif median is not None:
            metric = f"{metric_name} = {median:g}"
        verdict = str(record.get("verdict", "unknown"))
        status_class, status_label = {
            "keep": ("verified", "VERIFIED"),
            "stop_incumbent_configuration": ("verified", "VERIFIED STOP"),
            "inconclusive": ("evaluating", "EVALUATING"),
            "superseded": ("evaluating", "EVALUATING"),
            "discard": ("evaluating", "EVALUATING"),
            "worse": ("evaluating", "EVALUATING"),
        }.get(verdict, ("proposed", "PROPOSED"))
        candidate = str(record.get("candidate_commit", "working tree"))[:10]
        run_id = str(
            record.get("run_id", "multiple" if record.get("run_ids") else "not recorded")
        )
        rows.append(
            "<tr>"
            f"<td>{html.escape(str(record.get('recorded_at', 'unknown'))[:10])}</td>"
            f"<td>{html.escape(str(record.get('lane', 'unknown')))}</td>"
            f"<td><span class=\"status {status_class}\">{status_label}</span></td>"
            f"<td>{html.escape(str(record.get('backend', 'unknown')))}</td>"
            f"<td><code>{html.escape(metric)}</code></td>"
            f"<td><code>{html.escape(run_id[:12])}</code><br><span class=\"muted\">{html.escape(candidate)}</span></td>"
            "</tr>"
        )
    return "\n".join(rows)


def quoted_field(block: str, name: str) -> str:
    match = re.search(rf'(?m)^{re.escape(name)}\s*=\s*"([^"]*)"\s*$', block)
    return match.group(1) if match else ""


def array_field(block: str, name: str) -> list[str]:
    match = re.search(rf'(?ms)^{re.escape(name)}\s*=\s*\[(.*?)\]\s*$', block)
    return re.findall(r'"([^"]+)"', match.group(1)) if match else []


def toml_blocks(path: Path, table: str) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for table_name, block in TABLE_BLOCK_RE.findall(path.read_text(encoding="utf-8")):
        if table_name != table:
            continue
        records.append(
            {
                "id": quoted_field(block, "id"),
                "name": quoted_field(block, "name"),
                "surface": quoted_field(block, "surface"),
                "checkpoint": quoted_field(block, "checkpoint"),
                "status": quoted_field(block, "status"),
                "outputs": array_field(block, "outputs"),
            }
        )
    return records


def golden_path_state() -> tuple[str, str]:
    checkpoints = toml_blocks(GOLDEN_SCENARIO, "checkpoints")
    gates = toml_blocks(GOLDEN_PROGRAM, "phases.gates")
    gate_by_checkpoint: dict[str, list[dict[str, object]]] = {}
    for gate in gates:
        gate_by_checkpoint.setdefault(str(gate["checkpoint"]), []).append(gate)

    rank = {"future": 0, "proposed": 1, "code_complete": 2, "evaluating": 3, "verified": 4}
    labels = {
        "verified": ("verified", "VERIFIED"),
        "code_complete": ("complete", "CODE COMPLETE"),
        "evaluating": ("evaluating", "EVALUATING"),
        "proposed": ("proposed", "PROPOSED"),
        "future": ("proposed", "FUTURE"),
    }
    rows: list[str] = []
    verified = 0
    for index, checkpoint in enumerate(checkpoints, start=1):
        checkpoint_gates = gate_by_checkpoint.get(str(checkpoint["id"]), [])
        status = max(
            (str(gate["status"]) for gate in checkpoint_gates),
            key=lambda value: rank.get(value, -1),
            default="future",
        )
        if status == "verified":
            verified += 1
        css_class, label = labels.get(status, ("proposed", status.upper()))
        gate_ids = ", ".join(str(gate["id"]) for gate in checkpoint_gates) or "unassigned"
        outputs = ", ".join(str(value) for value in checkpoint["outputs"])
        rows.append(
            "<tr>"
            f"<td>{index}</td>"
            f"<td><strong>{html.escape(str(checkpoint['name']))}</strong><br><span class=\"muted\"><code>{html.escape(str(checkpoint['id']))}</code></span></td>"
            f"<td>{html.escape(str(checkpoint['surface']))}</td>"
            f"<td><span class=\"status {css_class}\">{label}</span></td>"
            f"<td><code>{html.escape(gate_ids)}</code></td>"
            f"<td><span class=\"muted\">{html.escape(outputs)}</span></td>"
            "</tr>"
        )
    summary = f"{verified} / {len(checkpoints)} verified"
    return summary, "\n".join(rows)


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
    golden_summary, golden_rows = golden_path_state()
    body = body.replace("{{RECENT_RECEIPTS}}", ledger_rows())
    body = body.replace("{{GOLDEN_PATH_SUMMARY}}", html.escape(golden_summary))
    body = body.replace("{{GOLDEN_PATH_ROWS}}", golden_rows)
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
  <meta name="description" content="Living objectKV architecture, evidence, performance, and infrastructure tracker">
  <title>objectKV Program Tracker</title>
  {css}
</head>
<body>
  {body}
</body>
</html>
"""
    required = [
        "Highest verified rung",
        "Incumbent resident transaction plane",
        "Golden path levels",
        "Systems to infrastructure",
        "Recent measured receipts",
    ]
    if len(document) < 25_000 or any(item not in document for item in required):
        raise RuntimeError("assembled tracker failed content validation")
    return document


if __name__ == "__main__":
    output = render()
    OUTPUT.write_text(output, encoding="utf-8")
    print(f"wrote {OUTPUT} ({len(output):,} bytes)")
