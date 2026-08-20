from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

DB_PATH = os.getenv("DATABASE_PATH", "driftline.db")


class ContractCreate(BaseModel):
    dataset: str
    version: str
    required_columns: list[str] = []
    max_null_rate: dict[str, float] = {}
    min_rows: int = 0
    max_age_minutes: int | None = None


class ProfileCreate(BaseModel):
    dataset: str
    version: str
    row_count: int = Field(ge=0)
    columns: dict[str, dict[str, Any]]
    observed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class LineageEvent(BaseModel):
    event_id: str
    job: str
    inputs: list[str]
    outputs: list[str]
    event_type: str = "COMPLETE"


class CompareRequest(BaseModel):
    baseline_id: str
    candidate_id: str
    psi_threshold: float = 0.2
    js_threshold: float = 0.1
    ks_threshold: float = 0.2


@contextmanager
def db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    with db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS contracts(
          id TEXT PRIMARY KEY,dataset TEXT,version TEXT,document_json TEXT,created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS profiles(
          id TEXT PRIMARY KEY,dataset TEXT,version TEXT,document_json TEXT,created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS lineage(
          event_id TEXT PRIMARY KEY,job TEXT,inputs_json TEXT,outputs_json TEXT,
          event_type TEXT,created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS incidents(
          id TEXT PRIMARY KEY,dataset TEXT,severity TEXT,findings_json TEXT,
          affected_json TEXT,created_at TEXT
        );
        """)


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    yield


app = FastAPI(title="DriftLine", version="0.1.0", lifespan=lifespan)


@app.get("/healthz")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/readyz")
def ready() -> dict[str, str]:
    with db() as conn:
        conn.execute("SELECT 1")
    return {"status": "ready"}


@app.get("/metrics", response_class=Response)
def metrics() -> Response:
    with db() as conn:
        profiles = conn.execute("SELECT COUNT(*) FROM profiles").fetchone()[0]
        incident_count = conn.execute("SELECT COUNT(*) FROM incidents").fetchone()[0]
    body = f"driftline_profiles_total {profiles}\ndriftline_incidents_total {incident_count}\n"
    return Response(body, media_type="text/plain; version=0.0.4")


@app.post("/v1/contracts", status_code=201)
def create_contract(contract: ContractCreate) -> dict[str, str]:
    payload = canonical(contract.model_dump(mode="json"))
    identity = digest(f"{contract.dataset}:{contract.version}:{payload}")
    with db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO contracts VALUES(?,?,?,?,?)",
            (identity, contract.dataset, contract.version, payload, now()),
        )
    return {"id": identity, "dataset": contract.dataset, "version": contract.version}


@app.post("/v1/profiles", status_code=201)
def create_profile(profile: ProfileCreate) -> dict[str, Any]:
    payload = canonical(profile.model_dump(mode="json"))
    identity = digest(f"{profile.dataset}:{profile.version}:{payload}")
    with db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO profiles VALUES(?,?,?,?,?)",
            (identity, profile.dataset, profile.version, payload, now()),
        )
        contract_row = conn.execute(
            "SELECT * FROM contracts WHERE dataset=? ORDER BY created_at DESC LIMIT 1", (profile.dataset,)
        ).fetchone()
    violations = (
        contract_violations(json.loads(contract_row["document_json"]), profile.model_dump(mode="json"))
        if contract_row
        else []
    )
    return {"id": identity, "dataset": profile.dataset, "contract_violations": violations}


@app.post("/v1/lineage", status_code=202)
def lineage(event: LineageEvent) -> dict[str, Any]:
    with db() as conn:
        cursor = conn.execute(
            "INSERT OR IGNORE INTO lineage VALUES(?,?,?,?,?,?)",
            (
                event.event_id,
                event.job,
                canonical(event.inputs),
                canonical(event.outputs),
                event.event_type,
                now(),
            ),
        )
    return {"accepted": cursor.rowcount == 1, "event_id": event.event_id}


@app.post("/v1/compare")
def compare(request: CompareRequest) -> dict[str, Any]:
    with db() as conn:
        baseline = conn.execute("SELECT * FROM profiles WHERE id=?", (request.baseline_id,)).fetchone()
        candidate = conn.execute("SELECT * FROM profiles WHERE id=?", (request.candidate_id,)).fetchone()
        if not baseline or not candidate:
            raise HTTPException(404, "profile not found")
        if baseline["dataset"] != candidate["dataset"]:
            raise HTTPException(409, "profiles belong to different datasets")
        old, new = json.loads(baseline["document_json"]), json.loads(candidate["document_json"])
        findings = drift_findings(old, new, request)
        affected = affected_consumers(conn, baseline["dataset"])
        incident_id = digest(f"{request.baseline_id}:{request.candidate_id}:{canonical(findings)}")
        severity = (
            "high" if any(f["severity"] == "high" for f in findings) else "medium" if findings else "none"
        )
        if findings:
            conn.execute(
                "INSERT OR REPLACE INTO incidents VALUES(?,?,?,?,?,?)",
                (incident_id, baseline["dataset"], severity, canonical(findings), canonical(affected), now()),
            )
    return {
        "incident_id": incident_id if findings else None,
        "dataset": baseline["dataset"],
        "severity": severity,
        "findings": findings,
        "affected_consumers": affected,
    }


@app.get("/v1/incidents")
def incidents() -> list[dict[str, Any]]:
    with db() as conn:
        rows = conn.execute("SELECT * FROM incidents ORDER BY created_at DESC LIMIT 100").fetchall()
    return [
        {
            "id": r["id"],
            "dataset": r["dataset"],
            "severity": r["severity"],
            "findings": json.loads(r["findings_json"]),
            "affected_consumers": json.loads(r["affected_json"]),
            "created_at": r["created_at"],
        }
        for r in rows
    ]


@app.get("/v1/incidents/stream")
def incident_stream() -> StreamingResponse:
    def generate():
        yield f"event: snapshot\ndata: {json.dumps(incidents())}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


def contract_violations(contract: dict[str, Any], profile: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    columns = profile.get("columns", {})
    for column in contract.get("required_columns", []):
        if column not in columns:
            out.append({"kind": "missing_column", "column": column})
    if profile.get("row_count", 0) < contract.get("min_rows", 0):
        out.append(
            {
                "kind": "row_count_below_minimum",
                "actual": profile.get("row_count"),
                "expected": contract["min_rows"],
            }
        )
    for column, maximum in contract.get("max_null_rate", {}).items():
        actual = columns.get(column, {}).get("null_rate", 0)
        if actual > maximum:
            out.append(
                {"kind": "null_rate_exceeded", "column": column, "actual": actual, "expected": maximum}
            )
    return out


def drift_findings(
    old: dict[str, Any], new: dict[str, Any], thresholds: CompareRequest
) -> list[dict[str, Any]]:
    findings = []
    for column in sorted(set(old.get("columns", {})) & set(new.get("columns", {}))):
        left, right = old["columns"][column], new["columns"][column]
        if "distribution" in left and "distribution" in right:
            psi_value = psi(left["distribution"], right["distribution"])
            js_value = js_divergence(left["distribution"], right["distribution"])
            if psi_value >= thresholds.psi_threshold:
                findings.append(
                    {
                        "metric": "psi",
                        "column": column,
                        "value": round(psi_value, 6),
                        "threshold": thresholds.psi_threshold,
                        "severity": "high",
                    }
                )
            if js_value >= thresholds.js_threshold:
                findings.append(
                    {
                        "metric": "jensen_shannon",
                        "column": column,
                        "value": round(js_value, 6),
                        "threshold": thresholds.js_threshold,
                        "severity": "medium",
                    }
                )
        if "samples" in left and "samples" in right:
            value = ks_statistic(left["samples"], right["samples"])
            if value >= thresholds.ks_threshold:
                findings.append(
                    {
                        "metric": "ks",
                        "column": column,
                        "value": round(value, 6),
                        "threshold": thresholds.ks_threshold,
                        "severity": "high",
                    }
                )
    return findings


def psi(expected: list[float], actual: list[float]) -> float:
    e, a = probabilities(expected), probabilities(actual)
    return sum((y - x) * math.log(y / x) for x, y in zip(e, a, strict=True))


def js_divergence(left: list[float], right: list[float]) -> float:
    p, q = probabilities(left), probabilities(right)
    midpoint = [(x + y) / 2 for x, y in zip(p, q, strict=True)]
    return 0.5 * sum(x * math.log(x / m) for x, m in zip(p, midpoint, strict=True)) + 0.5 * sum(
        y * math.log(y / m) for y, m in zip(q, midpoint, strict=True)
    )


def probabilities(values: list[float]) -> list[float]:
    safe = [max(float(v), 1e-6) for v in values]
    total = sum(safe)
    return [value / total for value in safe]


def ks_statistic(left: list[float], right: list[float]) -> float:
    a, b = sorted(left), sorted(right)
    points = sorted(set(a + b))
    return max(
        abs(sum(x <= point for x in a) / len(a) - sum(x <= point for x in b) / len(b)) for point in points
    )


def affected_consumers(conn: sqlite3.Connection, dataset: str) -> list[dict[str, str]]:
    rows = conn.execute("SELECT * FROM lineage").fetchall()
    return [
        {"job": row["job"], "output": output}
        for row in rows
        if dataset in json.loads(row["inputs_json"])
        for output in json.loads(row["outputs_json"])
    ]


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:16]


def now() -> str:
    return datetime.now(UTC).isoformat()
