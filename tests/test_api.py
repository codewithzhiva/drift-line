import os
from pathlib import Path
from uuid import uuid4

os.environ["DATABASE_PATH"] = str(Path(__file__).parent / "test.db")
from fastapi.testclient import TestClient

from app.main import app


def profile(version, distribution, samples):
    return {
        "dataset": "features.customer_risk",
        "version": version,
        "row_count": 1000,
        "columns": {"score": {"null_rate": 0, "distribution": distribution, "samples": samples}},
    }


def test_contract_drift_and_lineage_impact():
    with TestClient(app) as client:
        client.post(
            "/v1/contracts",
            json={
                "dataset": "features.customer_risk",
                "version": "1",
                "required_columns": ["score"],
                "max_null_rate": {"score": 0.01},
                "min_rows": 500,
            },
        )
        client.post(
            "/v1/lineage",
            json={
                "event_id": "run-1",
                "job": "train-fraud-model",
                "inputs": ["features.customer_risk"],
                "outputs": ["models.fraud.v2"],
            },
        )
        old = client.post("/v1/profiles", json=profile("1", [90, 10], [1, 2, 3, 4, 5])).json()["id"]
        new = client.post("/v1/profiles", json=profile("2", [20, 80], [50, 60, 70, 80, 90])).json()["id"]
        report = client.post("/v1/compare", json={"baseline_id": old, "candidate_id": new}).json()
        assert report["severity"] == "high"
        assert {f["metric"] for f in report["findings"]} >= {"psi", "ks"}
        assert report["affected_consumers"][0]["job"] == "train-fraud-model"


def test_lineage_is_idempotent():
    event = {"event_id": f"duplicate-{uuid4()}", "job": "job", "inputs": [], "outputs": []}
    with TestClient(app) as client:
        assert client.post("/v1/lineage", json=event).json()["accepted"] is True
        assert client.post("/v1/lineage", json=event).json()["accepted"] is False
