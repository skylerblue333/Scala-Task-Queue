"""Metric anomaly analysis primitive for the SKYCOIN4444 ecosystem."""

import time
from fastapi import FastAPI
from pydantic import BaseModel, Field

app = FastAPI(title="Scala-Task-Queue", version="3.1.0")


class DataPoint(BaseModel):
    metric: str = Field(min_length=1, max_length=200)
    value: float
    threshold: float = Field(gt=0)


@app.post("/api/v1/analyze")
def analyze(point: DataPoint):
    deviation = abs(point.value - point.threshold)
    is_anomaly = abs(point.value) > point.threshold
    severity = "high" if deviation > point.threshold * 0.5 else "low"
    return {
        "metric": point.metric.strip(),
        "is_anomaly": is_anomaly,
        "deviation": round(deviation, 4),
        "severity": severity,
    }


@app.get("/health")
def health():
    return {
        "status": "healthy",
        "service": "Scala-Task-Queue",
        "timestamp": int(time.time()),
    }
