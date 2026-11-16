import json
import logging
import os
import time
import uuid
from typing import Any

import mlflow
import mlflow.sklearn
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from mlflow import MlflowClient
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
from pydantic import BaseModel
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("property-api")

MODEL_NAME = os.getenv("MODEL_NAME", "property_price_model")
MODEL_ALIAS = os.getenv("MODEL_ALIAS", "champion")
MODEL_URI = f"models:/{MODEL_NAME}@{MODEL_ALIAS}"

PREDICTIONS_TOTAL = Counter("property_predictions_total", "Predicciones de precio", ["status"])
PREDICTION_LATENCY = Histogram("property_prediction_latency_seconds", "Latencia de predicción")
PREDICTION_ERRORS_TOTAL = Counter("property_prediction_errors_total", "Errores", ["reason"])
MODEL_LOADED = Gauge("property_model_loaded", "Modelo champion cargado")

state: dict[str, Any] = {"model": None, "model_version": None, "expected_columns": []}


class PredictionRequest(BaseModel):
    features: dict[str, Any]


def database_url():
    return (
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:"
        f"{os.environ['POSTGRES_PASSWORD']}@postgres:5432/mlops"
    )


engine = create_engine(database_url(), pool_pre_ping=True)


def load_champion():
    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
    champion = MlflowClient().get_model_version_by_alias(MODEL_NAME, MODEL_ALIAS)
    model = mlflow.sklearn.load_model(MODEL_URI)
    expected_columns = list(getattr(model, "feature_names_in_", []))
    if not expected_columns:
        raise RuntimeError("No fue posible obtener las variables esperadas del modelo.")

    state.update(model=model, model_version=str(champion.version), expected_columns=expected_columns)
    MODEL_LOADED.set(1)


app = FastAPI(title="Property Price API", version="1.0.0")


@app.on_event("startup")
def startup():
    load_champion()


@app.get("/health")
def health():
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return {"status": "ok", "model_loaded": True, "model_version": state["model_version"]}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/model-info")
def model_info():
    return {
        "model_name": MODEL_NAME,
        "alias": MODEL_ALIAS,
        "version": state["model_version"],
        "expected_columns": state["expected_columns"],
    }


@app.post("/predict")
def predict(payload: PredictionRequest):
    expected, received = set(state["expected_columns"]), set(payload.features)
    missing, unknown = sorted(expected - received), sorted(received - expected)

    if missing or unknown:
        PREDICTION_ERRORS_TOTAL.labels(reason="validation").inc()
        raise HTTPException(status_code=422, detail={"missing_features": missing, "unknown_features": unknown})

    started = time.perf_counter()
    try:
        frame = pd.DataFrame([payload.features], columns=state["expected_columns"])
        predicted_price = float(state["model"].predict(frame)[0])
        latency_ms = (time.perf_counter() - started) * 1000
        request_id = str(uuid.uuid4())

        with engine.begin() as connection:
            connection.execute(text("""
                INSERT INTO mlops.inference_logs (
                    request_id, input_payload, prediction, score,
                    model_name, model_version, latency_ms
                ) VALUES (
                    CAST(:request_id AS uuid), CAST(:input_payload AS jsonb),
                    :prediction, :score, :model_name, :model_version, :latency_ms
                )
            """), {
                "request_id": request_id,
                "input_payload": json.dumps(payload.features, default=str),
                "prediction": "property_price",
                "score": predicted_price,
                "model_name": MODEL_NAME,
                "model_version": state["model_version"],
                "latency_ms": latency_ms,
            })

        PREDICTIONS_TOTAL.labels(status="success").inc()
        PREDICTION_LATENCY.observe(latency_ms / 1000)
        return {
            "request_id": request_id,
            "predicted_price": round(predicted_price, 2),
            "model_name": MODEL_NAME,
            "model_version": state["model_version"],
            "latency_ms": round(latency_ms, 2),
        }
    except Exception as exc:
        PREDICTION_ERRORS_TOTAL.labels(reason="internal").inc()
        logger.exception("Fallo durante inferencia")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/metrics", include_in_schema=False)
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
