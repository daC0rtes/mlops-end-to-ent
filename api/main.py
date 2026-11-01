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
logger = logging.getLogger("diabetes-api")

MODEL_NAME = os.getenv("MODEL_NAME", "diabetes_readmission")
MODEL_ALIAS = os.getenv("MODEL_ALIAS", "champion")
MODEL_URI = f"models:/{MODEL_NAME}@{MODEL_ALIAS}"

PREDICTIONS_TOTAL = Counter(
    "diabetes_predictions_total",
    "Total de predicciones realizadas",
    ["prediction"],
)
PREDICTION_LATENCY = Histogram(
    "diabetes_prediction_latency_seconds",
    "Latencia de predicción y auditoría",
)
PREDICTION_ERRORS_TOTAL = Counter(
    "diabetes_prediction_errors_total",
    "Total de errores de predicción",
    ["reason"],
)
MODEL_LOADED = Gauge(
    "diabetes_model_loaded",
    "Indica si el modelo champion está cargado",
)

state: dict[str, Any] = {
    "model": None,
    "model_version": None,
    "expected_columns": [],
}


class PredictionRequest(BaseModel):
    features: dict[str, Any]


def database_url() -> str:
    return (
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:"
        f"{os.environ['POSTGRES_PASSWORD']}@postgres:5432/mlops"
    )


engine = create_engine(database_url(), pool_pre_ping=True)


def load_champion() -> None:
    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
    client = MlflowClient()
    champion = client.get_model_version_by_alias(MODEL_NAME, MODEL_ALIAS)

    model = mlflow.sklearn.load_model(MODEL_URI)
    expected_columns = list(getattr(model, "feature_names_in_", []))

    if not expected_columns:
        raise RuntimeError("No fue posible obtener las columnas esperadas del modelo.")

    state["model"] = model
    state["model_version"] = str(champion.version)
    state["expected_columns"] = expected_columns
    MODEL_LOADED.set(1)

    logger.info(
        "Modelo %s@%s cargado: versión %s, %s variables.",
        MODEL_NAME,
        MODEL_ALIAS,
        champion.version,
        len(expected_columns),
    )


app = FastAPI(
    title="Diabetes Readmission API",
    version="0.1.0",
    description="API de inferencia para el modelo champion de readmisión temprana.",
)


@app.on_event("startup")
def startup() -> None:
    load_champion()


@app.get("/health")
def health() -> dict[str, Any]:
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return {
            "status": "ok",
            "model_loaded": state["model"] is not None,
            "model_version": state["model_version"],
        }
    except Exception as exc:
        logger.exception("Fallo de salud")
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/model-info")
def model_info() -> dict[str, Any]:
    return {
        "model_name": MODEL_NAME,
        "alias": MODEL_ALIAS,
        "version": state["model_version"],
        "expected_columns": state["expected_columns"],
    }


@app.post("/predict")
def predict(payload: PredictionRequest) -> dict[str, Any]:
    if state["model"] is None:
        raise HTTPException(status_code=503, detail="El modelo aún no está disponible.")

    expected = set(state["expected_columns"])
    received = set(payload.features)

    missing = sorted(expected - received)
    unknown = sorted(received - expected)
    if missing or unknown:
        PREDICTION_ERRORS_TOTAL.labels(reason="validation").inc()
        raise HTTPException(
            status_code=422,
            detail={
                "missing_features": missing,
                "unknown_features": unknown,
                "expected_feature_count": len(expected),
            },
        )

    started = time.perf_counter()

    try:
        frame = pd.DataFrame([payload.features], columns=state["expected_columns"])
        probability = float(state["model"].predict_proba(frame)[0, 1])
        prediction_value = int(state["model"].predict(frame)[0])
        prediction = (
            "early_readmission_under_30_days"
            if prediction_value == 1
            else "no_early_readmission_under_30_days"
        )

        latency_ms = (time.perf_counter() - started) * 1000
        request_id = str(uuid.uuid4())

        with engine.begin() as connection:
            connection.execute(
                text("""
                    INSERT INTO mlops.inference_logs (
                        request_id, input_payload, prediction, score,
                        model_name, model_version, latency_ms
                    ) VALUES (
                        CAST(:request_id AS uuid),
                        CAST(:input_payload AS jsonb),
                        :prediction,
                        :score,
                        :model_name,
                        :model_version,
                        :latency_ms
                    )
                """),
                {
                    "request_id": request_id,
                    "input_payload": json.dumps(payload.features, default=str),
                    "prediction": prediction,
                    "score": probability,
                    "model_name": MODEL_NAME,
                    "model_version": state["model_version"],
                    "latency_ms": latency_ms,
                },
            )

        PREDICTIONS_TOTAL.labels(prediction=prediction).inc()
        PREDICTION_LATENCY.observe(latency_ms / 1000)

        return {
            "request_id": request_id,
            "prediction": prediction,
            "early_readmission_probability": round(probability, 6),
            "model_name": MODEL_NAME,
            "model_version": state["model_version"],
            "latency_ms": round(latency_ms, 2),
        }

    except HTTPException:
        raise
    except Exception as exc:
        PREDICTION_ERRORS_TOTAL.labels(reason="internal").inc()
        logger.exception("Fallo durante la inferencia")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/metrics", include_in_schema=False)
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)