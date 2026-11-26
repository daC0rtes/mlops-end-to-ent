import json
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from threading import Event, Lock, Thread
from typing import Any

import mlflow
import mlflow.sklearn
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import Response
from mlflow import MlflowClient
from prometheus_client import Counter, Gauge, Histogram, Info, generate_latest, CONTENT_TYPE_LATEST
from pydantic import BaseModel
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("property-api")

MODEL_NAME = os.getenv("MODEL_NAME", "property_price_model")
MODEL_ALIAS = os.getenv("MODEL_ALIAS", "champion")
MODEL_POLL_SECONDS = max(15, int(os.getenv("MODEL_POLL_SECONDS", "60")))

PREDICTIONS_TOTAL = Counter("property_predictions_total", "Predicciones de precio", ["status"])
PREDICTION_LATENCY = Histogram("property_prediction_latency_seconds", "Latencia de predicción")
PREDICTION_ERRORS_TOTAL = Counter("property_prediction_errors_total", "Errores", ["reason"])
MODEL_LOADED = Gauge("property_model_loaded", "Modelo champion cargado")
MODEL_INFO = Info("property_served_model", "Versión del modelo activo en este proceso")
MODEL_REFRESH_ERRORS = Counter(
    "property_model_refresh_errors_total", "Fallos al comprobar o actualizar el modelo",
)


@dataclass(frozen=True)
class LoadedModel:
    model: Any
    version: str
    columns: tuple[str, ...]


_model_lock = Lock()
_active_model: LoadedModel | None = None


class PredictionRequest(BaseModel):
    features: dict[str, Any]


def database_url():
    return (
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:"
        f"{os.environ['POSTGRES_PASSWORD']}@postgres:5432/mlops"
    )


engine = create_engine(database_url(), pool_pre_ping=True)


def active_model():
    with _model_lock:
        return _active_model


def refresh_champion(client):
    global _active_model
    current = active_model()
    version = str(client.get_model_version_by_alias(MODEL_NAME, MODEL_ALIAS).version)
    if current is not None and version == current.version:
        return

    # Load the resolved version so an alias change during the download cannot
    # make the model bytes disagree with the reported version.
    model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/{version}")
    columns = tuple(getattr(model, "feature_names_in_", ()))
    if not columns or not callable(getattr(model, "predict", None)):
        raise RuntimeError("La versión no declara un contrato de predicción válido.")
    if current is not None and columns != current.columns:
        raise RuntimeError("La versión nueva cambia las columnas del contrato activo.")
    if str(client.get_model_version_by_alias(MODEL_NAME, MODEL_ALIAS).version) != version:
        logger.info("El alias cambió durante la descarga; se revisará de nuevo.")
        return

    loaded = LoadedModel(model=model, version=version, columns=columns)
    with _model_lock:
        _active_model = loaded
        MODEL_INFO.info({"model": MODEL_NAME, "alias": MODEL_ALIAS, "version": version})
        MODEL_LOADED.set(1)
    logger.info("Modelo activo %s@%s, versión %s", MODEL_NAME, MODEL_ALIAS, version)


def poll_champion(client, stop):
    while not stop.wait(MODEL_POLL_SECONDS):
        try:
            refresh_champion(client)
        except Exception:
            MODEL_REFRESH_ERRORS.inc()
            logger.exception("Falló la recarga; se mantiene la versión activa.")


@asynccontextmanager
async def lifespan(_app):
    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
    client = MlflowClient()
    refresh_champion(client)
    if active_model() is None:
        raise RuntimeError("No se pudo activar el champion al iniciar la API.")
    stop = Event()
    worker = Thread(target=poll_champion, args=(client, stop), name="champion-poll", daemon=True)
    worker.start()
    try:
        yield
    finally:
        stop.set()
        worker.join(timeout=2)


app = FastAPI(title="Property Price API", version="1.0.0", lifespan=lifespan)


@app.get("/health")
def health():
    loaded = active_model()
    if loaded is None:
        raise HTTPException(status_code=503, detail="Modelo no disponible.")
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return {"status": "ok", "model_loaded": True, "model_version": loaded.version}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/model-info")
def model_info():
    loaded = active_model()
    if loaded is None:
        raise HTTPException(status_code=503, detail="Modelo no disponible.")
    return {
        "model_name": MODEL_NAME,
        "alias": MODEL_ALIAS,
        "version": loaded.version,
        "expected_columns": loaded.columns,
    }


@app.get("/training-history")
def training_history(limit: int = Query(default=20, ge=1, le=50)):
    """Return one latest recorded training decision per ingested batch."""
    try:
        with engine.begin() as connection:
            connection.execute(text("SET TRANSACTION READ ONLY"))
            connection.execute(text("SET LOCAL statement_timeout = '3s'"))
            if not connection.scalar(text("SELECT to_regclass('mlops.property_training_runs')")):
                return {"items": []}
            result = connection.execute(text("""
                WITH recent_batches AS (
                    SELECT batch_id, record_count, retrieved_at
                    FROM raw.property_batches
                    ORDER BY retrieved_at DESC, batch_id DESC
                    LIMIT :limit
                )
                SELECT b.batch_id, b.record_count AS raw_rows, a.gold_records AS gold_rows,
                       COALESCE(r.status, 'no_training_record') AS status,
                       r.mode, r.assessment->>'reason' AS training_reason,
                       r.comparison->>'reason' AS comparison_reason,
                       r.comparison->'mae_delta_fraction' AS mae_delta_fraction,
                       r.comparison->'rmse_delta_fraction' AS rmse_delta_fraction,
                       r.comparison->>'run_id' AS mlflow_run_id,
                       r.comparison->>'candidate_version' AS candidate_version,
                       r.comparison->>'champion_version' AS champion_version,
                       r.comparison->'promoted' AS promoted,
                       r.job_name, r.updated_at
                FROM recent_batches b
                LEFT JOIN mlops.batch_audit a ON a.batch_id = b.batch_id
                LEFT JOIN LATERAL (
                    SELECT status, mode, assessment, comparison, job_name, updated_at
                    FROM mlops.property_training_runs
                    WHERE batch_id = b.batch_id
                    ORDER BY updated_at DESC, dag_id, run_id
                    LIMIT 1
                ) r ON true
                ORDER BY b.retrieved_at DESC, b.batch_id DESC
            """), {"limit": limit})
            return {"items": [dict(row) for row in result.mappings()]}
    except Exception as exc:
        logger.exception("No se pudo cargar el historial de entrenamiento")
        raise HTTPException(status_code=503, detail="Historial temporalmente no disponible.") from exc


@app.post("/predict")
def predict(payload: PredictionRequest):
    loaded = active_model()
    if loaded is None:
        raise HTTPException(status_code=503, detail="Modelo no disponible.")
    expected, received = set(loaded.columns), set(payload.features)
    missing, unknown = sorted(expected - received), sorted(received - expected)

    if missing or unknown:
        PREDICTION_ERRORS_TOTAL.labels(reason="validation").inc()
        raise HTTPException(status_code=422, detail={"missing_features": missing, "unknown_features": unknown})

    started = time.perf_counter()
    try:
        frame = pd.DataFrame([payload.features], columns=loaded.columns)
        predicted_price = float(loaded.model.predict(frame)[0])
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
                "model_version": loaded.version,
                "latency_ms": latency_ms,
            })

        PREDICTIONS_TOTAL.labels(status="success").inc()
        PREDICTION_LATENCY.observe(latency_ms / 1000)
        return {
            "request_id": request_id,
            "predicted_price": round(predicted_price, 2),
            "model_name": MODEL_NAME,
            "model_version": loaded.version,
            "latency_ms": round(latency_ms, 2),
        }
    except Exception as exc:
        PREDICTION_ERRORS_TOTAL.labels(reason="internal").inc()
        logger.exception("Fallo durante inferencia")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/metrics", include_in_schema=False)
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
