import hashlib
import json
import os
from pathlib import Path

import gdown
import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from mlflow import MlflowClient
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score, f1_score, precision_score, recall_score, roc_auc_score
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sqlalchemy import create_engine, text

DATASET_ID = "1k5-1caezQ3zWJbKaiMULTGq-3sz6uThC"
MODEL_NAME = "diabetes_readmission"
BATCH_SIZE = 15000
RANDOM_STATE = 42

NUMERIC_COLUMNS = [
    "admission_type_id", "discharge_disposition_id", "admission_source_id",
    "time_in_hospital", "num_lab_procedures", "num_procedures",
    "num_medications", "number_outpatient", "number_emergency",
    "number_inpatient", "number_diagnoses",
]

def database_url():
    return (
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:"
        f"{os.environ['POSTGRES_PASSWORD']}@postgres:5432/mlops"
    )

def record_hash(record):
    value = json.dumps(record, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(value.encode()).hexdigest()

def as_jsonable(record):
    return {
        key: None if pd.isna(value) else value.item() if hasattr(value, "item") else value
        for key, value in record.items()
    }

def download_dataset():
    path = Path("/tmp/Diabetes.csv")
    if not path.exists():
        gdown.download(id=DATASET_ID, output=str(path), quiet=False)
    return path

def store_raw_and_clean(df, engine):
    df = df.replace("?", np.nan)
    df["row_hash"] = [record_hash(as_jsonable(row)) for row in df.to_dict("records")]
    df["batch_id"] = [f"bootstrap-{index // BATCH_SIZE + 1:03d}" for index in range(len(df))]

    with engine.begin() as conn:
        for batch_id, chunk in df.groupby("batch_id", sort=True):
            records = []
            for row in chunk.to_dict("records"):
                payload = as_jsonable({
                    key: value for key, value in row.items()
                    if key not in {"row_hash", "batch_id"}
                })
                records.append({
                    "batch_id": batch_id,
                    "source_file": "Diabetes.csv",
                    "row_hash": row["row_hash"],
                    "payload": json.dumps(payload),
                })

            conn.execute(text("""
                INSERT INTO raw.patient_records
                    (batch_id, source_file, row_hash, payload)
                VALUES
                    (:batch_id, :source_file, :row_hash, CAST(:payload AS jsonb))
                ON CONFLICT (row_hash) DO NOTHING
            """), records)

        conn.execute(text("DELETE FROM clean.patient_features WHERE batch_id LIKE 'bootstrap-%'"))

        raw_ids = dict(conn.execute(text("""
            SELECT row_hash, id
            FROM raw.patient_records
            WHERE source_file = 'Diabetes.csv'
        """)).all())

        target = (df["readmitted"] == "<30").astype(int)
        excluded = {
            "encounter_id", "patient_nbr", "readmitted", "row_hash", "batch_id",
            "weight", "payer_code", "medical_specialty",
        }
        feature_columns = [column for column in df.columns if column not in excluded]

        clean_records = []
        for row, label in zip(df.to_dict("records"), target):
            features = as_jsonable({
                column: row[column] for column in feature_columns
            })
            clean_records.append({
                "raw_record_id": raw_ids[row["row_hash"]],
                "batch_id": row["batch_id"],
                "features": json.dumps(features),
                "target": str(int(label)),
            })

        for start in range(0, len(clean_records), 5000):
            conn.execute(text("""
                INSERT INTO clean.patient_features
                    (raw_record_id, batch_id, features, target)
                VALUES
                    (:raw_record_id, :batch_id, CAST(:features AS jsonb), :target)
            """), clean_records[start:start + 5000])

    return feature_columns, target

def build_pipeline(feature_columns, frame):
    numeric = [column for column in NUMERIC_COLUMNS if column in feature_columns]
    categorical = [column for column in feature_columns if column not in numeric]

    for column in numeric:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    for column in categorical:
        frame[column] = frame[column].fillna("Unknown").astype(str)

    preprocessor = ColumnTransformer([
        ("numeric", Pipeline([("imputer", SimpleImputer(strategy="median"))]), numeric),
        ("categorical", Pipeline([
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("encoder", OneHotEncoder(handle_unknown="ignore")),
        ]), categorical),
    ])

    return Pipeline([
        ("preprocessor", preprocessor),
        ("classifier", LogisticRegression(
            solver="saga",
            class_weight="balanced",
            max_iter=250,
            n_jobs=2,
            random_state=RANDOM_STATE,
        )),
    ])

def main():
    csv_path = download_dataset()
    raw_df = pd.read_csv(csv_path, dtype=str)
    engine = create_engine(database_url(), pool_pre_ping=True)

    feature_columns, target = store_raw_and_clean(raw_df.copy(), engine)
    features = raw_df[feature_columns].replace("?", np.nan).copy()
    pipeline = build_pipeline(feature_columns, features)

    x_train_full, x_test, y_train_full, y_test = train_test_split(
        features, target, test_size=0.20, stratify=target, random_state=RANDOM_STATE
    )
    x_train, x_val, y_train, y_val = train_test_split(
        x_train_full, y_train_full, test_size=0.25,
        stratify=y_train_full, random_state=RANDOM_STATE
    )

    pipeline.fit(x_train, y_train)

    validation_probabilities = pipeline.predict_proba(x_val)[:, 1]
    thresholds = np.arange(0.10, 0.91, 0.05)
    threshold = max(
        thresholds,
        key=lambda value: f1_score(y_val, validation_probabilities >= value)
    )

    test_probabilities = pipeline.predict_proba(x_test)[:, 1]
    test_predictions = (test_probabilities >= threshold).astype(int)

    metrics = {
        "test_f1_positive": float(f1_score(y_test, test_predictions)),
        "validation_f1_positive": float(f1_score(y_val, validation_probabilities >= threshold)),
        "test_precision_positive": float(precision_score(y_test, test_predictions)),
        "test_recall_positive": float(recall_score(y_test, test_predictions)),
        "test_roc_auc": float(roc_auc_score(y_test, test_probabilities)),
        "test_pr_auc": float(average_precision_score(y_test, test_probabilities)),
        "validation_threshold": float(threshold),
    }

    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
    mlflow.set_experiment("diabetes_readmission")
    client = MlflowClient()

    try:
        champion = client.get_model_version_by_alias(MODEL_NAME, "champion")
        previous_f1 = client.get_run(champion.run_id).data.metrics.get("validation_f1_positive", -1.0)
    except Exception:
        previous_f1 = -1.0

    with mlflow.start_run() as run:
        mlflow.log_params({
            "algorithm": "logistic_regression",
            "positive_class": "readmitted_less_than_30_days",
            "batch_size": BATCH_SIZE,
            "training_rows": len(x_train),
            "validation_rows": len(x_val),
            "test_rows": len(x_test),
        })
        mlflow.log_metrics(metrics)
        mlflow.log_dict({
            "dropped_columns": ["encounter_id", "patient_nbr", "weight", "payer_code", "medical_specialty"],
            "feature_columns": feature_columns,
            "selection_metric": "validation_f1_positive",
        }, "preprocessing.json")
        mlflow.sklearn.log_model(pipeline, "model")

        model_version = mlflow.register_model(
            f"runs:/{run.info.run_id}/model", MODEL_NAME
        )

        promoted = metrics["validation_f1_positive"] > previous_f1
        if promoted:
            client.set_registered_model_alias(MODEL_NAME, "champion", model_version.version)

        print(json.dumps({
            "run_id": run.info.run_id,
            "model_version": model_version.version,
            "promoted_to_champion": promoted,
            "previous_f1": previous_f1,
            **metrics,
        }, indent=2))

if __name__ == "__main__":
    main()
