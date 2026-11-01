import hashlib
import json
import os
from datetime import datetime
from pathlib import Path

import gdown
import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from mlflow import MlflowClient
from psycopg2.extras import execute_values
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

DATASET_ID = "1k5-1caezQ3zWJbKaiMULTGq-3sz6uThC"
BATCH_SIZE = 15000
MODEL_NAME = "diabetes_readmission"
RANDOM_STATE = 42

NUMERIC_COLUMNS = [
    "admission_type_id",
    "discharge_disposition_id",
    "admission_source_id",
    "time_in_hospital",
    "num_lab_procedures",
    "num_procedures",
    "num_medications",
    "number_outpatient",
    "number_emergency",
    "number_inpatient",
    "number_diagnoses",
]

DROPPED_COLUMNS = [
    "encounter_id",
    "patient_nbr",
    "readmitted",
    "weight",
    "payer_code",
    "medical_specialty",
]


def connection():
    return psycopg2.connect(
        host="postgres",
        dbname="mlops",
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def canonical_hash(record, batch_id):
    content = json.dumps(record, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(f"{batch_id}:{content}".encode()).hexdigest()


def normalize_frame(frame, feature_columns):
    result = frame[feature_columns].copy()

    for column in NUMERIC_COLUMNS:
        if column in result.columns:
            result[column] = pd.to_numeric(result[column], errors="coerce")

    for column in result.columns:
        if column not in NUMERIC_COLUMNS:
            result[column] = result[column].fillna("Unknown").astype(str)

    return result


def build_pipeline(feature_columns):
    numeric = [item for item in NUMERIC_COLUMNS if item in feature_columns]
    categorical = [item for item in feature_columns if item not in numeric]

    preprocessor = ColumnTransformer(
        [
            (
                "numeric",
                Pipeline(
                    [("imputer", SimpleImputer(strategy="median"))]
                ),
                numeric,
            ),
            (
                "categorical",
                Pipeline(
                    [
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("encoder", OneHotEncoder(handle_unknown="ignore")),
                    ]
                ),
                categorical,
            ),
        ]
    )

    return Pipeline(
        [
            ("preprocessor", preprocessor),
            (
                "classifier",
                LogisticRegression(
                    solver="saga",
                    class_weight="balanced",
                    max_iter=250,
                    n_jobs=2,
                    random_state=RANDOM_STATE,
                ),
            ),
        ]
    )


with DAG(
    dag_id="diabetes_mlops_pipeline",
    description="Ingesta incremental, entrenamiento y promoción automática del modelo champion.",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["mlops", "diabetes", "kubernetes"],
) as dag:

    @task
    def validate_source():
        path = Path("/tmp/Diabetes.csv")

        if not path.exists():
            gdown.download(id=DATASET_ID, output=str(path), quiet=False)

        frame = pd.read_csv(path, dtype=str)
        required = {"readmitted", "race", "gender", "age", "time_in_hospital"}

        if len(frame) == 0 or not required.issubset(frame.columns):
            raise ValueError("El archivo fuente no cumple las columnas mínimas.")

        return str(path)

    @task
    def incremental_load(source_path):
        context = get_current_context()
        run_conf = context["dag_run"].conf or {}
        batch_index = int(run_conf.get("batch_index", 0))

        frame = pd.read_csv(source_path, dtype=str).replace("?", np.nan)
        start = batch_index * BATCH_SIZE
        end = min(start + BATCH_SIZE, len(frame))

        if start >= len(frame):
            raise ValueError("batch_index supera los registros disponibles.")

        batch_id = f"airflow-batch-{batch_index + 1:03d}"
        batch = frame.iloc[start:end].copy()

        rows = []
        for record in batch.to_dict("records"):
            payload = {
                key: None if pd.isna(value) else value
                for key, value in record.items()
            }
            rows.append(
                (
                    batch_id,
                    "Diabetes.csv-airflow",
                    canonical_hash(payload, batch_id),
                    json.dumps(payload),
                )
            )

        with connection() as conn, conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO raw.patient_records
                    (batch_id, source_file, row_hash, payload)
                VALUES %s
                ON CONFLICT (row_hash) DO NOTHING
                """,
                rows,
                template="(%s, %s, %s, %s::jsonb)",
                page_size=1000,
            )

        return batch_id

    @task
    def validate_raw_quality(batch_id):
        with connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                FROM raw.patient_records
                WHERE batch_id = %s
                """,
                (batch_id,),
            )
            total = cur.fetchone()[0]

            cur.execute(
                """
                SELECT count(*)
                FROM raw.patient_records
                WHERE batch_id = %s
                  AND payload ? 'readmitted'
                """,
                (batch_id,),
            )
            with_target = cur.fetchone()[0]

        if total == 0 or total != with_target:
            raise ValueError("La carga incremental no pasó la validación de calidad.")

        return batch_id

    @task
    def preprocess(batch_id):
        with connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS mlops.preprocessing_audit (
                    batch_id TEXT PRIMARY KEY,
                    dropped_columns JSONB NOT NULL,
                    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute(
                """
                INSERT INTO mlops.preprocessing_audit (batch_id, dropped_columns)
                VALUES (%s, %s::jsonb)
                ON CONFLICT (batch_id)
                DO UPDATE SET
                    dropped_columns = EXCLUDED.dropped_columns,
                    processed_at = NOW()
                """,
                (batch_id, json.dumps(DROPPED_COLUMNS)),
            )

        return batch_id

    @task
    def store_clean_data(batch_id):
        with connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, payload
                FROM raw.patient_records
                WHERE batch_id = %s
                ORDER BY id
                """,
                (batch_id,),
            )
            raw_rows = cur.fetchall()

            if not raw_rows:
                raise ValueError("No hay datos raw para transformar.")

            frame = pd.DataFrame([row[1] for row in raw_rows])
            target = (frame["readmitted"] == "<30").astype(int)

            feature_columns = [
                column for column in frame.columns
                if column not in DROPPED_COLUMNS
            ]

            cur.execute(
                "DELETE FROM clean.patient_features WHERE batch_id = %s",
                (batch_id,),
            )

            clean_rows = []
            for (raw_id, _), features, label in zip(
                raw_rows,
                frame[feature_columns].to_dict("records"),
                target,
            ):
                clean_rows.append(
                    (
                        raw_id,
                        batch_id,
                        json.dumps(
                            {
                                key: None if pd.isna(value) else value
                                for key, value in features.items()
                            }
                        ),
                        str(int(label)),
                    )
                )

            execute_values(
                cur,
                """
                INSERT INTO clean.patient_features
                    (raw_record_id, batch_id, features, target)
                VALUES %s
                """,
                clean_rows,
                template="(%s, %s, %s::jsonb, %s)",
                page_size=1000,
            )

        return batch_id

    @task
    def split_dataset(batch_id):
        with connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS mlops.training_splits (
                    batch_id TEXT NOT NULL,
                    raw_record_id BIGINT NOT NULL,
                    split TEXT NOT NULL,
                    PRIMARY KEY (batch_id, raw_record_id)
                )
                """
            )

            cur.execute(
                """
                SELECT raw_record_id, target
                FROM clean.patient_features
                WHERE batch_id = %s
                ORDER BY raw_record_id
                """,
                (batch_id,),
            )
            rows = cur.fetchall()

            ids = [row[0] for row in rows]
            labels = [int(row[1]) for row in rows]

            train_val_ids, test_ids, train_val_y, _ = train_test_split(
                ids,
                labels,
                test_size=0.20,
                stratify=labels,
                random_state=RANDOM_STATE,
            )
            train_ids, validation_ids = train_test_split(
                train_val_ids,
                test_size=0.25,
                stratify=train_val_y,
                random_state=RANDOM_STATE,
            )

            cur.execute(
                "DELETE FROM mlops.training_splits WHERE batch_id = %s",
                (batch_id,),
            )

            split_rows = (
                [(batch_id, item, "train") for item in train_ids]
                + [(batch_id, item, "validation") for item in validation_ids]
                + [(batch_id, item, "test") for item in test_ids]
            )

            execute_values(
                cur,
                """
                INSERT INTO mlops.training_splits
                    (batch_id, raw_record_id, split)
                VALUES %s
                """,
                split_rows,
                page_size=1000,
            )

        return batch_id

    @task
    def train_model(batch_id):
        with connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT clean.features, clean.target, splits.split
                FROM clean.patient_features AS clean
                JOIN mlops.training_splits AS splits
                  ON clean.raw_record_id = splits.raw_record_id
                 AND clean.batch_id = splits.batch_id
                WHERE clean.batch_id = %s
                """,
                (batch_id,),
            )
            rows = cur.fetchall()

        partitions = {"train": [], "validation": [], "test": []}
        labels = {"train": [], "validation": [], "test": []}

        for features, target, split_name in rows:
            partitions[split_name].append(features)
            labels[split_name].append(int(target))

        feature_columns = list(partitions["train"][0].keys())
        x_train = normalize_frame(pd.DataFrame(partitions["train"]), feature_columns)
        x_validation = normalize_frame(
            pd.DataFrame(partitions["validation"]),
            feature_columns,
        )
        x_test = normalize_frame(pd.DataFrame(partitions["test"]), feature_columns)

        pipeline = build_pipeline(feature_columns)
        pipeline.fit(x_train, labels["train"])

        validation_probabilities = pipeline.predict_proba(x_validation)[:, 1]
        threshold = max(
            np.arange(0.10, 0.91, 0.05),
            key=lambda value: f1_score(
                labels["validation"],
                validation_probabilities >= value,
            ),
        )

        test_probabilities = pipeline.predict_proba(x_test)[:, 1]
        test_predictions = (test_probabilities >= threshold).astype(int)

        metrics = {
            "validation_f1_positive": float(
                f1_score(labels["validation"], validation_probabilities >= threshold)
            ),
            "test_f1_positive": float(f1_score(labels["test"], test_predictions)),
            "test_precision_positive": float(
                precision_score(labels["test"], test_predictions)
            ),
            "test_recall_positive": float(
                recall_score(labels["test"], test_predictions)
            ),
            "test_roc_auc": float(roc_auc_score(labels["test"], test_probabilities)),
            "test_pr_auc": float(
                average_precision_score(labels["test"], test_probabilities)
            ),
            "validation_threshold": float(threshold),
        }

        mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
        mlflow.set_experiment("diabetes_readmission")
        with mlflow.start_run() as run:
            mlflow.log_params(
                {
                    "orchestrator": "airflow",
                    "batch_id": batch_id,
                    "algorithm": "logistic_regression",
                    "selection_metric": "validation_f1_positive",
                    "training_rows": len(x_train),
                    "validation_rows": len(x_validation),
                    "test_rows": len(x_test),
                }
            )
            mlflow.log_metrics(metrics)
            mlflow.log_dict(
                {
                    "feature_columns": feature_columns,
                    "dropped_columns": DROPPED_COLUMNS,
                },
                "preprocessing.json",
            )
            mlflow.sklearn.log_model(pipeline, "model")

            return {
                "run_id": run.info.run_id,
                "metrics": metrics,
            }

    @task
    def register_model(training_result):
        mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
        version = mlflow.register_model(
            f"runs:/{training_result['run_id']}/model",
            MODEL_NAME,
        )

        return {
            "run_id": training_result["run_id"],
            "model_version": str(version.version),
            "metrics": training_result["metrics"],
        }

    @task
    def compare_models(candidate):
        mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
        client = MlflowClient()
        previous_f1 = -1.0

        try:
            champion = client.get_model_version_by_alias(MODEL_NAME, "champion")
            previous_f1 = client.get_run(champion.run_id).data.metrics.get(
                "validation_f1_positive",
                -1.0,
            )
        except Exception:
            pass

        candidate["previous_f1"] = previous_f1
        candidate["promote"] = (
            candidate["metrics"]["validation_f1_positive"] > previous_f1
        )
        return candidate

    @task
    def promote_champion(comparison):
        mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
        if comparison["promote"]:
            client = MlflowClient()
            client.set_registered_model_alias(
                MODEL_NAME,
                "champion",
                comparison["model_version"],
            )

        return comparison

    source = validate_source()
    loaded_batch = incremental_load(source)
    validated_batch = validate_raw_quality(loaded_batch)
    preprocessing_done = preprocess(validated_batch)
    clean_batch = store_clean_data(validated_batch)
    preprocessing_done >> clean_batch
    split_batch = split_dataset(clean_batch)
    training = train_model(split_batch)
    registered = register_model(training)
    compared = compare_models(registered)
    promote_champion(compared)