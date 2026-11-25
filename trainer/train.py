import json
import os
import re

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from mlflow import MlflowClient
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, r2_score, root_mean_squared_error
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder
from sqlalchemy import URL, create_engine, text

MODEL_NAME = "property_price_model"
EXPERIMENT_NAME = "property_price_regression"
RANDOM_STATE = 42
ROWS_PER_SPLIT = {"train": 20000, "validation": 4000, "test": 4000}
MIN_MAE_GAIN = 0.03
MAX_RMSE_INCREASE = 0.01

NUMERIC_COLUMNS = [
    "brokered_by", "bed", "bath", "acre_lot", "street",
    "zip_code", "house_size", "prev_sold_year",
]
CATEGORICAL_COLUMNS = ["status", "city", "state"]
FEATURE_COLUMNS = NUMERIC_COLUMNS + CATEGORICAL_COLUMNS


def database_url():
    return URL.create(
        "postgresql+psycopg2",
        username=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host="postgres",
        port=5432,
        database="mlops",
    )


def load_splits(engine, batch_id):
    # clean_id is stable within this database. A separate hash orders the
    # capped sample so the split decision is independent of sample order.
    query = text("""
        SELECT
            clean_id, brokered_by, status, bed, bath, acre_lot, street,
            city, state, zip_code, house_size, prev_sold_year, target_price
        FROM gold.property_training_features
        WHERE batch_id = :batch_id
          AND target_price > 0
          AND target_price < 'Infinity'::double precision
          AND (get_byte(decode(md5(clean_id::text || ':split-v1'), 'hex'), 0) % 10)
              BETWEEN :first_bucket AND :last_bucket
        ORDER BY md5(clean_id::text || ':sample-v1'), clean_id
        LIMIT :row_limit
    """)
    splits = {}
    with engine.connect() as conn:
        conn.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
        conn.execute(text("SET LOCAL work_mem = '8MB'"))
        conn.execute(text("SET LOCAL max_parallel_workers_per_gather = 0"))
        conn.execute(text("SET LOCAL statement_timeout = '90s'"))
        for name, (first, last, minimum) in {
            "train": (4, 9, 1000),
            "validation": (2, 3, 200),
            "test": (0, 1, 200),
        }.items():
            frame = pd.read_sql(query, conn, params={
                "batch_id": batch_id,
                "first_bucket": first,
                "last_bucket": last,
                "row_limit": ROWS_PER_SPLIT[name],
            })
            if len(frame) < minimum:
                raise ValueError(f"{batch_id}: {name} tiene {len(frame)} filas; mínimo {minimum}.")
            splits[name] = frame
    return splits


def build_pipeline():
    numeric_pipeline = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
    ])
    categorical_pipeline = Pipeline([
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("encoder", OrdinalEncoder(
            handle_unknown="use_encoded_value",
            unknown_value=-1,
        )),
    ])

    preprocessor = ColumnTransformer([
        ("numeric", numeric_pipeline, NUMERIC_COLUMNS),
        ("categorical", categorical_pipeline, CATEGORICAL_COLUMNS),
    ])

    return Pipeline([
        ("preprocessor", preprocessor),
        ("regressor", HistGradientBoostingRegressor(
            max_iter=100,
            max_leaf_nodes=31,
            l2_regularization=1.0,
            random_state=RANDOM_STATE,
        )),
    ])


def features(frame):
    data = frame[FEATURE_COLUMNS].copy()
    data[CATEGORICAL_COLUMNS] = data[CATEGORICAL_COLUMNS].fillna(np.nan)
    return data


def evaluate(model, frame):
    truth = frame["target_price"]
    predictions = model.predict(features(frame))
    return {
        "mae": float(mean_absolute_error(truth, predictions)),
        "rmse": float(root_mean_squared_error(truth, predictions)),
        "r2": float(r2_score(truth, predictions)),
    }


def compare(candidate, champion):
    mae_passed = candidate["mae"] <= champion["mae"] * (1 - MIN_MAE_GAIN)
    rmse_passed = candidate["rmse"] <= champion["rmse"] * (1 + MAX_RMSE_INCREASE)
    if not mae_passed:
        return "rejected", "mae_improvement_below_threshold"
    if not rmse_passed:
        return "rejected", "rmse_increase_above_threshold"
    return "eligible_for_promotion", "common_test_thresholds_met"


def main():
    batch_id = os.environ["CANDIDATE_BATCH_ID"]
    commit = os.environ["GIT_COMMIT"]
    training_reason = os.environ["TRAINING_REASON"]
    if not re.fullmatch(r"[a-f0-9]{40}", commit):
        raise ValueError("GIT_COMMIT debe ser el SHA completo de la imagen.")

    engine = create_engine(database_url(), pool_pre_ping=True)
    try:
        splits = load_splits(engine, batch_id)
    finally:
        engine.dispose()
    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
    mlflow.set_experiment(EXPERIMENT_NAME)
    client = MlflowClient()
    champion = client.get_model_version_by_alias(MODEL_NAME, "champion")
    champion_uri = f"models:/{MODEL_NAME}/{champion.version}"

    with mlflow.start_run(run_name=f"candidate-preview-{batch_id}") as run:
        mlflow.log_params({
            "algorithm": "hist_gradient_boosting_regressor",
            "batch_id": batch_id,
            "git_commit": commit,
            "training_reason": training_reason,
            "champion_version": champion.version,
            "champion_run_id": champion.run_id,
            "champion_holdout_exposure": "historical_training_rows_not_verified",
            "split_strategy": "clean_id_hash_v1",
            "min_mae_gain": MIN_MAE_GAIN,
            "max_rmse_increase": MAX_RMSE_INCREASE,
            "promotion_mode": "held_for_review",
            "regressor_max_iter": 100,
            "regressor_max_leaf_nodes": 31,
            "regressor_l2_regularization": 1.0,
            "numeric_imputer": "median",
            "categorical_imputer": "most_frequent",
            "categorical_encoder": "ordinal_unknown_minus_one",
            **{f"{name}_rows": len(frame) for name, frame in splits.items()},
        })
        pipeline = build_pipeline()
        pipeline.fit(features(splits["train"]), splits["train"]["target_price"])
        candidate_scores = {name: evaluate(pipeline, frame) for name, frame in splits.items()}
        champion_model = mlflow.pyfunc.load_model(champion_uri)
        champion_score = evaluate(champion_model, splits["test"])
        candidate_score = candidate_scores["test"]
        decision, reason = compare(candidate_score, champion_score)
        changes = {
            f"{name}_delta_fraction": (candidate_score[name] - champion_score[name]) / champion_score[name]
            for name in ("mae", "rmse") if champion_score[name] > 0
        }
        mlflow.log_metrics({
            **{f"candidate_{split}_{key}": value
               for split, scores in candidate_scores.items() for key, value in scores.items()},
            **{f"champion_test_{key}": value for key, value in champion_score.items()},
            **changes,
        })
        mlflow.log_dict({
            "source_layer": "gold.property_training_features",
            "feature_columns": FEATURE_COLUMNS,
            "target_column": "target_price",
            "selection_metric": "mae",
            "test_clean_ids": [int(value) for value in splits["test"]["clean_id"]],
        }, "data_contract.json")
        mlflow.sklearn.log_model(
            pipeline, "model", input_example=features(splits["train"]).head(3),
        )
        model_version = mlflow.register_model(
            f"runs:/{run.info.run_id}/model",
            MODEL_NAME,
        )
        mlflow.set_tags({"comparison_decision": decision, "decision_reason": reason})
        client.set_model_version_tag(MODEL_NAME, model_version.version, "comparison_decision", decision)
        report = {
            "run_id": run.info.run_id,
            "batch_id": batch_id,
            "candidate_version": model_version.version,
            "champion_version": champion.version,
            "candidate_test": candidate_score,
            "champion_test": champion_score,
            "decision": decision,
            "reason": reason,
            "promoted": False,
            **changes,
        }
        mlflow.log_dict(report, "comparison.json")
        print(json.dumps(report, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
