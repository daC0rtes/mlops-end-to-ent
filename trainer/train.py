import json
import os

import mlflow
import mlflow.sklearn
import pandas as pd
from mlflow import MlflowClient
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, r2_score, root_mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder
from sqlalchemy import create_engine, text

MODEL_NAME = "property_price_model"
EXPERIMENT_NAME = "property_price_regression"
RANDOM_STATE = 42
MAX_ROWS = 75000

NUMERIC_COLUMNS = [
    "brokered_by", "bed", "bath", "acre_lot", "street",
    "zip_code", "house_size", "prev_sold_year",
]
CATEGORICAL_COLUMNS = ["status", "city", "state"]
FEATURE_COLUMNS = NUMERIC_COLUMNS + CATEGORICAL_COLUMNS


def database_url():
    return (
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:"
        f"{os.environ['POSTGRES_PASSWORD']}@postgres:5432/mlops"
    )


def load_gold_data(engine):
    query = text("""
        SELECT
            brokered_by, status, bed, bath, acre_lot, street,
            city, state, zip_code, house_size, prev_sold_year, target_price
        FROM gold.property_training_features
        WHERE target_price IS NOT NULL
          AND target_price > 0
        LIMIT :max_rows
    """)
    frame = pd.read_sql(query, engine, params={"max_rows": MAX_ROWS})

    if len(frame) < 1000:
        raise ValueError(
            f"Gold solo tiene {len(frame)} filas válidas; se requieren al menos 1000."
        )

    return frame


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


def champion_metrics(client):
    try:
        champion = client.get_model_version_by_alias(MODEL_NAME, "champion")
        metrics = client.get_run(champion.run_id).data.metrics
        return champion.version, metrics.get("test_mae", float("inf"))
    except Exception:
        return None, float("inf")


def main():
    engine = create_engine(database_url(), pool_pre_ping=True)
    frame = load_gold_data(engine)

    features = frame[FEATURE_COLUMNS].copy()
    target = frame["target_price"].astype(float)

    x_train, x_test, y_train, y_test = train_test_split(
        features,
        target,
        test_size=0.20,
        random_state=RANDOM_STATE,
    )

    pipeline = build_pipeline()
    pipeline.fit(x_train, y_train)

    predictions = pipeline.predict(x_test)
    metrics = {
        "test_mae": float(mean_absolute_error(y_test, predictions)),
        "test_rmse": float(root_mean_squared_error(y_test, predictions)),
        "test_r2": float(r2_score(y_test, predictions)),
    }

    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
    mlflow.set_experiment(EXPERIMENT_NAME)
    client = MlflowClient()
    champion_version, champion_mae = champion_metrics(client)

    with mlflow.start_run() as run:
        mlflow.log_params({
            "algorithm": "hist_gradient_boosting_regressor",
            "training_rows": len(x_train),
            "test_rows": len(x_test),
            "max_rows": MAX_ROWS,
            "git_commit": os.getenv("GIT_COMMIT", "unknown"),
        })
        mlflow.log_metrics(metrics)
        mlflow.log_dict({
            "source_layer": "gold.property_training_features",
            "feature_columns": FEATURE_COLUMNS,
            "target_column": "target_price",
            "selection_metric": "test_mae",
        }, "data_contract.json")
        mlflow.sklearn.log_model(pipeline, "model")

        model_version = mlflow.register_model(
            f"runs:/{run.info.run_id}/model",
            MODEL_NAME,
        )

        promoted = champion_version is None or metrics["test_mae"] < champion_mae
        if promoted:
            client.set_registered_model_alias(
                MODEL_NAME,
                "champion",
                model_version.version,
            )

        print(json.dumps({
            "run_id": run.info.run_id,
            "model_version": model_version.version,
            "promoted_to_champion": promoted,
            "previous_champion_version": champion_version,
            "previous_champion_mae": champion_mae,
            **metrics,
        }, indent=2))


if __name__ == "__main__":
    main()
