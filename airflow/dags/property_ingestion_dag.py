import hashlib
import json
import os
from contextlib import closing
from datetime import datetime, timedelta
from urllib.parse import urlencode
from urllib.request import urlopen

from urllib.error import HTTPError
from airflow.exceptions import AirflowSkipException

import psycopg2
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from psycopg2.extras import execute_values

API_URL = os.getenv("DATA_API_URL", "http://data-api.mlops.svc.cluster.local/data")
GROUP_NUMBER = int(os.getenv("DATA_API_GROUP", "5"))
MIN_TRAINING_ROWS = 5000
HOUSE_SIZE_DRIFT_THRESHOLD = 0.20
NEW_CATEGORY_RATE_THRESHOLD = 0.10


def connection():
    return psycopg2.connect(
        host="postgres",
        dbname="mlops",
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def training_assessment(batch_id):
    """Compare one Gold batch against the last completed, earlier batch."""
    with closing(connection()) as conn, conn, conn.cursor() as cur:
        cur.execute("SET TRANSACTION READ ONLY")
        cur.execute("""
            SELECT b.record_count, a.validation_results, a.silver_records,
                   previous.batch_id
            FROM raw.property_batches b
            JOIN mlops.batch_audit a ON a.batch_id = b.batch_id
            LEFT JOIN LATERAL (
                SELECT old.batch_id
                FROM raw.property_batches old
                JOIN mlops.batch_audit history ON history.batch_id = old.batch_id
                WHERE old.group_number = b.group_number
                  AND old.source_batch_number < b.source_batch_number
                  AND history.gold_records >= %s
                ORDER BY old.source_batch_number DESC
                LIMIT 1
            ) previous ON TRUE
            WHERE b.batch_id = %s
        """, (MIN_TRAINING_ROWS, batch_id))
        audit = cur.fetchone()
        if audit is None:
            raise ValueError(f"No hay auditoría para el lote {batch_id}.")
        raw_rows, validation, silver_rows, reference_id = audit

        cur.execute("""
            SELECT batch_id, count(*), count(house_size), count(*)
                FILTER (WHERE house_size <= 0 OR house_size > 20000)
            FROM gold.property_training_features
            WHERE batch_id IN (%s, %s)
            GROUP BY batch_id
        """, (batch_id, reference_id))
        coverage = {row[0]: row[1:] for row in cur.fetchall()}
        row_count, sized_rows, unusual_sizes = coverage.get(batch_id, (0, 0, 0))
        reference_count = coverage.get(reference_id, (0, 0, 0))[0]
        if reference_id and reference_count < MIN_TRAINING_ROWS:
            raise ValueError(f"La referencia {reference_id} no contiene suficientes filas Gold.")

        house_size_drift = None
        category_changes = {}
        if reference_id and row_count >= MIN_TRAINING_ROWS and validation.get("quality_passed") is True:
            cur.execute("""
                SELECT batch_id,
                    width_bucket(house_size, ARRAY[1000, 1500, 2000, 2500, 3500, 5000]::double precision[]),
                    count(*)
                FROM gold.property_training_features
                WHERE batch_id IN (%s, %s) AND house_size > 0
                GROUP BY 1, 2
            """, (batch_id, reference_id))
            distributions = {batch_id: {}, reference_id: {}}
            for lot, bucket, count in cur.fetchall():
                distributions[lot][bucket] = count
            current, reference = distributions[batch_id], distributions[reference_id]
            current_total, reference_total = sum(current.values()), sum(reference.values())
            if current_total and reference_total:
                house_size_drift = 0.5 * sum(
                    abs(current.get(bucket, 0) / current_total
                        - reference.get(bucket, 0) / reference_total)
                    for bucket in current.keys() | reference.keys()
                )

            cur.execute("""
                WITH counts AS (
                    SELECT g.batch_id, v.feature, v.category, count(*) AS n
                    FROM gold.property_training_features g
                    CROSS JOIN LATERAL (VALUES
                        ('status', g.status), ('city', g.city), ('state', g.state)
                    ) v(feature, category)
                    WHERE g.batch_id IN (%s, %s)
                      AND NULLIF(btrim(v.category), '') IS NOT NULL
                    GROUP BY 1, 2, 3
                )
                SELECT c.feature,
                       count(*) FILTER (WHERE r.category IS NULL),
                       COALESCE(sum(c.n) FILTER (WHERE r.category IS NULL), 0),
                       sum(c.n)
                FROM counts c
                LEFT JOIN counts r ON r.batch_id = %s
                    AND r.feature = c.feature AND r.category = c.category
                WHERE c.batch_id = %s
                GROUP BY c.feature
            """, (batch_id, reference_id, reference_id, batch_id))
            category_changes = {
                feature: {"new_categories": count, "row_share": float(new_rows / total)}
                for feature, count, new_rows, total in cur.fetchall()
            }

    validation_passed = validation.get("quality_passed") is True
    if not validation_passed:
        decision, reason = "skip", "bronze_quality_not_confirmed"
    elif row_count < MIN_TRAINING_ROWS:
        decision, reason = "skip", "insufficient_gold_rows"
    elif not reference_id:
        decision, reason = "eligible", "first_sufficient_batch"
    elif house_size_drift is not None and house_size_drift >= HOUSE_SIZE_DRIFT_THRESHOLD:
        decision, reason = "eligible", "house_size_distribution_changed"
    elif any(item["row_share"] >= NEW_CATEGORY_RATE_THRESHOLD
             for item in category_changes.values()):
        decision, reason = "eligible", "frequent_new_categories"
    elif house_size_drift is None:
        decision, reason = "skip", "insufficient_house_size_coverage"
    else:
        decision, reason = "skip", "no_significant_change_detected"

    return {
        "batch_id": batch_id,
        "reference_batch_id": reference_id,
        "decision": decision,
        "reason": reason,
        "raw_rows": raw_rows,
        "gold_rows": row_count,
        "reference_gold_rows": reference_count,
        "gold_rows_vs_reference": row_count / reference_count if reference_count else None,
        "raw_to_silver_row_loss": raw_rows - silver_rows if silver_rows is not None else None,
        "missing_house_size": row_count - sized_rows,
        "house_size_outside_review_range": unusual_sizes,
        "house_size_total_variation": house_size_drift,
        "new_categories": category_changes,
        "thresholds": {
            "min_gold_rows": MIN_TRAINING_ROWS,
            "house_size_total_variation": HOUSE_SIZE_DRIFT_THRESHOLD,
            "new_category_row_share": NEW_CATEGORY_RATE_THRESHOLD,
        },
    }


with DAG(
    dag_id="property_batch_ingestion",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["mlops", "properties", "medallion"],
) as dag:

    @task
    def prepare_storage():
        with connection() as conn, conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS raw")
            cur.execute("CREATE SCHEMA IF NOT EXISTS clean")
            cur.execute("CREATE SCHEMA IF NOT EXISTS gold")
            cur.execute("CREATE SCHEMA IF NOT EXISTS mlops")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS raw.property_batches (
                    batch_id TEXT PRIMARY KEY,
                    group_number INTEGER NOT NULL,
                    source_batch_number INTEGER NOT NULL,
                    retrieved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    ingestion_mode TEXT NOT NULL,
                    source_url TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    payload_hash TEXT NOT NULL UNIQUE,
                    record_count INTEGER NOT NULL
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS clean.property_records (
                    id BIGSERIAL PRIMARY KEY,
                    batch_id TEXT NOT NULL REFERENCES raw.property_batches(batch_id),
                    source_record_index INTEGER NOT NULL,
                    row_hash TEXT NOT NULL UNIQUE,
                    features JSONB NOT NULL,
                    target_price DOUBLE PRECISION,
                    transformed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE(batch_id, source_record_index)
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS gold.property_training_features (
                    clean_id BIGINT PRIMARY KEY REFERENCES clean.property_records(id),
                    batch_id TEXT NOT NULL,
                    target_price DOUBLE PRECISION NOT NULL,
                    brokered_by DOUBLE PRECISION,
                    status TEXT,
                    bed DOUBLE PRECISION,
                    bath DOUBLE PRECISION,
                    acre_lot DOUBLE PRECISION,
                    street DOUBLE PRECISION,
                    city TEXT,
                    state TEXT,
                    zip_code DOUBLE PRECISION,
                    house_size DOUBLE PRECISION,
                    prev_sold_year INTEGER,
                    prepared_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS mlops.batch_audit (
                    batch_id TEXT PRIMARY KEY,
                    dag_run_id TEXT NOT NULL,
                    schema_received JSONB NOT NULL,
                    validation_results JSONB NOT NULL,
                    silver_records INTEGER,
                    gold_records INTEGER,
                    training_decision TEXT,
                    decision_reason TEXT,
                    result TEXT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute(
                "ALTER TABLE mlops.batch_audit ADD COLUMN IF NOT EXISTS silver_records INTEGER"
            )
            cur.execute(
                "ALTER TABLE mlops.batch_audit ADD COLUMN IF NOT EXISTS gold_records INTEGER"
            )
            cur.execute(
                "ALTER TABLE mlops.batch_audit ADD COLUMN IF NOT EXISTS training_decision TEXT"
            )
            cur.execute(
                "ALTER TABLE mlops.batch_audit ADD COLUMN IF NOT EXISTS decision_reason TEXT"
            )

    @task(retries=3, retry_delay=timedelta(seconds=15))
    def fetch_bronze_batch():
        context = get_current_context()
        source_url = f"{API_URL}?{urlencode({'group_number': GROUP_NUMBER})}"

        try:
            with urlopen(source_url, timeout=90) as response:
                body = response.read().decode("utf-8")
        except HTTPError as error:
            if error.code == 400:
                raise AirflowSkipException(
                    "La API no tiene más lotes disponibles para este grupo."
                ) from error
            raise

        payload = json.loads(body)
        if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
            raise ValueError("La API no devolvió el formato esperado.")

        records = payload["data"]
        if not records:
            raise ValueError("La API devolvió un lote vacío.")

        group_number = int(payload["group_number"])
        source_batch_number = int(payload["batch_number"])
        batch_id = f"group-{group_number}-batch-{source_batch_number}"
        payload_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()

        schema_received = {
            column: type(value).__name__
            for column, value in records[0].items()
        }
        validation_results = {
            "response_format_valid": True,
            "records_received": len(records),
            "payload_size_bytes": len(body.encode("utf-8")),
        }

        with connection() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO raw.property_batches
                    (batch_id, group_number, source_batch_number, ingestion_mode,
                     source_url, payload, payload_hash, record_count)
                VALUES (%s, %s, %s, 'airflow', %s, %s, %s, %s)
                ON CONFLICT (batch_id) DO NOTHING
            """, (
                batch_id, group_number, source_batch_number, source_url,
                body, payload_hash, len(records),
            ))

            cur.execute("""
                INSERT INTO mlops.batch_audit
                    (batch_id, dag_run_id, schema_received, validation_results, result)
                VALUES (%s, %s, %s::jsonb, %s::jsonb, 'stored_bronze')
                ON CONFLICT (batch_id) DO UPDATE SET
                    dag_run_id = EXCLUDED.dag_run_id,
                    schema_received = EXCLUDED.schema_received,
                    validation_results = EXCLUDED.validation_results,
                    result = EXCLUDED.result,
                    updated_at = NOW()
            """, (
                batch_id,
                context["dag_run"].run_id,
                json.dumps(schema_received),
                json.dumps(validation_results),
            ))

        return {"batch_id": batch_id}

    @task
    def validate_bronze(bronze_result):
        batch_id = bronze_result["batch_id"]
        required_columns = {
            "brokered_by", "status", "price", "bed", "bath", "acre_lot",
            "street", "city", "state", "zip_code", "house_size", "prev_sold_date",
        }
        numeric_columns = {
            "brokered_by", "price", "bed", "bath", "acre_lot",
            "street", "zip_code", "house_size",
        }

        with connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT payload FROM raw.property_batches WHERE batch_id = %s",
                (batch_id,),
            )
            records = json.loads(cur.fetchone()[0])["data"]

            missing_columns = sorted(required_columns - set(records[0]))
            valid_prices = sum(
                isinstance(record.get("price"), (int, float))
                and record["price"] > 0
                for record in records
            )
            invalid_numeric = sum(
                value is not None
                and (not isinstance(value, (int, float)) or value < 0)
                for record in records
                for key, value in record.items()
                if key in numeric_columns
            )
            price_valid_ratio = valid_prices / len(records)

            validation_results = {
                "required_columns_missing": missing_columns,
                "price_valid_ratio": round(price_valid_ratio, 6),
                "invalid_numeric_values": invalid_numeric,
                "quality_passed": not missing_columns and price_valid_ratio >= 0.80,
            }

            cur.execute("""
                UPDATE mlops.batch_audit
                SET validation_results = validation_results || %s::jsonb,
                    result = 'validated_bronze',
                    updated_at = NOW()
                WHERE batch_id = %s
            """, (json.dumps(validation_results), batch_id))

        if missing_columns or price_valid_ratio < 0.80:
            raise ValueError(f"El batch {batch_id} no superó la validación.")

        return bronze_result

    @task
    def build_silver(bronze_result):
        batch_id = bronze_result["batch_id"]

        with connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT payload FROM raw.property_batches WHERE batch_id = %s",
                (batch_id,),
            )
            row = cur.fetchone()

            if not row:
                raise ValueError(f"No existe el batch RAW {batch_id}.")

            records = json.loads(row[0])["data"]

            for start in range(0, len(records), 1000):
                rows = []
                for index, record in enumerate(records[start:start + 1000], start):
                    target_price = record.get("price")
                    features = {
                        key: value for key, value in record.items()
                        if key != "price"
                    }
                    row_hash = hashlib.sha256(
                        json.dumps(record, sort_keys=True).encode("utf-8")
                    ).hexdigest()

                    rows.append((
                        batch_id,
                        index,
                        row_hash,
                        json.dumps(features),
                        float(target_price) if target_price is not None else None,
                    ))

                execute_values(
                    cur,
                    """
                    INSERT INTO clean.property_records
                        (batch_id, source_record_index, row_hash, features, target_price)
                    VALUES %s
                    ON CONFLICT (row_hash) DO NOTHING
                    """,
                    rows,
                    template="(%s, %s, %s, %s::jsonb, %s)",
                    page_size=1000,
                )

            cur.execute(
                "SELECT count(*) FROM clean.property_records WHERE batch_id = %s",
                (batch_id,),
            )
            silver_records = cur.fetchone()[0]

            cur.execute("""
                UPDATE mlops.batch_audit
                SET silver_records = %s,
                    result = 'stored_silver',
                    updated_at = NOW()
                WHERE batch_id = %s
            """, (silver_records, batch_id))

        return {"batch_id": batch_id, "silver_records": silver_records}

    @task
    def build_gold(silver_result):
        batch_id = silver_result["batch_id"]

        with connection() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO gold.property_training_features (
                    clean_id, batch_id, target_price, brokered_by, status, bed, bath,
                    acre_lot, street, city, state, zip_code, house_size, prev_sold_year
                )
                SELECT
                    id,
                    batch_id,
                    target_price,
                    NULLIF(features->>'brokered_by', '')::double precision,
                    features->>'status',
                    NULLIF(features->>'bed', '')::double precision,
                    NULLIF(features->>'bath', '')::double precision,
                    NULLIF(features->>'acre_lot', '')::double precision,
                    NULLIF(features->>'street', '')::double precision,
                    features->>'city',
                    features->>'state',
                    NULLIF(features->>'zip_code', '')::double precision,
                    NULLIF(features->>'house_size', '')::double precision,
                    CASE
                        WHEN features->>'prev_sold_date' ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                        THEN EXTRACT(YEAR FROM (features->>'prev_sold_date')::date)::integer
                    END
                FROM clean.property_records
                WHERE batch_id = %s
                  AND target_price IS NOT NULL
                  AND target_price > 0
                ON CONFLICT (clean_id) DO NOTHING
            """, (batch_id,))

            cur.execute(
                "SELECT count(*) FROM gold.property_training_features WHERE batch_id = %s",
                (batch_id,),
            )
            gold_records = cur.fetchone()[0]

            cur.execute("""
                UPDATE mlops.batch_audit
                SET gold_records = %s,
                    result = 'stored_gold',
                    updated_at = NOW()
                WHERE batch_id = %s
            """, (gold_records, batch_id))

        return {"batch_id": batch_id, "gold_records": gold_records}

    @task
    def assess_training(gold_result):
        assessment = training_assessment(gold_result["batch_id"])
        print(json.dumps(assessment, ensure_ascii=False))
        return assessment

    @task.branch
    def decide_training(assessment):
        return {
            "eligible": "record_eligible_for_training",
            "skip": "skip_training",
        }[assessment["decision"]]

    def record_training_decision(assessment, result):
        with closing(connection()) as conn, conn, conn.cursor() as cur:
            cur.execute("""
                UPDATE mlops.batch_audit
                SET validation_results = validation_results ||
                    jsonb_build_object('training_assessment', %s::jsonb),
                    training_decision = %s,
                    decision_reason = %s,
                    result = %s,
                    updated_at = NOW()
                WHERE batch_id = %s
            """, (
                json.dumps(assessment), assessment["decision"],
                assessment["reason"], result, assessment["batch_id"],
            ))
            if cur.rowcount != 1:
                raise ValueError("No se pudo actualizar la auditoría del lote.")

    @task
    def record_eligible_for_training(assessment):
        record_training_decision(assessment, "training_eligible_pending")

    @task
    def skip_training(assessment):
        record_training_decision(assessment, "training_skipped")

    prepare = prepare_storage()
    bronze = fetch_bronze_batch()
    validated = validate_bronze(bronze)
    silver = build_silver(validated)
    gold = build_gold(silver)
    assessment = assess_training(gold)
    decision = decide_training(assessment)
    decision >> [record_eligible_for_training(assessment), skip_training(assessment)]

    prepare >> bronze >> validated >> silver >> gold


with DAG(
    dag_id="property_training_assessment_preview",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["mlops", "properties", "read-only"],
) as preview_dag:

    @task
    def preview_assessment():
        batch_id = get_current_context()["dag_run"].conf.get("batch_id")
        if not isinstance(batch_id, str) or not batch_id:
            raise ValueError("Indica un batch_id ya procesado en Gold.")
        assessment = training_assessment(batch_id)
        print(json.dumps(assessment, ensure_ascii=False))
        return assessment

    preview_assessment()
