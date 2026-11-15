import hashlib
import json
import os
from datetime import datetime, timedelta
from urllib.parse import urlencode
from urllib.request import urlopen

import psycopg2
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context

API_URL = os.getenv("DATA_API_URL", "http://data-api.mlops.svc.cluster.local/data")
GROUP_NUMBER = int(os.getenv("DATA_API_GROUP", "5"))


def connection():
    return psycopg2.connect(
        host="postgres",
        dbname="mlops",
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


with DAG(
    dag_id="property_batch_ingestion",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    tags=["mlops", "properties", "ingestion"],
) as dag:

    @task
    def prepare_raw_storage():
        with connection() as conn, conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS raw")
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
                CREATE TABLE IF NOT EXISTS mlops.batch_audit (
                    batch_id TEXT PRIMARY KEY,
                    dag_run_id TEXT NOT NULL,
                    schema_received JSONB NOT NULL,
                    validation_results JSONB NOT NULL,
                    training_decision TEXT,
                    decision_reason TEXT,
                    result TEXT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)

    @task(retries=3, retry_delay=timedelta(seconds=15))
    def fetch_and_store_batch():
        context = get_current_context()
        source_url = f"{API_URL}?{urlencode({'group_number': GROUP_NUMBER})}"

        with urlopen(source_url, timeout=90) as response:
            body = response.read().decode("utf-8")

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
        first_record = records[0]
        schema_received = {
            column: type(value).__name__
            for column, value in first_record.items()
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
                VALUES (%s, %s, %s::jsonb, %s::jsonb, 'stored_raw')
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

        return {
            "batch_id": batch_id,
            "records": len(records),
            "payload_bytes": validation_results["payload_size_bytes"],
        }

    prepare_raw_storage() >> fetch_and_store_batch()