import hashlib
import json
import ssl
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import yaml

NAMESPACE = "mlops"
API = "https://kubernetes.default.svc"
SERVICE_ACCOUNT = Path("/var/run/secrets/kubernetes.io/serviceaccount")
TEMPLATE = Path("/opt/airflow/job-templates/candidate-job.yaml")
TRAINER_COMMIT = "f5d57f01948919b8487b437114534d66d6432b0b"
CACHE_CODE = "property-candidate-921c4da-code"
CACHE_COMMIT = "921c4da1604ffdf94ededd398bd11f12e892b9c8"
CACHE_IMAGE = "dac0rtes/mlops-jupyter:sha-155ac37e7a0500c9af689edb802f8e8051df2bde"
JOBS = f"/apis/batch/v1/namespaces/{NAMESPACE}/jobs"
PODS = f"/api/v1/namespaces/{NAMESPACE}/pods"


def job_name(dag_id, run_id):
    digest = hashlib.sha256(f"{dag_id}\n{run_id}".encode()).hexdigest()
    return f"property-candidate-{digest[:24]}"


def request(path, body=None, raw=False):
    token = (SERVICE_ACCOUNT / "token").read_text().strip()
    context = ssl.create_default_context(cafile=str(SERVICE_ACCOUNT / "ca.crt"))
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Authorization": f"Bearer {token}"}
    if data is not None:
        headers["Content-Type"] = "application/json"
    req = Request(f"{API}{path}", data=data, headers=headers)
    with urlopen(req, context=context, timeout=20) as response:
        payload = response.read()
    return payload.decode() if raw else json.loads(payload)


def make_job(name, assessment, cached_runtime):
    manifest = yaml.safe_load(TEMPLATE.read_text())
    manifest["metadata"].pop("generateName", None)
    manifest["metadata"]["name"] = name
    manifest["spec"]["ttlSecondsAfterFinished"] = 86400
    pod = manifest["spec"]["template"]["spec"]
    pod["automountServiceAccountToken"] = False
    trainer = pod["containers"][0]
    env = {item["name"]: item for item in trainer["env"]}
    env["CANDIDATE_BATCH_ID"]["value"] = assessment["batch_id"]
    env["TRAINING_REASON"]["value"] = assessment["reason"]
    env["GIT_COMMIT"]["value"] = TRAINER_COMMIT
    trainer["image"] = f"dac0rtes/mlops-property-trainer:sha-{TRAINER_COMMIT}"
    if cached_runtime:
        # Only for the explicitly requested replay on the space-limited VM.
        env["GIT_COMMIT"]["value"] = CACHE_COMMIT
        trainer["image"] = CACHE_IMAGE
        trainer["imagePullPolicy"] = "Never"
        trainer["command"] = ["python", "/opt/candidate/train.py"]
        trainer["volumeMounts"] = [
            {"name": "cached-script", "mountPath": "/opt/candidate", "readOnly": True},
        ]
        pod["nodeSelector"] = {"kubernetes.io/hostname": "nbam09"}
        pod["volumes"] = [{"name": "cached-script", "configMap": {"name": CACHE_CODE}}]
    return manifest


def parse_report(log, batch_id):
    decoder = json.JSONDecoder()
    for line in reversed([i for i in range(len(log))
                          if log[i] == "{" and (i == 0 or log[i - 1] == "\n")]):
        try:
            report, _ = decoder.raw_decode(log[line:])
        except json.JSONDecodeError:
            continue
        if not isinstance(report, dict) or report.get("batch_id") != batch_id:
            continue
        if (report.get("decision") in {"eligible_for_promotion", "rejected"}
                and report.get("promoted") is False
                and report.get("run_id") and report.get("candidate_version")):
            return report
    raise ValueError("El Job no devolvió un reporte válido para el lote.")


def run_candidate(name, assessment, cached_runtime):
    path = f"{JOBS}/{name}"
    try:
        request(path)
    except HTTPError as error:
        if error.code != 404:
            raise
        try:
            request(JOBS, make_job(name, assessment, cached_runtime))
        except HTTPError as conflict:
            if conflict.code != 409:
                raise

    deadline = time.monotonic() + 1260
    while time.monotonic() < deadline:
        conditions = {
            item["type"]: item for item in request(path).get("status", {}).get("conditions", [])
            if item.get("status") == "True"
        }
        if "Failed" in conditions:
            reason = conditions["Failed"].get("reason", "Unknown")
            raise RuntimeError(f"Job {name} falló ({reason}); revisar sus eventos y logs.")
        if "Complete" in conditions:
            query = urlencode({"labelSelector": f"job-name={name}", "fieldSelector": "status.phase=Succeeded"})
            pods = request(f"{PODS}?{query}")["items"]
            if len(pods) == 1:
                pod_name = pods[0]["metadata"]["name"]
                log = request(f"{PODS}/{pod_name}/log?container=trainer&tailLines=100", raw=True)
                return parse_report(log, assessment["batch_id"])
        time.sleep(10)
    raise TimeoutError(f"El Job {name} no entregó un resultado a tiempo.")
