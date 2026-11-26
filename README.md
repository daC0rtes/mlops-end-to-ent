# 🏠 Proyecto Final MLOps: predicción de precios inmobiliarios

<p align="center">
  <img src="https://img.shields.io/badge/Kubernetes-K3s-326CE5?logo=kubernetes&logoColor=white" alt="K3s" />
  <img src="https://img.shields.io/badge/Apache%20Airflow-Orquestación-017CEE?logo=apache-airflow&logoColor=white" alt="Airflow" />
  <img src="https://img.shields.io/badge/MLflow-Model%20Registry-0194E2?logo=mlflow&logoColor=white" alt="MLflow" />
  <img src="https://img.shields.io/badge/Argo%20CD-GitOps-EF7B4D?logo=argo&logoColor=white" alt="Argo CD" />
  <img src="https://img.shields.io/badge/GitHub%20Actions-CI%2FCD-2088FF?logo=github-actions&logoColor=white" alt="GitHub Actions" />
</p>

Plataforma MLOps end-to-end para estimar el precio de inmuebles. El proyecto procesa lotes incrementales entregados por una API docente, conserva trazabilidad por capas **Bronze / Silver / Gold**, entrena y registra un modelo en MLflow, publica predicciones mediante FastAPI y las expone en Streamlit.

El despliegue está diseñado para una VM de recursos limitados: K3s, almacenamiento persistente acotado, imágenes versionadas y GitOps con Argo CD.

---

## 🎯 Resultado actual

- Ingesta incremental real desde `cristiandiaz13/mlops-puj:data-api-pf-v2`, grupo 5.
- DAG `property_batch_ingestion` ejecutado exitosamente con persistencia Bronze, Silver y Gold.
- Reproducción histórica con Airflow: lote 4 omitido y lote 5 entrenado, auditado y registrado en MLflow como candidato v3.
- Modelo `property_price_model`, versión **1**, alias **`champion`** en MLflow.
- Primera evaluación: **MAE 41.027,94**, **RMSE 49.531,39** y **R² 0,2332**.
- API de predicción, interfaz Streamlit, Prometheus, Grafana y Locust conectados al caso de precios.
- Cadena de despliegue demostrada: `push a main → GitHub Actions → Docker Hub → manifiesto GitOps → Argo CD → K3s`.

---

## 🏗️ Arquitectura

```mermaid
flowchart TB
    SRC[API docente<br/>lotes inmobiliarios] --> AF[Airflow<br/>property_batch_ingestion]

    subgraph K3S[K3s / namespace mlops]
        AF --> BR[(PostgreSQL<br/>Bronze / raw)]
        BR --> SI[(PostgreSQL<br/>Silver / clean)]
        SI --> GO[(PostgreSQL<br/>Gold)]
        GO --> DEC[Airflow: decisión por lote]
        DEC -->|Elegible| TR[Job de entrenamiento]
        DEC -->|Omitido| HIST[(PostgreSQL: historial por lote)]
        TR --> HIST
        TR --> MLF[MLflow Model Registry]
        MLF --> MINIO[MinIO / artefactos]

        API[FastAPI<br/>property-api] --> MLF
        API --> LOGS[(PostgreSQL<br/>inference_logs)]
        ST[Streamlit] --> API
        API -->|Historial| HIST
        LOC[Locust] --> API
        PROM[Prometheus] -->|scrape /metrics| API
        GRAF[Grafana] --> PROM
    end

    GH[GitHub Actions] --> DH[Docker Hub]
    DH --> K3S
    GH --> GIT[Repositorio / main]
    GIT --> ARGO[Argo CD]
    ARGO --> K3S
```

### Flujo end-to-end

```text
Nuevo lote de datos
  → Airflow: Bronze → validación → Silver → Gold
  → Airflow decide si omite o dispara el Job de entrenamiento
  → MLflow: versión y alias champion
  → FastAPI: /predict, auditoría y métricas
  → Streamlit y Locust consumen la API
  → Prometheus / Grafana observan el serving
  → GitHub Actions y Argo CD versionan y despliegan cambios
```

---

## 📸 Evidencia funcional

### GitOps: plataforma sincronizada

Argo CD observa `main/k8s`, aplica los manifiestos y mantiene la plataforma en estado `Synced / Healthy`. El árbol confirma que Airflow, Data API, JupyterLab, serving, observabilidad y Streamlit están desplegados como recursos administrados.

![Argo CD: plataforma Healthy y Synced](docs/evidence/01-argocd-platform-healthy.png)

### Prueba de carga y observabilidad

Locust ejecutó 705 solicitudes contra `property-api`, con 0 fallos y 5,2 RPS agregados. Grafana recibió las métricas Prometheus del mismo intervalo: 569 predicciones exitosas, modelo champion cargado y latencia p95 de 47,69 ms.

![Locust y Grafana durante prueba de carga](docs/evidence/02-load-test-grafana-locust.png)

### Integración continua e imágenes inmutables

GitHub Actions construyó la imagen de Airflow y la publicó en Docker Hub con un tag SHA inmutable. Este patrón permite identificar exactamente qué versión fue desplegada.

![GitHub Actions y Docker Hub](docs/evidence/03-ci-dockerhub-airflow.png)

El panel lista los workflows de las imágenes propias. Las ejecuciones visibles en verde corresponden a JupyterLab y Streamlit; la captura anterior muestra además el build de Airflow y su tag en Docker Hub.

![Workflows exitosos de GitHub Actions](docs/evidence/04-github-actions-successful-workflows.png)

El trabajo se integró mediante Pull Requests cerrados y fusionados, conservando una historia auditable de funcionalidades, correcciones y evidencias.

![Pull Requests fusionados del proyecto](docs/evidence/05-github-pull-requests-merged.png)

### Airflow y trazabilidad por lote

La ejecución seleccionada de `property_batch_ingestion` completó las cinco tareas hasta Gold. El historial también conserva ejecuciones con otros estados.

![Airflow: ejecución completada hasta Gold](docs/evidence/06-airflow-medallion.png)

La consulta de auditoría cruza el identificador de Bronze con los conteos de Silver y Gold. La captura registra un corte histórico: `group-5-batch-4` alcanzó Gold con 320.696 filas.

![PostgreSQL: conteos por capa para el mismo lote](docs/evidence/07-postgres-batch-trace.png)

### MLflow y exploración en JupyterLab

El experimento `property_price_regression` reúne el entrenamiento que originó `property_price_model` v1, los runs de EDA y la evaluación de drift del lote 5.

![MLflow: listado de runs y modelo registrado v1](docs/evidence/08-mlflow-property-runs.png)

El reporte Sweetviz, generado en JupyterLab a partir de 10.000 filas de Gold, quedó guardado como artefacto en MLflow. El histograma de `target_price` caracteriza la referencia inicial.

![Sweetviz: distribución del precio en la muestra Gold](docs/evidence/09-mlflow-sweetviz.png)

### Decisión de entrenamiento por lote en Airflow

El DAG de vista previa consultó dos lotes históricos en Gold. El lote 4 devolvió `skip` porque su auditoría no confirmaba la calidad de Bronze. El lote 5 devolvió `eligible`: la variación total de `house_size` frente al lote 4 fue 0,2773, superior al umbral de 0,20. Las nuevas ciudades representaron el 4,48 % de las filas con ciudad, por debajo del umbral de 10 %. La vista previa opera en solo lectura.

![Airflow: comparación de los XCom del lote 4 y del lote 5](docs/evidence/10-airflow-xcom-batch-decisions.png)

### Entrenamiento orquestado y trazabilidad

Dos ejecuciones del DAG `property_training_replay` reutilizaron lotes ya persistidos. La del lote 4 ejecutó `skip_training`. En la del lote 5, `train_candidate` lanzó el Job de Kubernetes y `hold_promotion` registró el resultado sin modificar el alias productivo. La ruta de ingesta y el replay comparten la lógica de decisión y entrenamiento. Estas dos capturas provienen del replay histórico.

![Airflow: lote 4, rama de omisión](docs/evidence/13-airflow-replay-batch-4-skip.png)

![Airflow: lote 5, entrenamiento y promoción en revisión](docs/evidence/14-airflow-replay-batch-5-train.png)

| Lote | Estado en `mlops.property_training_runs` | Job de Kubernetes | Run de MLflow | Versión candidata |
|---|---|---|---|---|
| `group-5-batch-4` | `training_skipped` | — | — | — |
| `group-5-batch-5` | `promotion_held` | `property-candidate-67141ada4ef7e8aac380f504` | `2d61d2a8135a47aba1b55694728da368` | v3 |

---

## 🥉🥈🥇 Capas de datos

| Capa | Tabla | Propósito |
|---|---|---|
| Bronze / RAW | `raw.property_batches` | Respuesta original de cada lote, identificador, fecha, hash, conteo y payload como texto. |
| Silver / CLEAN | `clean.property_records` | Registros normalizados y deduplicados por `row_hash`. |
| Gold | `gold.property_training_features` | Variables tipadas listas para entrenar el modelo de precio. |
| Auditoría | `mlops.batch_audit` | Esquema recibido, validaciones, conteos y resultado por lote. |
| Historial de entrenamiento | `mlops.property_training_runs` | Decisión por ejecución, modo, Job, comparación y run de MLflow. |

La respuesta RAW se almacena como `TEXT`, no como JSONB completo. Un lote real de 16,4 MB produjo `OOMKilled` al transformarlo directamente en PostgreSQL con 1 GiB; conservar el cuerpo original y transformar por etapas controla el consumo de memoria.

---

## 🔄 Orquestación con Airflow

El DAG `property_batch_ingestion` procesa un evento de datos por ejecución:

```text
prepare_storage
  → fetch_bronze_batch
  → validate_bronze
  → build_silver
  → build_gold
  → assess_training
  → decide_training
    ├─ skip_training
    └─ train_candidate → decide_promotion → hold_promotion / record_rejection
```

Controles incluidos:

- Consulta incremental: cada llamada consume un lote nuevo del grupo asignado.
- Validación de columnas esperadas y proporción válida de precios.
- Deduplicación por hash en Silver.
- Auditoría de conteos Bronze, Silver y Gold.
- HTTP 400 por lotes agotados tratado como final controlado, no como caída del DAG.

---

## 🧪 Modelo, registro y promoción

El Job `property-price-training` lee Gold y usa una muestra máxima de 75.000 filas para respetar los límites de memoria de la VM. El modelo es un `HistGradientBoostingRegressor` con tratamiento de variables numéricas y categóricas.

MLflow registra parámetros, métricas, contrato de datos y el artefacto del modelo. La API resuelve siempre:

```text
models:/property_price_model@champion
```

FastAPI resuelve el alias y carga el modelo al iniciar el proceso. En esta versión, cambiar el alias en MLflow requiere reiniciar el proceso para cargar la nueva versión.

La [bitácora de experimentación](jupyter/notebooks/README.md) documenta el cambio de distribución entre los lotes 4 y 5 y los primeros candidatos evaluados en JupyterLab. En un primer ensayo se lanzó manualmente el Job que registró v2. En el replay posterior, Airflow lanzó el Job que registró v3. Ambos ensayos reutilizaron la imagen local de JupyterLab para evitar otra descarga en el nodo. El champion siguió en v1.

El Job reservó un conjunto de prueba mediante una partición determinista por `clean_id` y evaluó en él al candidato v2 y al champion v1. La regla exige al menos 3 % de mejora en MAE y que el RMSE no empeore más de 1 %. Se prioriza MAE porque expresa directamente el error absoluto de precio; RMSE limita el deterioro ante errores grandes.

| Métrica en el test del lote 5 | Champion v1 | Candidato v2 | Reducción |
|---|---:|---:|---:|
| MAE | 722.391 | 374.534 | 48,15 % |
| RMSE | 1.560.576 | 1.009.044 | 35,34 % |

El run `10d1f09cc7f549ba824fe3eb12d64cf0` registró `eligible_for_promotion` y conservó v1 como `champion`. El resultado corresponde al test del lote 5; la exposición histórica del champion a esas filas quedó sin verificar.

![MLflow: comparación en el mismo test del lote 5 y decisión registrada](docs/evidence/11-mlflow-champion-candidate-comparison.png)

![MLflow Model Registry: candidato v2 registrado y alias champion en v1](docs/evidence/12-mlflow-model-registry.png)

---

## 🚀 Serving e interfaz

### FastAPI

`property-api` expone:

| Endpoint | Uso |
|---|---|
| `GET /health` | Comprueba disponibilidad de PostgreSQL y del modelo. |
| `GET /model-info` | Nombre, alias, versión y variables esperadas. |
| `GET /training-history` | Última decisión registrada por lote, con comparación y referencia al run de MLflow. |
| `POST /predict` | Valida el contrato, estima precio y registra auditoría. |
| `GET /metrics` | Métricas Prometheus de predicciones, errores, latencia y modelo cargado. |

Cada inferencia guarda `request_id`, payload, precio estimado, modelo, versión y latencia en `mlops.inference_logs`.

### Streamlit

La interfaz consulta `property-api`. La vista de **Predicción** muestra el precio estimado, la latencia, la versión usada y el identificador de la solicitud. **Historial por lote** muestra la última decisión registrada, los motivos, las variaciones de MAE y RMSE, las versiones y el run de MLflow. Los lotes anteriores sin registro de entrenamiento aparecen como **Sin decisión registrada**.

![Streamlit: historial por lote con omisión y promoción en revisión](docs/evidence/15-streamlit-training-history.png)

![Streamlit: predicción de ejemplo con champion v1 y Request ID](docs/evidence/16-streamlit-prediction.png)

---

## 📈 Observabilidad y carga

- **Prometheus** hace scrape de `property-api:8000/metrics` cada 5 segundos.
- **Grafana** muestra predicciones, errores, estado del modelo `champion`, latencia p95 y tasa de error.
- **Locust** simula usuarios contra el Service interno `property-api:8000`; 80% de las tareas usan `/predict` y 20% `/health`.

La carga se mantiene controlada para la VM: se recomienda iniciar con 5 usuarios y tasa de spawn 1 usuario/segundo.

---

## 🔁 CI/CD y GitOps

### GitHub Actions

Los workflows construyen imágenes versionadas con el SHA del commit:

| Workflow | Imagen |
|---|---|
| `build-airflow.yml` | `dac0rtes/mlops-airflow:sha-<commit>` |
| `build-trainer.yml` | `dac0rtes/mlops-property-trainer:sha-<commit>` |
| `build-property-api.yml` | `dac0rtes/mlops-property-api:sha-<commit>` |
| `build-property-streamlit.yml` | `dac0rtes/mlops-property-streamlit:sha-<commit>` |

Para API y Streamlit, un push a `main` construye la imagen, actualiza automáticamente el tag correspondiente en el manifiesto y hace commit de despliegue. El cambio de manifiesto no dispara un bucle de build.

### Argo CD

La Application `mlops-platform` observa `main/k8s` y se validó como **Synced / Healthy**. El archivo `k8s/kustomization.yaml` define los servicios que Argo administra:

```text
data-api · Airflow · property-api · Prometheus · Grafana · Streamlit · Locust · JupyterLab
```

El Job de entrenamiento está deliberadamente fuera de esa lista: una sincronización GitOps no debe relanzar un entrenamiento por accidente.

---

## 🛠️ Retos técnicos y decisiones

| Reto | Decisión | Aprendizaje |
|---|---|---|
| `/var` se llenó con datos de K3s. | Migración a `/opt/k3s-data`. | Revisar disco antes de descargar imágenes o escalar servicios. |
| Data API fallaba con 256 MiB. | Límite de 1 GiB y probes TCP. | La memoria debe dimensionarse con lotes reales. |
| PostgreSQL fue OOM al usar JSONB completo. | RAW como `TEXT`; transformación por capas. | Preservar trazabilidad no implica transformar todo en memoria. |
| Scheduler Airflow se reiniciaba. | Sonda TCP al puerto 8793. | Un liveness probe mal diseñado puede causar la falla que pretende detectar. |
| Grafana conservó metadatos del dashboard antiguo. | UID y archivo de provisión nuevos. | ConfigMaps y PVCs pueden tener tiempos de actualización diferentes. |
| Rama Git tuvo conflicto tras Squash merge. | Rebase con backup y `--force-with-lease`. | Squash cambia la historia; no reutilizar una rama fusionada sin rebasarla. |

---

## 📦 Estructura del repositorio

```text
.
├── .github/workflows/      # CI y actualización de tags GitOps
├── airflow/dags/           # property_ingestion_dag.py
├── api/                    # FastAPI de serving
├── argocd/                 # Application mlops-platform
├── k8s/
│   ├── base/               # Manifiestos Kubernetes
│   └── kustomization.yaml  # Alcance administrado por Argo CD
├── locust/                 # Prueba de carga
├── streamlit/              # Interfaz de precio
├── trainer/                # Entrenamiento y registro MLflow
└── docs/                   # Handoff y material de sustentación
```

---

## ▶️ Operación y demo local

### Salud mínima

```bash
df -h / /var /home
kubectl top node
kubectl -n mlops get pods
kubectl -n argocd get applications
```

### Interfaces de demostración

| Servicio | URL | Port-forward |
|---|---|---|
| FastAPI / Swagger | `http://IP_VM:8010/docs` | `svc/property-api 8010:8000` |
| MLflow | `http://IP_VM:8012` | `svc/mlflow 8012:5000` |
| JupyterLab | `http://IP_VM:8011/lab` | `svc/jupyterlab 8011:8888` |
| Prometheus | `http://IP_VM:8003` | `svc/prometheus 8003:9090` |
| Grafana | `http://IP_VM:8004` | `svc/grafana 8004:3000` |
| Streamlit | `http://IP_VM:8005` | `svc/streamlit 8005:8501` |
| Locust | `http://IP_VM:8006` | `svc/locust 8006:8089` |
| Airflow | `http://IP_VM:8007` | `svc/airflow-webserver 8007:8080` |
| Argo CD | `https://IP_VM:8008` | `svc/argocd-server 8008:443` |

Ejemplo:

```bash
kubectl -n mlops port-forward --address 0.0.0.0 \
  svc/streamlit 8005:8501
```

> Las credenciales no se publican en Git. Antes de cualquier demostración, confirmar que no existen `DiskPressure` ni Pods en error.

---

