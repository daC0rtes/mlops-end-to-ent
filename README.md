# Plataforma MLOps de readmision temprana - Diabetes

Proyecto académico que implementa un ciclo MLOps reproducible para estimar la probabilidad de readmisión hospitalaria antes de 30 días en pacientes con diabetes. La solución se despliega en un clúster K3s local y separa datos, experimentación, serving, interfaz, observabilidad y pruebas de carga.

## 1. Arquitectura en una mirada

```text
Dataset -> Airflow DAG -> PostgreSQL (raw / clean)
                         |
                         v
                 Entrenamiento -> MLflow <-> MinIO (artefactos)
                                      |
                                      v
                             champion v1 -> FastAPI -> PostgreSQL (inference_logs)
                                                ^
                                                |
                                           Streamlit / Locust
                                                |
                                                v
                                      Prometheus -> Grafana
```

## 2. Kubernetes sin misterio

Kubernetes es la capa que mantiene los servicios ejecutándose y comunicados entre sí. Docker construye una imagen; Kubernetes descarga esa imagen y ejecuta sus contenedores como Pods.

| Recurso | Uso en este proyecto |
|---|---|
| `Deployment` | FastAPI, MLflow, Airflow, Streamlit, Prometheus, Grafana y Locust. Mantiene una réplica disponible. |
| `StatefulSet` | PostgreSQL y MinIO. Conservan identidad y datos persistentes. |
| `Service` | Da un nombre interno estable, por ejemplo `diabetes-api:8000` o `postgres:5432`. |
| `PVC` | Reserva disco persistente para PostgreSQL, MinIO, Prometheus y Grafana. |
| `ConfigMap` | Configuración no sensible, scripts y dashboards. |
| `Secret` | Credenciales de PostgreSQL, MinIO, Airflow y Grafana. |
| `Job` | Tareas que terminan, como crear el bucket de MLflow o entrenar el modelo inicial. |
| `manifest` YAML | Declaración versionable del estado deseado; se aplica con `kubectl apply -f`. |

Los manifiestos se encuentran en `k8s/base/`. Las imágenes propias se publican en Docker Hub y Kubernetes las consume por tag o digest, evitando depender de la imagen local de la VM.

## 3. Componentes implementados

| Componente | Responsabilidad | Validación lograda |
|---|---|---|
| PostgreSQL | Capas `raw`, `clean` e `inference_logs`; metadatos de MLflow y Airflow. | 101.766 registros iniciales y auditoría de inferencias. |
| MinIO | Almacén S3 compatible para artefactos. | Bucket `mlflow` creado. |
| MLflow | Experimentos, registro y alias de modelos. | `diabetes_readmission` v1 con alias `champion`. |
| Airflow | Orquestación del pipeline. | DAG con validación, lote máximo de 15.000, calidad, transformación, entrenamiento, comparación y promoción. |
| FastAPI | Serving del modelo y métricas. | `/health`, `/model-info`, `/predict`, `/metrics`; carga dinámica de `champion`. |
| Streamlit | Interfaz de usuario. | Consume únicamente la API. |
| Prometheus y Grafana | Scraping y dashboard de observabilidad. | Target de FastAPI `up=1`; RPS, p95, solicitudes y errores visibles. |
| Locust | Prueba de carga. | Carga válida contra `/predict` desde el clúster. |

## 4. Resultado del experimento

- Dataset: 101.766 encuentros clínicos.
- Objetivo: `readmitted == "<30"`; representa 11,16% de los registros.
- Modelo: `diabetes_readmission`, versión `1`, alias `champion`.
- Métricas de prueba: F1 positiva `0,2625`, recall positivo `0,5500`, ROC-AUC `0,6516`, PR-AUC `0,2089`.
- Prueba de carga: 1.868 predicciones exitosas visibles en Grafana, 26,3 RPS y p95 de 204,23 ms.
- Auditoría total al cierre: 7.884 inferencias en PostgreSQL, latencia promedio de 57,21 ms.

## 5. Despliegue o réplica resumida

### Prerrequisitos

- K3s/Kubernetes funcional y `kubectl` configurado.
- Docker Hub autenticado para construir y publicar imágenes propias.
- Al menos 6 GB libres en `/var`; confirmar que el nodo no tiene `DiskPressure`.
- Puertos externos de la VM disponibles entre 8000 y 8025.

### Verificación inicial

```bash
kubectl get nodes -o wide
kubectl top nodes
kubectl get storageclass
df -h /var /home
```

### Despliegue en orden

```bash
kubectl apply -f k8s/base/namespace.yaml
kubectl apply -f k8s/base/secrets.yaml
kubectl apply -f k8s/base/postgres.yaml
kubectl -n mlops rollout status statefulset/postgres --timeout=180s

kubectl apply -f k8s/base/minio.yaml
kubectl -n mlops rollout status statefulset/minio --timeout=180s

kubectl apply -f k8s/base/mlflow.yaml
kubectl -n mlops rollout status deployment/mlflow --timeout=180s

kubectl apply -f k8s/base/airflow.yaml
kubectl apply -f k8s/base/airflow-init.yaml
kubectl -n mlops wait --for=condition=complete job/airflow-init --timeout=300s

kubectl apply -f k8s/base/api.yaml
kubectl apply -f k8s/base/streamlit.yaml
kubectl apply -f k8s/base/prometheus.yaml
kubectl apply -f k8s/base/grafana.yaml
kubectl apply -f k8s/base/locust.yaml
```

> Ajusta los nombres de manifiestos si el repositorio de réplica los organiza de otra forma. La regla importante es respetar las dependencias: persistencia antes de servicios, modelo registrado antes de API, API antes de interfaz/carga, y API antes de Prometheus.

### Verificación compacta

```bash
kubectl -n mlops get deploy,statefulset,job,pods,svc,pvc
kubectl -n mlops get endpoints
kubectl -n mlops logs deployment/mlflow --tail=30
kubectl -n mlops logs deployment/diabetes-api --tail=30
```

## 6. Ejecución del DAG

Abrir Airflow, activar `diabetes_mlops_pipeline` y ejecutar manualmente una corrida. El DAG valida la fuente, carga un batch de máximo 15.000 filas, aplica calidad y transformación, entrena, registra el modelo, compara métricas y promueve el mejor como `champion`.

Comprobación del lote:

```bash
kubectl -n mlops exec postgres-0 -- sh -c '
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
SELECT batch_id, count(*) FROM raw.patient_records
WHERE batch_id = '\''airflow-batch-001'\'' GROUP BY batch_id;"
'
```

## 7. Interfaces externas de la VM

Los servicios usan `ClusterIP` dentro del clúster. Para demostrarlos desde el computador se abre un `port-forward` por servicio y se mantiene esa terminal abierta.

| Servicio | Puerto externo | Comando base |
|---|---:|---|
| MLflow | 8001 | `kubectl -n mlops port-forward --address 0.0.0.0 svc/mlflow 8001:5000` |
| MinIO | 8002 | `kubectl -n mlops port-forward --address 0.0.0.0 svc/minio 8002:9001` |
| Prometheus | 8003 | `kubectl -n mlops port-forward --address 0.0.0.0 svc/prometheus 8003:9090` |
| Grafana | 8004 | `kubectl -n mlops port-forward --address 0.0.0.0 svc/grafana 8004:3000` |
| Streamlit | 8005 | `kubectl -n mlops port-forward --address 0.0.0.0 svc/streamlit 8005:8501` |
| Locust | 8006 | `kubectl -n mlops port-forward --address 0.0.0.0 svc/locust 8006:8089` |
| Airflow | 8007 | `kubectl -n mlops port-forward --address 0.0.0.0 svc/airflow-webserver 8007:8080` |

Abrir en el navegador como `http://IP_DE_LA_VM:PUERTO`. Si una interfaz no abre, verificar que el `port-forward` siga activo y que el puerto esté permitido por el firewall.

## 8. Prueba de carga y monitoreo

1. Abrir Locust y usar como host `http://diabetes-api:8000`.
2. Ejecutar, por ejemplo, 10 usuarios con tasa de arranque de 2 usuarios/segundo durante 60-90 segundos.
3. Abrir Grafana con rango de tiempo `Last 5 minutes` y refresco de 5 segundos.
4. Confirmar incremento de solicitudes exitosas, RPS, latencia p95 y registros de inferencia.

Consulta final de auditoría:

```bash
kubectl -n mlops exec postgres-0 -- sh -c '
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
SELECT count(*) AS inferencias_auditadas,
       round(avg(latency_ms)::numeric, 2) AS latencia_promedio_ms
FROM mlops.inference_logs;"
'
```

## 9. Evidencia mínima para entrega

1. Nodo Kubernetes saludable y pods/servicios/PVC en estado correcto.
2. Airflow con DAG exitoso y evidencia del batch incremental.
3. MLflow con modelo `champion` y MinIO con artefactos.
4. Predicción desde Streamlit o FastAPI y fila de auditoría en PostgreSQL.
5. Grafana con métricas vivas mientras Locust genera carga.
6. Video de máximo 10 minutos que narre la arquitectura, decisiones y resultados.

## 10. Retos técnicos y lecciones de réplica

Estas decisiones surgieron de incidentes reales del despliegue y son especialmente útiles al adaptar la plataforma a otro caso MLOps.

| Reto | Causa / riesgo | Solución aplicada | Lección replicable |
|---|---|---|---|
| Red de K3s | El rango de Services por defecto se solapaba con la red física de la VM. | Se reinstaló K3s con Pods `10.244.0.0/16` y Services `10.245.0.0/16`. | Elegir CIDR de Pods y Services que no choque con las redes existentes antes de desplegar cargas. |
| `DiskPressure` | Rancher embebido, imágenes antiguas y capas de contenedores llenaron `/var`. | Limpieza dirigida de contenedores/imágenes obsoletos y revisión de `df -h`, `docker system df` y condiciones del nodo. | Nunca hacer `prune` indiscriminado: identificar primero qué pertenece a otro proyecto y preservar volúmenes necesarios. |
| Imágenes reproducibles | Tags viejos o incompatibilidades de dependencias pueden fallar en tiempo de despliegue. | Imágenes propias en Docker Hub; MLflow fijó `SQLAlchemy==2.0.54`; los despliegues relevantes usan digest. | Fijar versiones y preferir digest para producción/demo repetible. |
| Scheduler de Airflow | Un liveness probe costoso con timeout corto provocaba reinicios falsos. | Se cambió por una comprobación ligera del proceso y se validó un pod sin reinicios. | Un probe debe comprobar salud rápida; no debe ejecutar tareas lentas ni depender de recursos externos innecesarios. |
| MinIO / imágenes minimalistas | Algunos clientes minimalistas no traen `/bin/sh`. | Se adaptó el Job para ejecutar el cliente compatible y se separaron pasos cuando fue necesario. | Leer los eventos del Pod y no asumir que toda imagen incluye shell o herramientas de depuración. |
| Observabilidad | Confundir métricas, logs y auditoría lleva a demos imprecisas. | Prometheus scrapea `/metrics`; Grafana consulta Prometheus; `kubectl logs` y PostgreSQL contienen logs/auditoría. | En esta arquitectura no hay Loki. Solo declararlo si realmente está desplegado como fuente de logs de Grafana. |

### Flujo interno que debe mantenerse al adaptar el proyecto

```text
Airflow -> PostgreSQL raw/clean -> entrenamiento -> MLflow/MinIO
MLflow alias champion -> FastAPI -> PostgreSQL inference_logs
Streamlit y Locust -> FastAPI
FastAPI /metrics -> Prometheus -> Grafana
```

Las piezas de UI, carga y observabilidad no deben conectarse directamente a la base de datos o al almacenamiento de artefactos. FastAPI mantiene el contrato de serving y PostgreSQL concentra la auditoría estructurada.
