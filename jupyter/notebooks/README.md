# Notebooks de experimentación y evidencia

Estos notebooks se ejecutan en JupyterLab contra servicios internos de K3s.
No contienen secretos: las credenciales se inyectan como variables de entorno
del Deployment.

## Recorrido recomendado

1. `00_platform_overview.ipynb`: conectividad y capas Medallion.
2. `01_eda_baseline_sweetviz.ipynb`: EDA, reporte Sweetviz y baseline.
3. `02_data_drift.ipynb`: exploración inicial de drift por lotes.
4. `03_concept_drift.ipynb`: desempeño del champion sobre datos etiquetados.
5. `04_candidate_training.ipynb`: candidates evaluados sin promoción.
6. `05_final_drift_decision.ipynb`: conclusión consolidada y decisión operativa.

Los archivos de `jupyter/reports/` son evidencias estáticas. Los mismos
artefactos relevantes también se registran en MLflow/MinIO. Los checkpoints
automáticos de Jupyter no se versionan.
