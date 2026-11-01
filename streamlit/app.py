import os

import requests
import streamlit as st

API_URL = os.getenv("API_BASE_URL", "http://diabetes-api:8000")

EXAMPLE_PATIENT = {
    "race": "Caucasian",
    "gender": "Female",
    "age": "[60-70)",
    "admission_type_id": "1",
    "discharge_disposition_id": "1",
    "admission_source_id": "7",
    "time_in_hospital": "3",
    "num_lab_procedures": "45",
    "num_procedures": "1",
    "num_medications": "12",
    "number_outpatient": "0",
    "number_emergency": "0",
    "number_inpatient": "0",
    "diag_1": "250.83",
    "diag_2": "401.9",
    "diag_3": "272.4",
    "number_diagnoses": "7",
    "max_glu_serum": "None",
    "A1Cresult": "None",
    "metformin": "No",
    "repaglinide": "No",
    "nateglinide": "No",
    "chlorpropamide": "No",
    "glimepiride": "No",
    "acetohexamide": "No",
    "glipizide": "No",
    "glyburide": "No",
    "tolbutamide": "No",
    "pioglitazone": "No",
    "rosiglitazone": "No",
    "acarbose": "No",
    "miglitol": "No",
    "troglitazone": "No",
    "tolazamide": "No",
    "examide": "No",
    "citoglipton": "No",
    "insulin": "Steady",
    "glyburide-metformin": "No",
    "glipizide-metformin": "No",
    "glimepiride-pioglitazone": "No",
    "metformin-rosiglitazone": "No",
    "metformin-pioglitazone": "No",
    "change": "No",
    "diabetesMed": "Yes",
}

st.set_page_config(
    page_title="Diabetes Readmission",
    page_icon="🩺",
    layout="wide",
)

st.title("🩺 Predicción de readmisión temprana")
st.caption("La interfaz consume exclusivamente la API FastAPI desplegada en Kubernetes.")


@st.cache_data(ttl=60)
def model_info():
    response = requests.get(f"{API_URL}/model-info", timeout=10)
    response.raise_for_status()
    return response.json()


try:
    info = model_info()
except requests.RequestException as error:
    st.error(f"No fue posible conectar con la API: {error}")
    st.stop()

st.info(
    f"Modelo activo: `{info['model_name']}` · "
    f"alias `{info['alias']}` · versión `{info['version']}`"
)

if st.button("Cargar valores de ejemplo"):
    for field, value in EXAMPLE_PATIENT.items():
        st.session_state[f"field_{field}"] = value

with st.form("prediction_form"):
    st.subheader("Datos del paciente")

    columns = st.columns(3)
    values = {}

    for index, field in enumerate(info["expected_columns"]):
        with columns[index % 3]:
            values[field] = st.text_input(
                field,
                value=EXAMPLE_PATIENT.get(field, ""),
                key=f"field_{field}",
            )

    submitted = st.form_submit_button("Predecir readmisión")

if submitted:
    try:
        response = requests.post(
            f"{API_URL}/predict",
            json={"features": values},
            timeout=30,
        )

        if response.status_code == 422:
            st.error("Los datos no cumplen el formato esperado.")
            st.json(response.json())
        else:
            response.raise_for_status()
            result = response.json()

            st.success("Predicción generada y registrada para auditoría.")
            result_columns = st.columns(3)
            result_columns[0].metric(
                "Predicción",
                result["prediction"].replace("_", " "),
            )
            result_columns[1].metric(
                "Probabilidad de readmisión <30 días",
                f"{result['early_readmission_probability']:.2%}",
            )
            result_columns[2].metric(
                "Latencia",
                f"{result['latency_ms']:.2f} ms",
            )

            st.caption(
                f"Request ID: `{result['request_id']}` · "
                f"Modelo: `{result['model_name']}` v{result['model_version']}"
            )

    except requests.RequestException as error:
        st.error(f"Error al consultar la API: {error}")