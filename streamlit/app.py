import os

import requests
import streamlit as st

API_URL = os.getenv("API_BASE_URL", "http://property-api:8000")

EXAMPLE_PROPERTY = {
    "brokered_by": 101640,
    "bed": 4,
    "bath": 2.0,
    "acre_lot": 0.38,
    "street": 1758218,
    "zip_code": 6016,
    "house_size": 1617,
    "prev_sold_year": 1999,
    "status": "for_sale",
    "city": "East Windsor",
    "state": "Connecticut",
}

st.set_page_config(page_title="Property Price MLOps", page_icon="🏠", layout="wide")
st.title("🏠 Estimación de precio inmobiliario")
st.caption("La interfaz consulta exclusivamente la API desplegada en Kubernetes.")


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

with st.form("property_prediction"):
    st.subheader("Características del inmueble")
    col1, col2, col3 = st.columns(3)

    with col1:
        brokered_by = st.number_input("Broker ID", min_value=0, value=EXAMPLE_PROPERTY["brokered_by"], step=1)
        bed = st.number_input("Habitaciones", min_value=0, value=EXAMPLE_PROPERTY["bed"], step=1)
        bath = st.number_input("Baños", min_value=0.0, value=EXAMPLE_PROPERTY["bath"], step=0.5)
        acre_lot = st.number_input("Área del lote (acres)", min_value=0.0, value=EXAMPLE_PROPERTY["acre_lot"], step=0.01, format="%.2f")

    with col2:
        street = st.number_input("Street ID", min_value=0, value=EXAMPLE_PROPERTY["street"], step=1)
        zip_code = st.number_input("Código postal", min_value=0, value=EXAMPLE_PROPERTY["zip_code"], step=1)
        house_size = st.number_input("Área construida (ft²)", min_value=0, value=EXAMPLE_PROPERTY["house_size"], step=1)
        prev_sold_year = st.number_input("Año de última venta", min_value=1900, max_value=2026, value=EXAMPLE_PROPERTY["prev_sold_year"], step=1)

    with col3:
        status = st.selectbox("Estado", ["for_sale", "sold", "ready_to_build"], index=0)
        city = st.text_input("Ciudad", value=EXAMPLE_PROPERTY["city"])
        state = st.text_input("Estado / región", value=EXAMPLE_PROPERTY["state"])

    submitted = st.form_submit_button("Estimar precio")

if submitted:
    features = {
        "brokered_by": brokered_by,
        "bed": bed,
        "bath": bath,
        "acre_lot": acre_lot,
        "street": street,
        "zip_code": zip_code,
        "house_size": house_size,
        "prev_sold_year": prev_sold_year,
        "status": status,
        "city": city,
        "state": state,
    }

    try:
        response = requests.post(f"{API_URL}/predict", json={"features": features}, timeout=30)
        if response.status_code == 422:
            st.error("Los datos no cumplen el contrato del modelo.")
            st.json(response.json())
        else:
            response.raise_for_status()
            result = response.json()

            st.success("Predicción generada y registrada para auditoría.")
            result_columns = st.columns(3)
            result_columns[0].metric("Precio estimado", f"${result['predicted_price']:,.0f}")
            result_columns[1].metric("Latencia", f"{result['latency_ms']:.2f} ms")
            result_columns[2].metric("Versión de modelo", result["model_version"])

            st.caption(
                f"Request ID: `{result['request_id']}` · "
                f"Modelo: `{result['model_name']}`"
            )
    except requests.RequestException as error:
        st.error(f"Error al consultar la API: {error}")
