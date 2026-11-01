from locust import HttpUser, between, task

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


class DiabetesApiUser(HttpUser):
    wait_time = between(0.1, 0.4)

    @task
    def predict_readmission(self):
        with self.client.post(
            "/predict",
            json={"features": EXAMPLE_PATIENT},
            name="/predict",
            catch_response=True,
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(
                    f"Estado inesperado: {response.status_code} - {response.text}"
                )