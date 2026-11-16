from locust import HttpUser, between, task

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


class PropertyApiUser(HttpUser):
    wait_time = between(0.5, 1.5)

    @task(4)
    def predict_price(self):
        with self.client.post(
            "/predict",
            json={"features": EXAMPLE_PROPERTY},
            name="/predict",
            catch_response=True,
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(
                    f"Estado inesperado: {response.status_code} - {response.text}"
                )

    @task(1)
    def health(self):
        self.client.get("/health", name="/health")
