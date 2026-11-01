FROM python:3.11-slim

RUN pip install --no-cache-dir \
    mlflow==2.17.2 \
    SQLAlchemy==2.0.54 \
    boto3==1.35.54 \
    psycopg2-binary==2.9.10

EXPOSE 5000

ENTRYPOINT ["mlflow"]
