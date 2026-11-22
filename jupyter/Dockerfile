FROM python:3.11-slim

WORKDIR /home/jovyan/work

RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 1000 jovyan

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY notebooks /opt/notebook-templates
COPY start.sh /usr/local/bin/start-jupyter
RUN chmod +x /usr/local/bin/start-jupyter \
    && chown -R jovyan:jovyan /home/jovyan /opt/notebook-templates

USER jovyan
EXPOSE 8888

ENTRYPOINT ["/usr/local/bin/start-jupyter"]
