#!/bin/sh
set -eu

mkdir -p /home/jovyan/work/notebooks

if [ -z "$(find /home/jovyan/work/notebooks -mindepth 1 -maxdepth 1 -print -quit)" ]; then
  cp -R /opt/notebook-templates/. /home/jovyan/work/notebooks/
fi

exec jupyter lab \
  --ip=0.0.0.0 \
  --port=8888 \
  --no-browser \
  --ServerApp.token="${JUPYTER_TOKEN:?JUPYTER_TOKEN is required}" \
  --ServerApp.root_dir=/home/jovyan/work
