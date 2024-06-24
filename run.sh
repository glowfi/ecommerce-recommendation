#!/usr/bin/env bash

# WORKERS=$(grep -c ^processor /proc/cpuinfo)
WORKERS=3

echo "Production build started with ${WORKERS} workers!"

# Clean pycache
find . | grep *__pycache__/ | xargs -I "{}" rm -rf "{}"

# Remove print statements
remove-print-statements ./**/*.py

# Start Web app
gunicorn main:app -w "${WORKERS}" -k uvicorn.workers.UvicornWorker
