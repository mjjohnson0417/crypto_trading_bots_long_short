#!/bin/bash

# Start Order Flow Trading Bot - Kraken
# Activates venv and runs the main entry point.

cd "$(dirname "$0")"
source start_venv.sh
exec python3 main.py
