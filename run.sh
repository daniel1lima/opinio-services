#!/bin/bash

export PYTHONPATH=$(pwd)
export FLASK_APP=./application.py

# Define the application module
APP_MODULE="application:app"

# Number of workers
# NUM_WORKERS=4

# Run Gunicorn with the specified number of workers and append output to nohup.out
nohup gunicorn --bind 0.0.0.0:5000 $APP_MODULE >> nohup.out
