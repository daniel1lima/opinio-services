#!/bin/bash

# Define the application module
APP_MODULE="application:app"

# Number of workers
NUM_WORKERS=4

# Run Gunicorn with the specified number of workers and append output to nohup.out
nohup gunicorn --workers $NUM_WORKERS $APP_MODULE >> nohup.out 2>&1 &
