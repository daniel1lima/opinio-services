#!/bin/bash

# Define the application module
APP_MODULE="myapp:app"

# Number of workers
NUM_WORKERS=4

# Run Gunicorn with the specified number of workers
gunicorn --workers $NUM_WORKERS $APP_MODULE
