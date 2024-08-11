#!/bin/bash

# Get the PID of the Gunicorn master process
GUNICORN_PID=$(pgrep -f 'gunicorn')

if [ -z "$GUNICORN_PID" ]; then
  echo "Gunicorn is not running."
else
  echo "Stopping Gunicorn (PID: $GUNICORN_PID)..."
  kill $GUNICORN_PID

  # Wait for the process to terminate
  while kill -0 $GUNICORN_PID 2>/dev/null; do
    sleep 1
  done
  echo "Gunicorn stopped."
fi

# Kill any remaining Gunicorn worker processes
echo "Killing any remaining Gunicorn workers..."
pkill -f 'gunicorn: worker'

echo "All Gunicorn processes have been stopped."
