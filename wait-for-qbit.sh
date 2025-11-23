#!/bin/bash

host=172.17.0.1
port=8080

echo "Waiting for qbit to be ready..."
until curl -f -s -o /dev/null "http://$host:$port/api/v2/app/version"; do
	echo "Service at $host:$port is not ready yet. Retrying in 2 seconds..."
	sleep 2
done

echo "qbit is ready! Starting application..."
exec "$@"
