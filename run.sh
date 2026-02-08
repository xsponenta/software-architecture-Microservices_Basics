#!/bin/bash

echo "Starting microservices"

if ! docker info > /dev/null 2>&1; then
    echo "Docker is not running."
fi

echo ""
echo "Starting services..."
docker-compose up --build

echo "Services running."
