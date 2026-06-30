#!/bin/bash
set -e

mkdir -p forge
cd forge

mkdir -p cmd/api cmd/worker
mkdir -p internal/queue internal/job internal/worker internal/retry internal/storage
mkdir -p api/rest api/grpc
mkdir -p sdk/go
mkdir -p dashboard/app dashboard/components dashboard/lib
mkdir -p deploy
mkdir -p docs
mkdir -p tests

# Placeholder files so empty dirs survive git (git ignores empty directories)
touch cmd/api/.gitkeep cmd/worker/.gitkeep
touch internal/queue/.gitkeep internal/job/.gitkeep internal/worker/.gitkeep
touch internal/retry/.gitkeep internal/storage/.gitkeep
touch api/rest/.gitkeep api/grpc/.gitkeep
touch sdk/go/.gitkeep
touch dashboard/app/.gitkeep dashboard/components/.gitkeep dashboard/lib/.gitkeep
touch deploy/.gitkeep
touch docs/.gitkeep
touch tests/.gitkeep

echo "Forge project structure created."
find . -type d | sort