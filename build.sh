#!/bin/bash

set -o errexit -o nounset -o pipefail

v="$(./scripts/make-version.sh tag)"

echo "version=${v}"

image="${DOCKER_REGISTRY}/erda-telegraf:${v}"

echo "image=${image}"

docker build -t "${image}" \
    --label "branch=$(git rev-parse --abbrev-ref HEAD)" \
    --label "commit=$(git rev-parse HEAD)" \
    --label "build-time=$(date '+%Y-%m-%d %T%z')" \
    -f "Dockerfile" .

docker login -u "${DOCKER_REGISTRY_USERNAME}" -p "${DOCKER_REGISTRY_PASSWORD}" "${DOCKER_REGISTRY}"

docker push "${image}"

echo "image=${image}" >> $METAFILE
