#!/bin/bash

set -o errexit -o nounset -o pipefail

v="$(head -n 1 VERSION)"
v="${v}-$(date '+%Y%m%d')-$(git rev-parse --short HEAD)"

image="registry.cn-hangzhou.aliyuncs.com/terminus/spot-telegraf:${v}"

docker build -t "${image}" \
    --label "branch=$(git rev-parse --abbrev-ref HEAD)" \
    --label "commit=$(git rev-parse HEAD)" \
    --label "build-time=$(date '+%Y-%m-%d %T%z')" \
    -f "Dockerfile" .
docker login -u "${TERMINUS_DOCKERHUB_ALIYUN_USERNAME}" -p "${TERMINUS_DOCKERHUB_ALIYUN_PASSWORD}" registry.cn-hangzhou.aliyuncs.com
docker push "${image}"