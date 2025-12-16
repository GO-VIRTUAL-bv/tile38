#!/bin/bash

set -e
cd $(dirname "${BASH_SOURCE[0]}")/..

# GIT_BRANCH is the current branch name
export GIT_BRANCH=$(git branch --show-current)
# GIT_VERSION - always the last verison number, like 1.12.1.
export GIT_VERSION=$(git describe --tags --abbrev=0 --always)
# GIT_COMMIT_SHORT - the short git commit number, like a718ef0.
export GIT_COMMIT_SHORT=$(git rev-parse --short HEAD)
# DOCKER_REPO - the base repository name to push the docker build to.
export DOCKER_REPO=gv/tile38

docker buildx build \
			-f Dockerfile \
			--platform linux/arm64,linux/amd64 \
			--build-arg VERSION=$GIT_VERSION \
			--tag $DOCKER_REPO:edge \
			.