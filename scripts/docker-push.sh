#!/bin/bash

set -e
cd $(dirname "${BASH_SOURCE[0]}")/..

# GIT_BRANCH is the current branch name
export GIT_BRANCH=$(git branch --show-current)
# GIT_VERSION - always the last verison number, like 1.12.1.
export GIT_VERSION=$(git describe --tags --abbrev=0)
# GIT_COMMIT_SHORT - the short git commit number, like a718ef0.
export GIT_COMMIT_SHORT=$(git rev-parse --short HEAD)
# DOCKER_REGISTRY - the registry to push to. Empty means Docker Hub.
export DOCKER_REGISTRY=${DOCKER_REGISTRY:-}
# DOCKER_REPO - the base repository name to push the docker build to.
if [ "$DOCKER_REGISTRY" == "" ]; then
	export DOCKER_REPO=$DOCKER_USER/tile38
else
	export DOCKER_REPO=$DOCKER_REGISTRY/$DOCKER_USER/tile38
fi

if [ "$GIT_BRANCH" != "master" ]; then
	echo "Not pushing, not on master"
elif [ "$DOCKER_USER" == "" ]; then
	echo "Not pushing, DOCKER_USER not set"
	exit 1
elif [ "$DOCKER_LOGIN" == "" ]; then
	echo "Not pushing, DOCKER_LOGIN not set"
	exit 1
elif [ "$DOCKER_PASSWORD" == "" ]; then
	echo "Not pushing, DOCKER_PASSWORD not set"
	exit 1
else
	# setup cross platform builder
	# https://github.com/tonistiigi/binfmt
	docker run --privileged --rm tonistiigi/binfmt --install all
	docker buildx create --name multiarch --platform linux/amd64,linux/amd64/v2,linux/amd64/v3,linux/arm64,linux/386,linux/arm/v7 --use default

	# docker login
	echo $DOCKER_PASSWORD | docker login $DOCKER_REGISTRY -u $DOCKER_LOGIN --password-stdin
	# On Docker Hub, skip re-tagging version/latest if this version already exists.
	# Custom registries have no Hub API to check, so always do the full build.
	if [ "$DOCKER_REGISTRY" != "" ] || [ "$(curl -s https://hub.docker.com/v2/repositories/$DOCKER_REPO/tags/$GIT_VERSION/ | grep "digest")" == "" ]; then
		# build the docker image
		docker buildx build \
			-f Dockerfile \
			--platform linux/arm64,linux/amd64 \
			--build-arg VERSION=$GIT_VERSION \
			--tag $DOCKER_REPO:$GIT_VERSION \
			--tag $DOCKER_REPO:latest \
			--tag $DOCKER_REPO:edge \
			--push \
			.
	else
		# build the docker image
		docker buildx build \
			-f Dockerfile \
			--platform linux/arm64,linux/amd64 \
			--build-arg VERSION=$GIT_VERSION \
			--tag $DOCKER_REPO:edge \
			--push \
			.
	fi
fi
