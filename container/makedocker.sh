#!/bin/sh

DIR=$(dirname $0)

TAG="latest"
REPO="crukcibioinformatics/referencebuilder:$TAG"

# Can't do this in the Dockerfile.
cp $DIR/../java/target/nf-referencebuilder-*.jar $DIR/nf-referencebuilder.jar

sudo docker build --tag "$REPO" --file Dockerfile .
#sudo docker push "$REPO"
