#!/usr/bin/env sh
set -euo pipefail

# This is a simple script used to try out running MatchEngine in a Docker container.

# We assume that the user is running this script the way they'd run the CLI, with
# a secrets file in the SECRETS_JSON variable.
SECRETS_JSON_PATH="`realpath $SECRETS_JSON`"
CONTAINER_NAME="matchengine_`date +"%Y%m%d_%H%M%S"`"
IMAGE_TAG="matchengine_test:latest"

# Build the image
docker build --tag "$IMAGE_TAG" .

# Run the container with the command-line arguments
set +e
docker run --rm \
  --tty --interactive \
  --volume "$SECRETS_JSON_PATH:/secrets.json:ro" \
  --read-only \
  --name "$CONTAINER_NAME" \
  "$IMAGE_TAG" \
  "$@"
EXIT_CODE="$?"
if test "$EXIT_CODE" -eq 137
then
  echo "MatchEngine exited because it ran out of memory"
  echo "Try increasing the memory available to Docker"
fi
exit $EXIT_CODE
