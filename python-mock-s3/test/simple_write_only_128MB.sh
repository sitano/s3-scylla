#!/usr/bin/env bash
set -euo pipefail

# generate random file
openssl rand -out tmpfile $((128*1024*1024))
# head -c 128M </dev/urandom > tmpfile
trap "rm tmpfile" EXIT

ENDPOINT="${ENDPOINT:http://13.49.78.57:80}"
echo "uploading to ${ENDPOINT}...."

# upload object
aws s3 --endpoint ${ENDPOINT} cp tmpfile s3://bucket_1/tmpfile
