#!/usr/bin/env bash
set -euo pipefail

# generate random file
openssl rand -out tmpfile $((128*1024*1024))
# head -c 128M </dev/urandom > tmpfile
trap "rm tmpfile" EXIT

echo "uploading..."

# upload object
aws s3 --endpoint http://localhost:8000 cp tmpfile s3://bucket_1/tmpfile
