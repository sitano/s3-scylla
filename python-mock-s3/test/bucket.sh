#!/usr/bin/env bash
set -euo pipefail

ENDPOINT="${ENDPOINT:http://13.49.78.57:80}"
echo "creating on ${ENDPOINT}...."

# create bucket
aws --endpoint ${ENDPOINT} --no-cli-pager s3api create-bucket --bucket bucket_1 --region eu-north-1

aws --endpoint ${ENDPOINT} --no-cli-pager s3api create-bucket --bucket bucket_2 --region eu-north-1

# list buckets
aws --endpoint ${ENDPOINT} --no-cli-pager s3api list-buckets
