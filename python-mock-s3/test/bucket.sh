#!/usr/bin/env bash
set -euo pipefail

# create bucket
aws --endpoint http://localhost:8000 --no-cli-pager s3api create-bucket --bucket bucket_1 --region eu-north-1

# list buckets
aws --endpoint http://localhost:8000 --no-cli-pager s3api list-buckets
