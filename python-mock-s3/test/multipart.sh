#!/usr/bin/env bash
set -euo pipefail

# create
aws --endpoint-url http://localhost:8000 --no-cli-pager s3api create-multipart-upload --bucket marcin --key k000

# ... the rest
