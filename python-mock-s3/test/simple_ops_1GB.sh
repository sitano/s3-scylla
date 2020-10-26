#!/usr/bin/env bash
set -euo pipefail

# generate random file
openssl rand -out tmpfile $((1*1024*1024*1024))
# head -c 1G </dev/urandom > tmpfile
trap "rm tmpfile tmpfile_s3" EXIT

echo "uploading..."

# upload object
time aws s3 --endpoint http://localhost:8000 cp tmpfile s3://bucket_1/tmpfile

# download object
time aws s3 --endpoint http://localhost:8000 cp s3://bucket_1/tmpfile tmpfile_s3

# compare
diff --report-identical-files --brief tmpfile tmpfile_s3
