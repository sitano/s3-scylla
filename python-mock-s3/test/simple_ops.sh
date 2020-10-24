#!/usr/bin/env bash
set -euo pipefail

# generate random file (1310720 bytes)
head -c 1310720 </dev/urandom > tmpfile
trap "rm tmpfile tmpfile_s3" EXIT

# upload object
aws s3 --endpoint http://localhost:8000 cp tmpfile s3://bucket_1/tmpfile

# download object
aws s3 --endpoint http://localhost:8000 cp s3://bucket_1/tmpfile tmpfile_s3

# compare
diff --report-identical-files --brief tmpfile tmpfile_s3
