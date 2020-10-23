#!/usr/bin/env bash
set -euo pipefail

# upload object
dd if=/dev/zero bs=1 count=212 of=tmpfile &&
  aws s3 --endpoint http://localhost:8000 cp tmpfile s3://bucket_1/tmpfile

# download object
aws s3 --endpoint http://localhost:8000 cp s3://bucket_1/tmpfile tmpfile_s3

# upload multi-chunk object (with chunk_size = 1024*128)
dd if=/dev/zero bs=1 count=1310720 of=tmpfile_big &&
  aws s3 --endpoint http://localhost:8000 cp tmpfile_big s3://bucket_1/tmpfile_big

# download multi-chunk object
aws s3 --endpoint http://localhost:8000 cp s3://bucket_1/tmpfile_big tmpfile_big_s3