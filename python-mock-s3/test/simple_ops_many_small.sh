#!/usr/bin/env bash
set -euo pipefail

# that's quite slow, but most likely tcp overhead, would be good to test with connection reuse

# generate random file (1048576 bytes)
mkdir -p tmpdir
for i in {1..999}; do
  openssl rand -out tmpdir/tmpfile_${i} 1048576;
  # head -c 1048576 </dev/urandom > tmpdir/tmpfile_${i};
done
trap "rm -rf tmpdir tmpdir_s3" EXIT

ENDPOINT="${ENDPOINT:http://13.49.78.57:80}"
echo "uploading to ${ENDPOINT}...."

# upload objects
time aws s3 --endpoint ${ENDPOINT} cp tmpdir s3://bucket_2/tmpdir --recursive

# download objects
time aws s3 --endpoint ${ENDPOINT} cp s3://bucket_2/tmpdir tmpdir_s3 --recursive

# compare
for i in {1..999}; do
  diff --report-identical-files --brief tmpdir/tmpfile_${i} tmpdir_s3/tmpfile_${i};
done