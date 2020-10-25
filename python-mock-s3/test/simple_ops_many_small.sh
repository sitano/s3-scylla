#!/usr/bin/env bash
set -euo pipefail

# that's quite slow, but most likely tcp overhead, would be good to test with connection reuse

# generate random file (1048576 bytes)
mkdir -p tmpdir
for i in {1..1024}; do
  head -c 1048576 </dev/urandom > tmpdir/tmpfile_${i};
done
trap "rm -rf tmpdir tmpdir_s3" EXIT

# upload objects
time aws s3 --endpoint http://localhost:8000 cp tmpdir s3://bucket_1/tmpdir --recursive

echo "sleeping before read..."
sleep 20

# download objects
time aws s3 --endpoint http://localhost:8000 cp s3://bucket_1/tmpdir tmpdir_s3 --recursive

# compare
for i in {1..1024}; do
  diff --report-identical-files --brief tmpdir/tmpfile_${i} tmpdir_s3/tmpfile_${i};
done