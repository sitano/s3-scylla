#!/usr/bin/env bash
set -euo pipefail

# that's quite slow, but most likely tcp overhead, would be good to test with connection reuse

# generate random file (1310720 bytes)
head -c 1310720 </dev/urandom > tmpfile
trap "rm tmpfile tmpfile_s3" EXIT

# upload object
time for i in {1..20}; do aws s3 --endpoint http://localhost:8000 cp tmpfile s3://bucket_1/tmpfile; done

# download object
time for i in {1..20}; do aws s3 --endpoint http://localhost:8000 cp s3://bucket_1/tmpfile tmpfile_s3; done

# compare
diff --report-identical-files --brief tmpfile tmpfile_s3
