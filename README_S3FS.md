How to mount S3 Scylla with a filesystem:

    $ s3fs ivan /tmp/m -d -f -s -o url=http://localhost:8000 -o nomultipart
