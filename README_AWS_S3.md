Example of `aws s3` limitations override.

Path `~/.aws/config`.

Additional information is at https://docs.aws.amazon.com/cli/latest/topic/s3-config.html.

    [default]
      region = eu-north-1

      s3 =
        max_concurrent_requests = 1
        max_queue_size = 10000
        multipart_threshold = 10000MB
        multipart_chunksize = 16MB
        max_bandwidth = 50MB/s

