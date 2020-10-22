A python3 port of Fake-S3

INSTALL:

    $ python setup.py install

RUN:

    $ make start

or

    $ python -m mock_s3.main

or

    $ LOGLEVEL=DEBUG python -m mock_s3.main

AWS S3 CLI:

    $ aws s3 --endpoint http://localhost:8000 ls

Usage example:

    $ LOGLEVEL=DEBUG python -m mock_s3.main
    INFO:root:Starting server at localhost:8000, use <Ctrl-C> to stop

    $ aws s3 --endpoint http://localhost:8000 ls
    127.0.0.1 - - [22/Oct/2020 09:04:17] "GET / HTTP/1.1" 200 -

    Host: localhost:8000
    Accept-Encoding: identity
    User-Agent: aws-cli/1.16.180 Python/3.8.6 Linux/5.9.1-arch1-1 botocore/1.16.26
    X-Amz-Date: 20201022T070417Z
    X-Amz-Content-SHA256: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
    Authorization: AWS4-HMAC-SHA256 Credential=AKIA3TLA3JBNYLPJZNLO/20201022/eu-north-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=f7b624c8af7a98eb912e1741571f9c1e584b9414080f878d56fa69c827632636


    DEBUG:root:Server: BaseHTTP/0.6 Python/3.8.6
    DEBUG:root:Date: Thu, 22 Oct 2020 07:04:17 GMT
    DEBUG:root:Content-Type: application/xml
    DEBUG:root:Write:
    <?xml version="1.0" encoding="UTF-8"?>
    <ListAllMyBucketsResult xmlns="http://doc.s3.amazonaws.com/2006-03-01">
      <Owner>
        <ID>123</ID>
        <DisplayName>MockS3</DisplayName>
      </Owner>
      <Buckets>
        <Bucket>
          <Name>ivan</Name>
          <CreationDate>2020-10-20T15:34:28.000Z</CreationDate>
        </Bucket>
      </Buckets>
    </ListAllMyBucketsResult>

    $ aws --endpoint-url http://localhost:8000 s3 ls s3://ivan
    127.0.0.1 - - [22/Oct/2020 09:04:24] "GET /ivan?list-type=2&prefix=&delimiter=%2F&encoding-type=url HTTP/1.1" 200 -

    Host: localhost:8000
    Accept-Encoding: identity
    User-Agent: aws-cli/1.16.180 Python/3.8.6 Linux/5.9.1-arch1-1 botocore/1.16.26
    X-Amz-Date: 20201022T070424Z
    X-Amz-Content-SHA256: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
    Authorization: AWS4-HMAC-SHA256 Credential=AKIA3TLA3JBNYLPJZNLO/20201022/eu-north-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=72630e53cfa61c11d85adaf65666dcd837a9b71fc9c27233f2ee39200a42bdbb


    DEBUG:root:Server: BaseHTTP/0.6 Python/3.8.6
    DEBUG:root:Date: Thu, 22 Oct 2020 07:04:24 GMT
    DEBUG:root:Content-Type: application/xml
    DEBUG:root:Write:
    <?xml version="1.0" encoding="UTF-8"?>
    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01">
      <Name>ivan</Name>
      <Prefix></Prefix>
      <Marker></Marker>
      <MaxKeys>1000</MaxKeys>
      <IsTruncated>false</IsTruncated>
      <Contents>
        <Key>terraform.tfstate</Key>
        <LastModified>2020-10-20T13:34:28.000Z</LastModified>
        <ETag>&quot;3536bd3768172920b7229f2c12108480&quot;</ETag>
        <Size>156</Size>
        <StorageClass>STANDARD</StorageClass>
        <Owner>
          <ID>123</ID>
          <DisplayName>MockS3</DisplayName>
        </Owner>
      </Contents>
    </ListBucketResult>
    ----------------------------------------

    $ aws --endpoint-url http://localhost:8000 s3 cp ./setup.py s3://ivan
    127.0.0.1 - - [22/Oct/2020 09:13:32] "PUT /ivan/setup.py HTTP/1.1" 200 -

    Host: localhost:8000
    Accept-Encoding: identity
    Content-Type: text/x-python
    User-Agent: aws-cli/1.16.180 Python/3.8.6 Linux/5.9.1-arch1-1 botocore/1.16.26
    Content-MD5: 2urvBzRGXAmIJN0/rjnHzw==
    Expect: 100-continue
    X-Amz-Date: 20201022T071331Z
    X-Amz-Content-SHA256: cce188ac61a59bf4a5afba3f3d5a8d72916ac16c42d0d1f3bf9afa2d12f3f908
    Authorization: AWS4-HMAC-SHA256 Credential=AKIA3TLA3JBNYLPJZNLO/20201022/eu-north-1/s3/aws4_request, SignedHeaders=content-md5;content-type;host;x-amz-content-sha256;x-amz-date, Signature=84f49a6c5665534f16ff5db431771a1520e65cd7916f3666321317580d56e962
    Content-Length: 1277


    DEBUG:root:Server: BaseHTTP/0.6 Python/3.8.6
    DEBUG:root:Date: Thu, 22 Oct 2020 07:13:32 GMT
    DEBUG:root:Etag: "daeaef0734465c098824dd3fae39c7cf"
    DEBUG:root:Content-Type: text/xml