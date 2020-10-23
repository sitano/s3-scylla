from builtins import object
import uuid


class Bucket(object):
    def __init__(self, name, bucket_id, creation_date):
        self.name = name
        self.bucket_id = bucket_id
        self.creation_date = creation_date


class BucketQuery(object):
    def __init__(self, bucket, matches=None, prefixes=None, is_truncated=False,
                 marker='', prefix='', max_keys=1000, delimiter=''):
        if matches is None:
            matches = []
        if prefixes is None:
            prefixes = []
        self.bucket = bucket
        self.matches = matches
        self.prefixes = prefixes
        self.is_truncated = is_truncated
        self.marker = marker
        self.prefix = prefix
        self.max_keys = max_keys
        self.delimiter = delimiter


class S3Item(object):
    def __init__(self, bucket, key, **kwargs):
        self.bucket = bucket
        self.key = key
        self.content_type = kwargs['content_type']
        self.md5 = kwargs['md5']
        self.size = kwargs['size']
        if 'blob_id' in kwargs:
            self.blob_id = uuid.UUID(kwargs['blob_id'])
        if 'chunk_size' in kwargs:
            self.chunk_size = kwargs['chunk_size']
        if 'chunks_per_part' in kwargs:
            self.chunks_per_part = kwargs['chunks_per_part']
        if 'creation_date' in kwargs:
            self.creation_date = kwargs['creation_date']
        if 'modified_date' in kwargs:
            self.creation_date = kwargs['modified_date']


class ObjectHeader(object):
    def __init__(self, bucket_id, key, object_id, version, metadata):
        self.bucket_id = bucket_id
        self.key = key
        self.object_id = object_id
        self.version = version
        self.metadata = metadata


class VersionHeader(object):
    def __init__(self, object_id, bucket_id, version, blob_id, chunk_size, chunks_per_part,
                 content_type, creation_date, md5, size, metadata):
        self.object_id = object_id
        self.bucket_id = bucket_id
        self.version = version
        self.blob_id = blob_id
        self.chunk_size = chunk_size
        self.chunks_per_part = chunks_per_part
        self.content_type = content_type
        self.creation_date = creation_date
        self.md5 = md5
        self.size = size
        self.metadata = metadata
