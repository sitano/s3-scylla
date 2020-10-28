from builtins import object


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


# TODO: it must have blob_id
class S3Item(object):
    def __init__(self, bucket, key, **kwargs):
        self.bucket = bucket
        self.key = key
        self.content_type = kwargs['content_type']
        self.md5 = kwargs['md5']
        self.size = kwargs['size']
        if 'object_id' in kwargs:
            self.object_id = kwargs['object_id']
        if 'version' in kwargs:
            self.version = kwargs['version']
        if 'chunk_size' in kwargs:
            self.chunk_size = kwargs['chunk_size']
        if 'chunks_per_partition' in kwargs:
            self.chunks_per_partition = kwargs['chunks_per_partition']
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
    def __init__(self, object_id, bucket_id, version, chunk_size, chunks_per_partition,
                 content_type, creation_date, md5, size, metadata):
        self.object_id = object_id
        self.bucket_id = bucket_id
        self.version = version
        self.chunk_size = chunk_size
        self.chunks_per_partition = chunks_per_partition
        self.content_type = content_type
        self.creation_date = creation_date
        self.md5 = md5
        self.size = size
        self.metadata = metadata


class PartHeader(object):
    def __init__(self, object_id, version, part, blob_id, md5, size):
        self.object_id = object_id
        self.version = version
        self.part = part
        self.blob_id = blob_id
        self.md5 = md5
        self.size = size
