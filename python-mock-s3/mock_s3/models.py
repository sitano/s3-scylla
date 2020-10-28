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


class S3Item(object):
    def __init__(self, bucket, obj):
        self.bucket = bucket
        self.key = obj.key
        self.object_id = obj.object_id
        self.blob_id = obj.blob_id
        self.version = obj.version
        self.parts = obj.parts
        self.content_type = obj.metadata['content_type']
        self.digest = obj.metadata['digest']
        self.size = obj.metadata['size']
        if 'chunk_size' in obj.metadata:
            self.chunk_size = obj.metadata['chunk_size']
        if 'chunks_per_partition' in obj.metadata:
            self.chunks_per_partition = obj.metadata['chunks_per_partition']
        if 'creation_date' in obj.metadata:
            self.creation_date = obj.metadata['creation_date']
        if 'modified_date' in obj.metadata:
            self.creation_date = obj.metadata['modified_date']


class ObjectHeader(object):
    def __init__(self, bucket_id, key, object_id, blob_id, version, parts, metadata):
        self.bucket_id = bucket_id
        self.key = key
        self.object_id = object_id
        self.blob_id = blob_id
        self.version = version
        self.parts = parts
        self.metadata = metadata


class VersionHeader(object):
    def __init__(self, object_id, bucket_id, blob_id, version, chunk_size, chunks_per_partition,
                 creation_date, digest, size, parts, metadata):
        self.object_id = object_id
        self.bucket_id = bucket_id
        self.blob_id = blob_id
        self.version = version
        self.chunk_size = chunk_size
        self.chunks_per_partition = chunks_per_partition
        self.creation_date = creation_date
        self.digest = digest
        self.size = size
        self.parts = parts
        self.metadata = metadata


class PartHeader(object):
    def __init__(self, blob_id, part, digest, size):
        self.blob_id = blob_id
        self.part = part
        self.digest = digest
        self.size = size


def row_to_object(row):
    if row:
        return ObjectHeader(
            bucket_id=row.bucket_id,
            key=row.key,
            object_id=row.object_id,
            blob_id=row.blob_id,
            version=row.version,
            parts=row.parts,
            metadata=row.metadata,
        )
    else:
        return None


def row_to_version(row):
    if row:
        return VersionHeader(
            object_id=row.object_id,
            bucket_id=row.bucket_id,
            blob_id=row.blob_id,
            version=row.version,
            chunk_size=row.chunk_size,
            chunks_per_partition=row.chunks_per_partition,
            creation_date=row.creation_date,
            digest=row.digest,
            size=row.size,
            parts=row.parts,
            metadata=row.metadata
        )
    else:
        return None


def row_to_part(row):
    if row:
        return PartHeader(
            blob_id=row.blob_id,
            part=row.part,
            digest=row.digest,
            size=row.size,
        )
    else:
        return None


def version_to_metadata(ver):
    return {
        'content_type': ver.metadata['content_type'],
        'creation_date': ver.creation_date.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
        'digest': ver.digest,
        'size': ver.size,
        'chunk_size': ver.chunk_size,
        'chunks_per_partition': ver.chunks_per_partition,
    }
