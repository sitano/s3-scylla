from datetime import datetime
from cassandra.cluster import Cluster

from .models import Bucket, BucketQuery, S3Item

class ScyllaStore(object):

    def __init__(self):
        self.cluster = Cluster()
        self.session = self.cluster.connect()

    def get_bucket(self, bucket_name):
        if bucket_name == 'test':
            return Bucket('test', datetime.now())
        return None

    def get_all_keys(self, bucket, **kwargs):
        # Mock data
        if bucket.name == 'test':
            matches = []
            metadata = {}
            metadata['size'] = 15
            metadata['md5'] = 'testing'
            metadata['content_type'] = 'testing'
            metadata['creation_date'] = datetime.now()

            example_query = self.session.execute('SELECT cql_version FROM system.local').one()
            example_s3_item = 'scylla_cql_' + str(example_query[0])

            matches.append(S3Item(example_s3_item, **metadata))
            return BucketQuery(bucket, matches, False, **kwargs)

        return None

    def get_item(self, bucket_name, item_name):
        # Mock data
        metadata = {}
        metadata['size'] = 15
        metadata['md5'] = 'testing'
        metadata['filename'] = item_name
        metadata['content_type'] = 'testing'
        metadata['creation_date'] = '2020-01-01T11:12:13.000Z'

        item = S3Item(bucket_name + '/' + item_name, **metadata)

        return item

    def get_fragment(self, item, start=None, length=None):
        if start is None and length is None:
            return 'mock whole file'

        return 'mock some fragment'

    def list_all_buckets(self):
        # Mock data
        return [Bucket('test', datetime.now())]

