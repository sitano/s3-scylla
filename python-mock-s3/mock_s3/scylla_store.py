from datetime import datetime
import hashlib
from cassandra.cluster import Cluster

from .models import Bucket, BucketQuery, S3Item

class ScyllaStore(object):

    def __init__(self):
        self.cluster = Cluster()
        self.session = self.cluster.connect()

    # Bucket operations

    def get_bucket(self, bucket_name):
        if bucket_name == 'test':
            return Bucket('test', datetime.now())
        return None

    def list_all_buckets(self):
        # Mock data
        return [Bucket('test', datetime.now())]

    def create_bucket(self, bucket_name):
        print('Stub creating bucket... ' + bucket_name)

        # TODO: Check if bucket already exists
        self.session.execute('''
            INSERT INTO s3.bucket(name, bucket_id, metadata)
            VALUES (%s, uuid(), %s)
        ''', (bucket_name, ''))

        return self.get_bucket(bucket_name)

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

    # Item operations

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

    def store_item(self, bucket, item_name, headers, data):
        print(f'Stub store_item {bucket.name}/{item_name}... of contents: {data}')

        m = hashlib.md5()
        m.update(data)

        metadata = {
            'content_type': headers['content-type'],
            'creation_date': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.000Z'),
            'md5': m.hexdigest(),
            'size': len(data)
        }

        return S3Item(f'{bucket.name}/{item_name}', **metadata)

