from datetime import datetime
import logging
import hashlib
from cassandra.cluster import Cluster, _NOT_SET, TokenAwarePolicy, DCAwareRoundRobinPolicy

from .models import Bucket, BucketQuery, S3Item


class ScyllaStore(object):
    keyspace = 's3'

    def __init__(self, hosts=_NOT_SET, port=9042):
        self.cluster = Cluster(contact_points=hosts, port=port,
                               load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()))

        self.session = self.cluster.connect()

        self.ensure_keyspace()
        self.ensure_tables()

        self.select_bucket_stmt = self.session.prepare("SELECT bucket_id, creation_date FROM s3.bucket WHERE name = ?")

    def ensure_keyspace(self):
        self.session.execute('''
            CREATE KEYSPACE IF NOT EXISTS s3 WITH replication = { 
                'class': 'NetworkTopologyStrategy',
                'replication_factor': '3' 
            } AND durable_writes = TRUE;
            ''')
        self.session.set_keyspace(self.keyspace)

    def ensure_tables(self):
        for cql in [
            # UUID is just an option. Better suggestions?
            # metadata: text stores metadata in JSON format
            '''
            CREATE TABLE IF NOT EXISTS bucket (
                name text,
                bucket_id UUID,
                creation_date DATE,
                PRIMARY KEY (name)
            );
            ''',
            # I suppose buckets may contain 1000s of objects.
            # It would be cool to have them sorted by name.
            # Having (bucket_id) as a partition key may make
            # the partition heavy. I also don’t know what is
            # the limit on the number of objects in a bucket.
            '''
            CREATE TABLE IF NOT EXISTS object (
                bucket_id UUID,
                name text,
                object_id UUID,
                version int,
                metadata text,
                PRIMARY KEY (bucket_id, name)
            );
            ''',
            '''
            CREATE TABLE IF NOT EXISTS version (
                object_id UUID,
                bucket_id UUID,
                version int,
                blob_id UUID,
                metadata text,
                PRIMARY KEY (object_id, version)
            );
            ''',
            '''
            CREATE TABLE IF NOT EXISTS chunk (
                blob_id UUID,
                partition int,
                ix int,
                data blob,
                PRIMARY KEY ((blob_id, partition), ix)
            ) WITH CLUSTERING ORDER BY (ix ASC);
            '''
        ]:
            self.session.execute(cql)

    # Bucket operations

    def get_bucket(self, bucket_name):
        bucket_info = self.session.execute(self.select_bucket_stmt, [bucket_name]).one()

        if bucket_info:
            bucket_id, creation_date = bucket_info
            return Bucket(bucket_name, bucket_id, creation_date)
        else:
            return None

    def list_all_buckets(self):
        buckets = []

        rows = self.session.execute('SELECT * FROM bucket')

        for row in rows:
            buckets.append(Bucket(name=row.name, bucket_id=row.bucket_id, creation_date=row.creation_date))

        return buckets

    # aws s3 --endpoint http://localhost:8000 mb s3://ivan
    # aws --endpoint http://localhost:8000 --debug s3api create-bucket --bucket xemul --region eu-north-1
    def create_bucket(self, bucket_name):
        if self.get_bucket(bucket_name):
            return None

        logging.debug('Creating bucket [%s]' % bucket_name)

        self.session.execute("INSERT INTO bucket(name, bucket_id, creation_date) VALUES (%s, uuid(), currentDate())",
                             (bucket_name, ))

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
