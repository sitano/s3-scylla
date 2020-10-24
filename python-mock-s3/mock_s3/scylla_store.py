from datetime import datetime
import logging
import json
import hashlib
from cassandra.cluster import Cluster, _NOT_SET, TokenAwarePolicy, DCAwareRoundRobinPolicy

from .models import *


class ScyllaStore(object):
    keyspace = 's3'

    chunk_size = 1024 * 128  # bytes
    chunks_per_partition = 512

    def __init__(self, hosts=_NOT_SET, port=9042):
        self.cluster = Cluster(contact_points=hosts, port=port,
                               load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()))

        self.session = self.cluster.connect()

        self.ensure_keyspace()
        self.ensure_tables()

        self.create_bucket_stmt = self.session.prepare("INSERT INTO bucket "
                                                       "(name, bucket_id, creation_date, metadata) VALUES "
                                                       "(?, uuid(), currentTimestamp(), NULL)")
        self.list_all_buckets_stmt = self.session.prepare("SELECT * FROM bucket")
        self.list_all_keys_stmt = self.session.prepare("SELECT * FROM object WHERE bucket_id = ? LIMIT ?")
        self.list_prefix_keys_stmt = self.session.prepare("SELECT * FROM object WHERE bucket_id = ? "
                                                          "AND key LIKE ? LIMIT ?")
        self.select_bucket_stmt = self.session.prepare("SELECT bucket_id, creation_date FROM bucket WHERE name = ?")
        self.insert_chunk_stmt = self.session.prepare("INSERT INTO chunk (blob_id, partition, ix, data) VALUES "
                                                      "(?, ?, ?, ?)")
        self.select_chunk_stmt = self.session.prepare(
            "SELECT data FROM chunk WHERE blob_id = ? AND partition = ? AND ix = ?")

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
                name TEXT,
                bucket_id UUID,
                creation_date TIMESTAMP,
                metadata TEXT,
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
                key TEXT,
                object_id UUID,
                version INT,
                metadata TEXT,
                PRIMARY KEY (bucket_id, key)
            );
            ''',
            '''
            CREATE TABLE IF NOT EXISTS version (
                object_id UUID,
                bucket_id UUID,
                version INT,
                chunk_size INT,
                chunks_per_partition INT,
                content_type TEXT,
                creation_date TIMESTAMP,
                md5 TEXT,
                size INT,
                metadata TEXT,
                PRIMARY KEY (object_id, version)
            ) WITH CLUSTERING ORDER BY (version DESC);
            ''',
            '''
             CREATE TABLE IF NOT EXISTS part (
                object_id UUID,
                version INT,
                part INT,
                blob_id UUID,
                md5 TEXT,
                size INT,
                PRIMARY KEY (object_id, version, part)
            ) WITH CLUSTERING ORDER BY (version DESC, part ASC);
            ''',
            '''
            CREATE TABLE IF NOT EXISTS chunk (
                blob_id UUID,
                partition INT,
                ix INT,
                data BLOB,
                PRIMARY KEY ((blob_id, partition), ix)
            ) WITH CLUSTERING ORDER BY (ix ASC);
            ''',
            '''
            CREATE TABLE IF NOT EXISTS multipart_upload (
                upload_id UUID,
                object_id UUID,
                PRIMARY KEY (object_id, upload_id)
            );
            '''
        ]:
            self.session.execute(cql)

    # Bucket operations

    def get_bucket(self, bucket_name):
        bucket_info = self.session.execute(self.select_bucket_stmt, [bucket_name]).one()

        if bucket_info:
            bucket_id, creation_date = bucket_info
            return Bucket(name=bucket_name, bucket_id=bucket_id, creation_date=creation_date)
        else:
            return None

    # aws s3 --endpoint http://localhost:8000 ls
    # aws --endpoint http://localhost:8000 --debug s3api list-buckets
    def list_all_buckets(self):
        buckets = []

        rows = self.session.execute(self.list_all_buckets_stmt)

        for row in rows:
            buckets.append(Bucket(name=row.name, bucket_id=row.bucket_id, creation_date=row.creation_date))

        return buckets

    # aws s3 --endpoint http://localhost:8000 mb s3://ivan
    # aws --endpoint http://localhost:8000 --debug s3api create-bucket --bucket xemul --region eu-north-1
    def create_bucket(self, bucket_name):
        if self.get_bucket(bucket_name):
            return None

        logging.info('Creating bucket [%s]' % bucket_name)

        self.session.execute(self.create_bucket_stmt, [bucket_name])

        return self.get_bucket(bucket_name)

    # aws s3 --endpoint http://localhost:8000 ls s3://ivan
    # aws --endpoint http://localhost:8000 --debug s3api list-objects --bucket ivan
    def get_all_keys(self, bucket, marker='', prefix='', max_keys=1000, delimiter=''):
        matches = []
        prefixes = []

        if prefix:
            rows = self.session.execute(self.list_prefix_keys_stmt, [bucket.bucket_id, f'{prefix}%', max_keys])
        else:
            rows = self.session.execute(self.list_all_keys_stmt, [bucket.bucket_id, max_keys])

        # TODO: marker
        for row in rows:
            obj = row_to_object(row)

            # if delimiter is set and prefix has form {path}{delimiter} like path/.
            if delimiter and (not prefix or prefix[-1] == delimiter):
                # then filter sub paths
                if delimiter in remove_prefix(obj.key, prefix):
                    sub_prefix = remove_prefix(obj.key, prefix).split(delimiter)[0]
                    if sub_prefix + delimiter not in prefixes:
                        prefixes.append(sub_prefix + delimiter)
                    continue

            metadata = self.get_item_metadata(obj)
            if obj is None:
                logging.info("missing version")
                return None

            matches.append(S3Item(bucket, obj.key, **metadata))

        return BucketQuery(bucket, matches, prefixes, False, marker, prefix, max_keys, delimiter)

    # Item operations

    def get_item(self, bucket_name, item_name):
        bucket = self.get_bucket(bucket_name)
        if bucket is None:
            logging.info("missing bucket")
            return None

        obj = self.get_object_header(bucket.bucket_id, item_name)
        if obj is None:
            logging.info("missing object")
            return None

        metadata = self.get_item_metadata(obj)
        if obj is None:
            logging.info("missing version")
            return None

        item = S3Item(bucket, key=item_name, **metadata)
        # item.chunks_per_part = obj.chunks_per_part
        # item.
        return item

    def get_item_metadata(self, obj):
        metadata = object()
        if obj.metadata is None or obj.metadata == '':
            ver = self.get_version_header(obj.object_id, obj.version)
            if ver is None:
                return None
            metadata = version_to_metadata(ver)
        else:
            metadata = json.loads(obj.metadata)
        metadata['version'] = obj.version
        metadata['object_id'] = obj.object_id
        return metadata

    def read_item(self, output_stream, item, start=None, length=None):
        self.read_parts(output_stream, item, start, length)

    def get_object_header(self, bucket_id, item_name):
        logging.info('get object header [%s/%s]' % (bucket_id, item_name))

        return row_to_object(
            self.session.execute("SELECT * FROM object WHERE bucket_id = %s AND key = %s",
                                 (bucket_id, item_name)).one())

    def get_version_header(self, object_id, version):
        logging.info('get version header [%s/%d]' % (object_id, version))

        return row_to_version(
            self.session.execute("SELECT * FROM version WHERE object_id = %s AND version = %s",
                                 (object_id, version)).one())

    def get_part_header(self, object_id, version, part):
        logging.info('get part header [%s/%d/%d]' % (object_id, version, part))

        return row_to_part(
            self.session.execute("SELECT * FROM part WHERE object_id = %s AND version = %s AND part = %s",
                                 (object_id, version, part)).one())

    def store_item(self, bucket, item_name, headers, size, data):
        logging.info(f'store_item {bucket.name}/{item_name}: {size} bytes')

        # load object header
        obj = self.get_object_header(bucket.bucket_id, item_name)
        if obj is None:
            self.session.execute("INSERT INTO object (bucket_id, key, object_id, version, metadata) "
                                 "VALUES (%s, %s, uuid(), 1, '')",
                                 (bucket.bucket_id, item_name))
            obj = self.get_object_header(bucket.bucket_id, item_name)
        logging.info(f'object_header {obj.object_id}#{obj.version}')

        # prepare next object version header
        ver = self.get_version_header(obj.object_id, obj.version)
        version = 1
        if ver:
            version = ver.version + 1
            # TODO: ensure version does not exist

        content_type = headers['content-type']
        self.session.execute("INSERT INTO version (object_id, bucket_id, version, "
                             "chunk_size, chunks_per_partition, content_type, creation_date, md5, size, metadata) "
                             "VALUES (%s, %s, %s,"
                             "%s, %s, %s, currentTimestamp(), '', %s, '')",
                             (obj.object_id, obj.bucket_id, version,
                              self.chunk_size, self.chunks_per_partition, content_type, size))

        ver = self.get_version_header(obj.object_id, version)
        logging.info(f'version_header {ver.object_id}#{ver.version}')

        # write data in one part
        digest = self.write_part(ver, 1, data, size)

        # update metadata+md5
        ver.md5 = digest

        metadata = {
            'content_type': content_type,
            'creation_date': ver.creation_date.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
            'md5': digest,
            'size': size,
        }

        self.session.execute("UPDATE version SET md5 = %s, metadata = %s WHERE object_id = %s AND version = %s",
                             (digest, json.dumps(metadata), ver.object_id, ver.version))

        self.session.execute("UPDATE object SET version = %s, metadata = %s WHERE bucket_id = %s AND key = %s",
                             (ver.version, json.dumps(version_to_metadata(ver)), obj.bucket_id, obj.key))

        return S3Item(bucket, item_name, **metadata)

    # Chunk/parts operations

    def write_part(self, version, part, data, size):
        m = hashlib.md5()
        self.session.execute("INSERT INTO part (object_id, version, part, blob_id, size)"
                             "VALUES (%s, %s, %s, uuid(), %s)",
                             (version.object_id, version.version, part, size))

        part_header = self.get_part_header(version.object_id, version.version, part)

        digest = self.write_chunks(part_header.blob_id, data, size, version.chunk_size, version.chunks_per_partition)

        self.session.execute("UPDATE part SET md5 = %s WHERE object_id = %s AND version = %s AND part = %s",
                             (digest, version.object_id, version.version, part))

    # aws s3api --endpoint http://localhost:8000 create-bucket --bucket my-bucket
    # dd if=/dev/zero bs=1 count=212 of=file
    # aws s3 --endpoint http://localhost:8000 cp file s3://my-bucket/file
    def write_chunks(self, blob_id, stream, size, chunk_size, partition_chunks):
        m = hashlib.md5()

        # Round it up - the last chunk might be smaller
        # than chunk_size. 
        chunk_count = (size + chunk_size - 1) // chunk_size
        last_chunk_number = chunk_count - 1

        # TODO - double check if this calculation is correct (what if there is a single chunk? - not tested)
        last_chunk_size = size - (chunk_count - 1) * chunk_size

        for chunk_number in range(chunk_count):
            partition = chunk_number // partition_chunks
            ix = chunk_number % partition_chunks

            bytes_to_read = chunk_size
            if chunk_number == last_chunk_number:
                bytes_to_read = last_chunk_size

            data = stream.read(bytes_to_read)
            m.update(data)
            self.session.execute(self.insert_chunk_stmt, [blob_id, partition, ix, data])

        return m.hexdigest()

    def read_parts(self, output_stream, item, start, length):
        parts = self.session.execute("SELECT * FROM part WHERE object_id = %s AND version = %s",
                             (item.object_id, item.version)).all()
        #FIXME: should be returned sorted but order by doesn't work
        def partNum(item):
            return item.part
        parts.sort(key=partNum)

        logging.info("parts [%s]" % parts)

        current_start = 0 # absolute position in object
        for part in parts:
            if length <= 0:
                return
            part_start = 0 # position in part blob
            if start > current_start:
                part_start = start - current_start

            logging.info("evaluating part [%s] part_start=%d" % (part, part_start))

            if part_start >= part.size:
                current_start += part.size
                logging.info("skipping part.part=%d part.size=%d" % (part.part, part.size))
                continue # skipping this part

            logging.info("sending part.part=%d" % part.part)

            self.read_chunks(output_stream, part.blob_id, part_start, min(part.size, length), item.chunk_size, item.chunks_per_partition)
            current_start += part.size
            length -= part.size
        #TODO: if at this point length > 0 then something went wrong (requested more data than we have)

    def read_chunks(self, output_stream, blob_id, start_byte, length, chunk_size, partition_chunks):
        end_byte = start_byte + length - 1

        start_chunk = start_byte // chunk_size
        end_chunk = end_byte // chunk_size

        # start_byte or end_byte can be non-multiples of chunk_size.
        # In such a case, we have to skip some bytes at the start
        # of the start chunk and at the end of the end chunk.

        # TODO - double check this calculations (for off-by-one errors etc)!
        start_chunk_offset = start_byte - start_chunk * chunk_size
        end_chunk_length = end_byte - (end_chunk - 1) * chunk_size + 1

        # TODO - do not read chunk by chunk, instead partition by partition!
        for chunk_number in range(start_chunk, end_chunk + 1):
            partition = chunk_number // partition_chunks
            ix = chunk_number % partition_chunks

            # TODO - handle errors!
            data = self.session.execute(self.select_chunk_stmt, [blob_id, partition, ix]).one().data

            if chunk_number == start_chunk:
                data = data[start_chunk_offset:]

            if chunk_number == end_chunk:
                data = data[:end_chunk_length]

            output_stream.write(data)


def row_to_object(row):
    if row:
        return ObjectHeader(
            bucket_id=row.bucket_id,
            key=row.key,
            object_id=row.object_id,
            version=row.version,
            metadata=row.metadata,
        )
    else:
        return None


def row_to_version(row):
    if row:
        return VersionHeader(
            object_id=row.object_id,
            bucket_id=row.bucket_id,
            version=row.version,
            chunk_size=row.chunk_size,
            chunks_per_partition=row.chunks_per_partition,
            content_type=row.content_type,
            creation_date=row.creation_date,
            md5=row.md5,
            size=row.size,
            metadata=row.metadata
        )
    else:
        return None

def row_to_part(row):
    if row:
        return PartHeader(
            object_id=row.object_id,
            version=row.version,
            part=row.part,
            blob_id=row.blob_id,
            md5=row.md5,
            size=row.size,
        )
    else:
        return None

def version_to_metadata(ver):
    return {
        'content_type': ver.content_type,
        'creation_date': ver.creation_date.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
        'md5': ver.md5,
        'size': ver.size,
        'chunk_size': ver.chunk_size,
        'chunks_per_partition': ver.chunks_per_partition,
    }


def remove_prefix(text, prefix):
    return text[text.startswith(prefix) and len(prefix):]
