import logging
import json
import hashlib
from cassandra.cluster import Cluster, _NOT_SET, TokenAwarePolicy, DCAwareRoundRobinPolicy, PlainTextAuthProvider

from .models import *


class ScyllaStore(object):
    keyspace = 's3'

    chunk_size = 1024 * 128  # bytes
    chunks_per_partition = 512

    def __init__(self,
                 hosts=_NOT_SET, port=9042,
                 username='', password='',
                 compaction_strategy=False):

        auth_provider = None
        if username or password:
            auth_provider = PlainTextAuthProvider(username=username, password=password)

        self.cluster = Cluster(contact_points=hosts, port=port, auth_provider=auth_provider,
                               load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()))

        self.session = self.cluster.connect()

        self.ensure_keyspace()
        self.ensure_tables(compaction_strategy)

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

    def ensure_tables(self, compaction_strategy=False):
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
                blob_id UUID,
                version INT,
                parts BOOL,
                metadata TEXT,
                PRIMARY KEY (bucket_id, key)
            );
            ''',
            '''
            CREATE TABLE IF NOT EXISTS version (
                object_id UUID,
                bucket_id UUID,
                version INT,
                blob_id UUID,
                chunk_size INT,
                chunks_per_partition INT,
                creation_date TIMESTAMP,
                digest TEXT,
                size INT,
                parts BOOL,
                metadata TEXT,
                PRIMARY KEY (object_id, version)
            ) WITH CLUSTERING ORDER BY (version DESC);
            ''',
            '''
             CREATE TABLE IF NOT EXISTS part (
                blob_id UUID,
                part INT,
                digest TEXT,
                size INT,
                PRIMARY KEY (blob_id, part)
            ) WITH CLUSTERING ORDER BY (part ASC);
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
                key TEXT,
                upload_id UUID,
                bucket_id UUID,
                blob_id UUID,
                metadata TEXT,
                PRIMARY KEY (key, upload_id)
            );
            '''
        ]:
            if compaction_strategy:
                if 'CREATE TABLE' in cql and 'chunk' in cql:
                    cql[:-1] += '''
                    AND compaction = { 
                        'class': 'ObjectAwareCompactionStrategy', 
                        'object-identifier': 'blob_id' 
                    }
                    '''
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

        return S3Item(bucket, key=item_name, obj=obj)

    # TODO: support reading whole partition with paging
    def read_item(self, output_stream, item, start=None, length=None):
        self.read_parts(output_stream, item, start, length)

    def get_object_header(self, bucket_id, item_name):
        logging.info('get object header [%s/%s]' % (bucket_id, item_name))

        return row_to_object(
            self.session.execute("SELECT * FROM object WHERE bucket_id = %s AND key = %s",
                                 (bucket_id, item_name)).one())

    def get_version_header(self, object_id, version):
        logging.info('get version header [object_id = %s version = %d]' % (object_id, version))

        return row_to_version(
            self.session.execute("SELECT * FROM version WHERE object_id = %s AND version = %s",
                                 (object_id, version)).one())

    def get_part_header(self, object_id, version, part):
        logging.info('get part header [object_id = %s version = %d part = %d]' % (object_id, version, part))

        return row_to_part(
            self.session.execute("SELECT * FROM part WHERE object_id = %s AND version = %s AND part = %s",
                                 (object_id, version, part)).one())

    def store_item(self, bucket, item_name, headers, size, data, optional_digest=None, version=1, object_id=None):
        # TODO: storing multipart became so different from single part that it should be split into two funcs
        logging.info(f'store_item {bucket.name}/{item_name}: {size} bytes')

        # load object header
        obj = self.get_object_header(bucket.bucket_id, item_name)
        if obj is None:
            if object_id is None:
                self.session.execute("INSERT INTO object (bucket_id, key, object_id, version, metadata) "
                                     "VALUES (%s, %s, uuid(), %s, '')",
                                     (bucket.bucket_id, item_name, version))
            else:
                self.session.execute("INSERT INTO object (bucket_id, key, object_id, version, metadata) "
                                     "VALUES (%s, %s, %s, %s, '')",
                                     (bucket.bucket_id, item_name, object_id, version))
            obj = self.get_object_header(bucket.bucket_id, item_name)

        obj.version = version
        logging.info(f'object_header {obj.object_id}#{obj.version}')

        # prepare next object version header
        ver = self.get_version_header(obj.object_id, obj.version)
        version = obj.version
        if data is not None and ver:
            version += 1
            # TODO: ensure version does not exist

        if data is not None or ver is None:
            ver = self.insert_version(obj.bucket_id, obj.object_id, version, headers, size)

        logging.info(f'version_header {ver.object_id}#{ver.version}')

        digest = optional_digest
        if data is not None:
            # write data in one part
            digest = self.write_part(ver, 1, data, size)

        # update metadata+digest
        ver.digest = digest
        ver.size = size

        metadata = {
            'content_type': headers['content-type'],
            'creation_date': ver.creation_date.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
            'digest': digest,
            'size': size,
        }

        self.session.execute(
            "UPDATE version SET digest = %s, metadata = %s, size = %s WHERE object_id = %s AND version = %s",
            (digest, json.dumps(metadata), size, ver.object_id, ver.version))

        self.session.execute("UPDATE object SET version = %s, metadata = %s  WHERE bucket_id = %s AND key = %s",
                             (ver.version, json.dumps(version_to_metadata(ver)), obj.bucket_id, obj.key))

        return S3Item(bucket, item_name, **metadata)

    def insert_version(self, bucket_id, object_id, version, headers, size):
        self.session.execute("INSERT INTO version (object_id, bucket_id, version, "
                             "chunk_size, chunks_per_partition, content_type, creation_date, digest, size, metadata) "
                             "VALUES (%s, %s, %s,"
                             "%s, %s, %s, currentTimestamp(), '', %s, '')",
                             (object_id, bucket_id, version,
                              self.chunk_size, self.chunks_per_partition, headers['content-type'], size))

        return self.get_version_header(object_id, version)

    # Chunk/parts operations
    def write_part(self, version, part, data, size):
        logging.info('write part [version = %d part = %d]' % (version.version, part))
        # TODO: possibly we could eliminate update by making chunks before this insert
        self.session.execute("INSERT INTO part (object_id, version, part, blob_id, size)"
                             "VALUES (%s, %s, %s, uuid(), %s)",
                             (version.object_id, version.version, part, size))

        part_header = self.get_part_header(version.object_id, version.version, part)

        digest = self.write_chunks(part_header.blob_id, data, size, version.chunk_size, version.chunks_per_partition)

        self.session.execute("UPDATE part SET digest = %s WHERE object_id = %s AND version = %s AND part = %s",
                             (digest, version.object_id, version.version, part))

        return digest

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

        # FIXME: should be returned sorted but order by doesn't work
        parts.sort(key=lambda i: i.part)

        logging.info("read all parts [parts = %s version = %d]" % (parts, item.version))

        current_start = 0  # absolute position in object
        for part in parts:
            if length <= 0:
                return
            part_start = 0  # position in part blob
            if start > current_start:
                part_start = start - current_start

            logging.info("evaluating part [%s] part_start=%d" % (part, part_start))

            if part_start >= part.size:
                current_start += part.size
                logging.info("skipping part.part=%d part.size=%d" % (part.part, part.size))
                continue  # skipping this part

            logging.info("sending part.part=%d" % part.part)

            self.read_chunks(output_stream, part.blob_id, part_start, min(part.size, length), item.chunk_size,
                             item.chunks_per_partition)
            current_start += part.size
            length -= part.size
        # TODO: if at this point length > 0 then something went wrong (requested more data than we have)

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

    def create_multipart_upload(self, bucket_name, key, headers):
        logging.info('create multipart upload [key = %s]' % (key,))
        obj = self.get_item(bucket_name, key)
        bucket = None
        if obj is not None:
            bucket = obj.bucket
        if bucket is None:
            # TODO: this call could be removed if we refactor get_item()
            bucket = self.session.execute("SELECT bucket_id FROM bucket WHERE name = %s", (bucket_name,)).one()

        version = 1
        if obj is not None:
            version = obj.version + 1

        if obj is None:
            self.session.execute(
                "INSERT INTO multipart_upload (object_id, key, version, upload_id, bucket_id, metadata)"
                "VALUES (uuid(), %s, %s, uuid(), %s, %s)",
                (key, version, bucket.bucket_id, json.dumps(headers)))
        else:
            self.session.execute(
                "INSERT INTO multipart_upload (object_id, key, version, upload_id, bucket_id, metadata)"
                "VALUES (%s, %s, %s, uuid(), %s, %s)",
                (obj.object_id, key, version, bucket.bucket_id, json.dumps(headers)))

        # TODO: possibly this could be improved
        upload = self.session.execute(
            "SELECT object_id, upload_id FROM multipart_upload WHERE key = %s AND version = %s LIMIT 1 ALLOW FILTERING",
            (key, version)).one()

        # TODO: not sure if according to doc version should be generated on create req
        self.insert_version(bucket.bucket_id, upload.object_id, version, headers, 0)

        logging.info(
            'multipart upload with version [key = %s version = %d upload_id = %s]' % (key, version, upload.upload_id))
        return upload.upload_id

    def complete_multipart_upload(self, bucket_name, key, uploadId):
        logging.info('complete multipart upload [key = %s upload_id = %s]' % (key, uploadId))
        upload = self.session.execute(f"SELECT * FROM multipart_upload WHERE key = %s AND upload_id = {uploadId}",
                                      (key,)).one()

        size, digest = self.gather_parts_headers(upload.object_id, upload.version)

        bucket = self.get_bucket(bucket_name)
        item = self.store_item(bucket, upload.key, json.loads(upload.metadata), size, None,
                               digest, upload.version, upload.object_id)

        self.session.execute(f"DELETE FROM multipart_upload WHERE key = %s AND upload_id = {upload.upload_id}",
                             (upload.key,))
        return item.digest

    def gather_parts_headers(self, object_id, version):
        parts = self.session.execute("SELECT * FROM part WHERE object_id = %s AND version = %s",
                                     (object_id, version)).all()

        # FIXME: should be returned sorted but order by doesn't work
        parts.sort(key=lambda item: item.part)

        size = 0
        m = hashlib.md5()
        for part in parts:
            size += part.size
            m.update(part.digest.encode('utf-8'))

        return size, m.hexdigest()

    def upload_part(self, key, part_number, upload_id, data, size):
        # TODO: some validation on uploadId would be great (or binding differently)
        # TODO: should upload_id be part of partition key?
        upload = self.session.execute(
            f"SELECT object_id, version FROM multipart_upload WHERE key = %s AND upload_id = {upload_id} ALLOW FILTERING",
            (key,)).one()

        ver = self.get_version_header(upload.object_id, upload.version)

        return self.write_part(ver, part_number, data, size)


def remove_prefix(text, prefix):
    return text[text.startswith(prefix) and len(prefix):]
