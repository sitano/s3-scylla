#!/usr/bin/env python

import argparse
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn

import sys

from .actions import *
from .scylla_store import ScyllaStore

logging.basicConfig(level=logging.INFO)

class S3Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            self.do_GET_wrapped()
        except ConnectionResetError:
            logging.warning("connection reset")
        except BrokenPipeError:
            logging.warning("broken pipe")

    def do_GET_wrapped(self):
        parsed_path = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(parsed_path.query, True)
        host = self.headers['host'].split(':')[0]
        path = parsed_path.path
        bucket_name = None
        item_name = None
        req_type = None

        mock_hostname = self.server.mock_hostname
        if host != mock_hostname and mock_hostname in host:
            idx = host.index(mock_hostname)
            bucket_name = host[:idx - 1]

        if path == '/' and not bucket_name:
            req_type = 'list_buckets'

        else:
            if not bucket_name:
                bucket_name, sep, item_name = path.strip('/').partition('/')
            else:
                item_name = path.strip('/')

            if not bucket_name:
                req_type = 'list_buckets'
            elif not item_name:
                req_type = 'ls_bucket'
            else:
                if 'acl' in qs and qs['acl'] == '':
                    req_type = 'get_acl'
                else:
                    req_type = 'get'

        logging.debug("request type: %s" % req_type)

        if req_type == 'list_buckets':
            list_buckets(self)

        elif req_type == 'ls_bucket':
            ls_bucket(self, bucket_name, qs)

        elif req_type == 'get_acl':
            get_acl(self)

        elif req_type == 'get':
            get_item(self, bucket_name, item_name)

        else:
            self.write(f'{req_type}: [{bucket_name}] {item_name}')

    def do_DELETE(self):
        parsed_path = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(parsed_path.query, True)
        host = self.headers['host'].split(':')[0]
        path = parsed_path.path
        bucket_name = None
        item_name = None

        mock_hostname = self.server.mock_hostname
        if host != mock_hostname and mock_hostname in host:
            idx = host.index(mock_hostname)
            bucket_name = host[:idx - 1]

        if not bucket_name:
            bucket_name, sep, item_name = path.strip('/').partition('/')
        else:
            item_name = path.strip('/')

        if bucket_name and item_name:
            delete_item(self, bucket_name, item_name)
        else:
            self.write('%s: [%s] %s' % ('DELETE', bucket_name, item_name))

        self.send_response(204)
        self.send_header('Content-Length', '0')
        self.end_headers()

    def do_HEAD(self):
        return self.do_GET()

    def do_POST(self):
        parsed_path = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(parsed_path.query, True)
        host = self.headers['host'].split(':')[0]
        path = parsed_path.path
        bucket_name = None
        item_name = None
        req_type = None

        mock_hostname = self.server.mock_hostname
        if host != mock_hostname and mock_hostname in host:
            idx = host.index(mock_hostname)
            bucket_name = host[:idx - 1]

        if path == '/' and bucket_name and 'delete' in qs:
            req_type = 'delete_keys'
        else:
            if not bucket_name:
                bucket_name, sep, item_name = path.strip('/').partition('/')
            else:
                item_name = path.strip('/')

            if 'uploads' in qs:
                req_type = 'CreateMultipartUpload'
            elif 'uploadId' in qs:
                req_type = 'CompleteMultipartUpload'

            if not item_name and 'delete' in qs:
                req_type = 'delete_keys'

        if req_type == 'delete_keys':
            size = int(self.headers['Content-Length'])
            data = self.rfile.read(size)
            root = ET.fromstring(data)
            keys = []
            for obj in root.findall('Object'):
                keys.append(obj.find('Key').text)
            delete_items(self, bucket_name, keys)
        elif req_type == 'CreateMultipartUpload':
            create_multipart_upload(self, bucket_name, item_name, self.headers)
        elif req_type == 'CompleteMultipartUpload':
            complete_multipart_upload(self, bucket_name, item_name, qs['uploadId'][0])
        else:
            self.write('%s: [%s] %s' % (req_type, bucket_name, item_name))

    def do_PUT(self):
        try:
            self.do_PUT_wrapped()
        except ConnectionResetError:
            logging.warning("connection reset")
        except BrokenPipeError:
            logging.warning("broken pipe")

    def do_PUT_wrapped(self):
        parsed_path = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(parsed_path.query, True)
        host = self.headers['host'].split(':')[0]
        path = parsed_path.path
        bucket_name = None
        item_name = None
        req_type = None

        mock_hostname = self.server.mock_hostname
        if host != mock_hostname and mock_hostname in host:
            idx = host.index(mock_hostname)
            bucket_name = host[:idx - 1]

        if path == '/' and bucket_name:
            req_type = 'create_bucket'
        elif 'partNumber' in qs and 'uploadId' in qs:
            item_name = os.path.basename(path.strip('/'))
            req_type = 'upload_part'
        else:
            if not bucket_name:
                bucket_name, sep, item_name = path.strip('/').partition('/')
            else:
                item_name = path.strip('/')

            if not item_name:
                req_type = 'create_bucket'
            else:
                if 'acl' in qs and qs['acl'] == '':
                    req_type = 'set_acl'
                else:
                    req_type = 'store'

        if 'x-amz-copy-source' in self.headers:
            copy_source = self.headers['x-amz-copy-source']
            src_bucket, sep, src_key = copy_source.partition('/')
            req_type = 'copy'

        if req_type == 'create_bucket':
            if self.server.store.create_bucket(bucket_name):
                self.send_response(200)
            else:
                # TODO: I am not sure about this error code:
                #  $ aws --debug s3api create-bucket --bucket ivan --region
                #    eu-north-1 --create-bucket-configuration LocationConstraint=eu-north-1
                self.send_response(400)

        elif req_type == 'store':
            size = int(self.headers['Content-Length'])
            if size < 1:
                self.send_response(400, '')
            else:
                bucket = self.server.store.get_bucket(bucket_name)
                if bucket:
                    item = self.server.store.store_item(bucket, item_name, self.headers, size, self.rfile)
                    self.send_response(200)
                    self.send_header('Etag', '"%s"' % item.digest)
                else:
                    self.send_response(404, '')

        elif req_type == 'upload_part':
            size = int(self.headers['Content-Length'])
            if size == 0:
                size = int(self.headers['content-length'])

            upload_part(self, item_name, qs['partNumber'][0], qs['uploadId'][0], size, self.rfile)

        elif req_type == 'copy':
            self.server.store.copy_item(src_bucket, src_key, bucket_name, item_name, self)
            # TODO: should be some xml here
            self.send_response(200)

        self.send_header('Content-Type', 'text/xml')
        self.end_headers()

    def log_request(self, code='-', size='-'):
        """Log an accepted request.

        This is called by send_response().

        """
        if isinstance(code, HTTPStatus):
            code = code.value
        self.log_message('"%s" %s %s\n\n%s', self.requestline, str(code), str(size),
                         self.headers)

    def send_header(self, keyword, value):
        """Logs response header to DEBUG"""
        logging.debug("%s: %s" % (keyword, value))
        super().send_header(keyword, value)

    def write(self, res) -> int:
        """Logs response body to DEBUG"""
        logging.debug("Write:\n%s" % res)
        return self.wfile.write(res.encode())


class S3HTTPServer(ThreadingMixIn, HTTPServer):
    store = None
    mock_hostname = ''

    def set_store(self, store):
        self.store = store

    def set_mock_hostname(self, mock_hostname):
        self.mock_hostname = mock_hostname


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    args = parse(argv)

    logging.root.setLevel(level=os.environ.get('LOGLEVEL', 'INFO'))

    server = S3HTTPServer((args.hostname, args.port), S3Handler)
    server.set_mock_hostname(args.hostname)

    logging.info('Connect to scylla storage %s:%d' % (args.scylla_hosts, args.scylla_port))
    server.set_store(ScyllaStore(hosts=args.scylla_hosts, port=args.scylla_port,
                                 username=args.username, password=args.password,
                                 compaction_strategy=args.compaction_strategy))
    server.store.chunk_size = args.chunk_size
    server.store.chunks_per_partition = args.chunks_per_partition

    logging.info('Starting server at %s:%d, use <Ctrl-C> to stop' % (args.hostname, args.port))

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass

    server.server_close()


def parse(argv=None):
    parser = argparse.ArgumentParser(description='A Mock-S3 server.')
    parser.add_argument('--hostname', dest='hostname', action='store',
                        default='localhost',
                        help='Hostname to listen on.')
    parser.add_argument('--port', dest='port', action='store',
                        default=8000, type=int,
                        help='Port to run server on.')
    parser.add_argument('--root', dest='root', action='store',
                        default='%s/s3store' % os.environ['HOME'],
                        help='Defaults to $HOME/s3store.')
    parser.add_argument('--scylla.hosts', dest='scylla_hosts', action='store',
                        default="127.0.0.1", type=str,
                        help='Scylla hosts 1,2,3')
    parser.add_argument('--scylla.port', dest='scylla_port', action='store',
                        default=9042, type=int,
                        help='Scylla port')
    parser.add_argument('--chunk_size', dest='chunk_size', action='store',
                        default=1024*128, type=int,
                        help='chunk size in bytes')
    parser.add_argument('--chunks_per_partition', dest='chunks_per_partition', action='store',
                        default=512, type=int,
                        help='number of chunks per partition')
    parser.add_argument('--compaction_strategy', dest='compaction_strategy', action='store',
                        default=False, type=bool,
                        help='use object-aware compaction strategy')
    parser.add_argument('--username', dest='username', action='store',
                        default='', type=str,
                        help='password authentication username')
    parser.add_argument('--password', dest='password', action='store',
                        default='', type=str,
                        help='password authentication password')

    args = parser.parse_args(argv)

    if args.scylla_hosts:
        args.scylla_hosts = args.scylla_hosts.split(",")

    return args


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
