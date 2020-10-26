#!/usr/bin/env python

import argparse
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

import sys

from .scylla_store import ScyllaStore

import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.responses import PlainTextResponse

logging.basicConfig(level=logging.INFO)

from .vars import *
from .actions import *

app = FastAPI()

@app.get("/{full_path:path}")
async def do_GET(request : Request, response : Response):
    try:
        return await do_GET_wrapped(request, response)
    except ConnectionResetError:
        logging.warning("connection reset")
    except BrokenPipeError:
        logging.warning("broken pipe")

async def do_GET_wrapped(request : Request, response : Response):
    qs = urllib.parse.parse_qs(request.url.query, True)
    host = request.headers['host'].split(':')[0]
    if host == 'localhost':
        host = '127.0.0.1'
    path = request.url.path
    bucket_name = None
    item_name = None
    req_type = None

    if host != vars.mock_hostname and vars.mock_hostname in host:
        idx = host.index(vars.mock_hostname)
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
        return list_buckets(request, response)

    elif req_type == 'ls_bucket':
        return ls_bucket(request, response, bucket_name, qs)

    elif req_type == 'get_acl':
        return get_acl(request, response)

    elif req_type == 'get':
        return await get_item(request, response, bucket_name, item_name)

    else:
        logging.warning(f'{req_type}: [{bucket_name}] {item_name}')
        return PlainTextResponse(f'{req_type}: [{bucket_name}] {item_name}')

# NOT IMPLEMENTED
# def do_DELETE(self):
#     parsed_path = urllib.parse.urlparse(self.path)
#     qs = urllib.parse.parse_qs(parsed_path.query, True)
#     host = self.headers['host'].split(':')[0]
#     path = parsed_path.path
#     bucket_name = None
#     item_name = None
#
#     mock_hostname = self.server.mock_hostname
#     if host != mock_hostname and mock_hostname in host:
#         idx = host.index(mock_hostname)
#         bucket_name = host[:idx - 1]
#
#     if not bucket_name:
#         bucket_name, sep, item_name = path.strip('/').partition('/')
#     else:
#         item_name = path.strip('/')
#
#     if bucket_name and item_name:
#         delete_item(self, bucket_name, item_name)
#     else:
#         self.write('%s: [%s] %s' % ('DELETE', bucket_name, item_name))
#
#     self.send_response(204)
#     self.send_header('Content-Length', '0')
#     self.end_headers()

@app.head("/{full_path:path}")
async def do_HEAD(request : Request, response : Response):
    return await do_GET(request, response)

@app.post("/{full_path:path}")
async def do_POST(request : Request, response : Response):
    qs = urllib.parse.parse_qs(request.url.query, True)
    host = request.headers['host'].split(':')[0]
    if host == 'localhost':
        host = '127.0.0.1'
    path = request.url.path

    bucket_name = None
    item_name = None
    req_type = None

    if host != vars.mock_hostname and vars.mock_hostname in host:
        idx = host.index(vars.mock_hostname)
        bucket_name = host[:idx - 1]

    if path == '/' and bucket_name and 'delete' in qs:
        req_type = 'delete_keys'
    else:
        if not bucket_name:
            bucket_name, sep, item_name = path.strip('/').partition('/')
        else:
            item_name = path.strip('/')

        headers = request.headers.mutablecopy()
        if 'content-type' not in headers:
            headers['content-type'] = 'application/octet-stream'

        if 'uploads' in qs:
            req_type = 'CreateMultipartUpload'
        elif 'uploadId' in qs:
            req_type = 'CompleteMultipartUpload'

        if not item_name and 'delete' in qs:
            req_type = 'delete_keys'

    if req_type == 'delete_keys':
        size = int(request.headers['content-length'])
        data = request.stream()
        root = ET.fromstring(data)
        keys = []
        for obj in root.findall('Object'):
            keys.append(obj.find('Key').text)
        return delete_items(request, response, bucket_name, keys)
    elif req_type == 'CreateMultipartUpload':
        return create_multipart_upload(request, response, bucket_name, item_name, tuplesToDict(headers.items()))
    elif req_type == 'CompleteMultipartUpload':
        return await complete_multipart_upload(request, response, bucket_name, item_name, qs['uploadId'][0])
    else:
        logging.warning('%s: [%s] %s' % (req_type, bucket_name, item_name))
        return PlainTextResponse('%s: [%s] %s' % (req_type, bucket_name, item_name))

@app.put("/{full_path:path}")
async def do_PUT(request : Request, response : Response):
    try:
        return await do_PUT_wrapped(request, response)
    except ConnectionResetError:
        logging.warning("connection reset")
    except BrokenPipeError:
        logging.warning("broken pipe")

async def do_PUT_wrapped(request : Request, response : Response):
    qs = urllib.parse.parse_qs(request.url.query, True)
    host = request.headers['host'].split(':')[0]
    if host == 'localhost':
        host = '127.0.0.1'
    path = request.url.path

    bucket_name = None
    item_name = None
    req_type = None

    if host != vars.mock_hostname and vars.mock_hostname in host:
        idx = host.index(vars.mock_hostname)
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

    if 'x-amz-copy-source' in request.headers:
        copy_source = request.headers['x-amz-copy-source']
        src_bucket, sep, src_key = copy_source.partition('/')
        req_type = 'copy'

    if req_type == 'create_bucket':
        if vars.store.create_bucket(bucket_name):
            return Response('', status_code=200)
        else:
            # TODO: I am not sure about this error code:
            #  $ aws --debug s3api create-bucket --bucket ivan --region
            #    eu-north-1 --create-bucket-configuration LocationConstraint=eu-north-1
            return Response('', status_code=400)

    elif req_type == 'store':
        bucket = vars.store.get_bucket(bucket_name)
        if bucket:
            headers = request.headers.mutablecopy()
            if 'content-type' not in headers:
                headers['content-type'] = 'application/octet-stream'

            size = int(headers['content-length'])

            item = await vars.store.store_item(bucket, item_name, tuplesToDict(headers.items()), size, request.stream())
            return Response('', status_code=200, headers={'Etag': '"%s"' % item.md5})
        else:
            return Response('', status_code=404)

    elif req_type == 'upload_part':
        size = int(request.headers['Content-Length'])
        if size == 0:
            size = int(request.headers['content-length'])

        return await upload_part(request, response, item_name, qs['partNumber'][0], qs['uploadId'][0], size, request.stream())

    elif req_type == 'copy':
        logging.warning("copy not implemented")
        #store.copy_item(src_bucket, src_key, bucket_name, item_name, self)
        # TODO: should be some xml here
        return Response('', status_code=405)


def tuplesToDict(l):
    d = {}
    for a, b in l:
        d.setdefault(a, b)
    return d


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    args = parse(argv)

    logging.root.setLevel(level=os.environ.get('LOGLEVEL', 'INFO'))

    vars.mock_hostname = args.hostname

    logging.info('Connect to scylla storage %s:%d' % (args.scylla_hosts, args.scylla_port))
    vars.store = ScyllaStore(hosts=args.scylla_hosts, port=args.scylla_port)
    if args.chunk_size > 0:
        vars.store.chunk_size = args.chunk_size
    if args.chunks_per_partition > 0:
        vars.store.chunks_per_partition = args.chunks_per_partition

    logging.info('Starting server at %s:%d, use <Ctrl-C> to stop' % (args.hostname, args.port))

    uvicorn.run(app, host=args.hostname, port=args.port, log_level="info")


def parse(argv=None):
    parser = argparse.ArgumentParser(description='A Mock-S3 server.')
    parser.add_argument('--hostname', dest='hostname', action='store',
                        default='127.0.0.1',
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
                        default=0, type=int,
                        help='chunk size in bytes')
    parser.add_argument('--chunks_per_partition', dest='chunks_per_partition', action='store',
                        default=0, type=int,
                        help='number of chunks per partition')

    args = parser.parse_args(argv)

    if args.scylla_hosts:
        args.scylla_hosts = args.scylla_hosts.split(",")

    return args


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
