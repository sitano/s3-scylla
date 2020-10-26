import datetime
import time
import json
from . import xml_templates

from fastapi import Request, Response
from fastapi.responses import StreamingResponse

from .vars import *

def list_buckets(request : Request, response : Response):
    buckets = vars.store.list_all_buckets()
    xml = ''
    for bucket in buckets:
        xml += xml_templates.buckets_bucket_xml.format(bucket=bucket)
    xml = xml_templates.buckets_xml.format(buckets=xml)
    return Response(xml, status_code=200, headers={'Content-Type': 'application/xml'})


def ls_bucket(request : Request, response : Response, bucket_name, qs):
    bucket = vars.store.get_bucket(bucket_name)
    if bucket:
        bucket_query = vars.store.get_all_keys(bucket,
                                                         marker=qs.get('marker', [''])[0],
                                                         prefix=qs.get('prefix', [''])[0],
                                                         max_keys=int(qs.get('max-keys', [1000])[0]),
                                                         delimiter=qs.get('delimiter', [''])[0])
        contents = ''
        prefixes = ''
        for item in bucket_query.matches:
            if item.content_type == 'application/x-directory':
                item.key += '/'
            contents += xml_templates.bucket_query_content_xml.format(s3_item=item) + "\n"
        for prefix in bucket_query.prefixes:
            prefixes += xml_templates.bucket_query_prefixes_xml.format(prefix=prefix) + "\n"
        xml = xml_templates.bucket_query_xml.format(bucket_query=bucket_query, contents=contents, prefixes=prefixes)

        return Response(xml, status_code=200, headers={'Content-Type': 'application/xml'})
    else:
        xml = xml_templates.error_no_such_bucket_xml.format(name=bucket_name)
        return Response(xml, status_code=404, headers={'Content-Type': 'application/xml'})


def get_acl(request : Request, response : Response):
    return Response(xml_templates.acl_xml, status_code=200, headers={'Content-Type': 'application/xml'})


def delete_item(request : Request, response : Response, bucket_name, item_name):
    return
    # TODO: not implemented
    # handler.server.store.delete_item(bucket_name, item_name)


async def get_item(request : Request, response : Response, bucket_name, item_name):
    item = vars.store.get_item(bucket_name, item_name)
    if not item:
        return Response(xml_templates.non_empty_stub, status_code=404, headers={'Content-Type': 'application/xml'})

    content_length = item.size
    headers = request.headers

    if hasattr(item, 'creation_date'):
        last_modified = item.creation_date
    else:
        last_modified = item.modified_date
    last_modified = datetime.datetime.strptime(last_modified, '%Y-%m-%dT%H:%M:%S.000Z')
    last_modified_str = last_modified.strftime('%a, %d %b %Y %H:%M:%S GMT')

    if 'range' in headers:
        range_ = request.headers['range'].split('=')[1]
        start = int(range_.split('-')[0])
        finish_string = range_.split('-')[1]
        finish = int(finish_string) if finish_string else 0
        if finish == 0:
            finish = content_length - 1
        bytes_to_read = finish - start + 1
        stream = vars.store.read_item(item, start, bytes_to_read)
        return StreamingResponse(stream, status_code=206, headers={
            'Content-Type': item.content_type,
            'Last-Modified': last_modified_str,
            'Etag': item.md5,
            'Accept-Ranges': 'bytes',
            'Content-Range': 'bytes %s-%s/%s' % (start, finish, content_length),
            'Content-Length': '%s' % bytes_to_read,
        })


    if request.method == 'HEAD' and item.content_type == 'application/x-directory':
        jsn = json.dumps({
            "AcceptRanges": "bytes",
            "LastModified": last_modified_str,
            "ContentLength": 0,
            "ETag": item.md5,
            "ContentType": "application/x-directory",
            "Metadata": {
                "ctime": str(int(time.mktime(last_modified.timetuple()))),
                "mode": "493",
                "gid": "1001",
                "uid": "1000",
                "mtime": str(int(time.mktime(last_modified.timetuple())))
            }
        })
        return Response(jsn, status_code=200, headers={
            'Content-Type': 'application/json',
            'Last-Modified': last_modified_str,
            'Etag': item.md5,
            'Accept-Ranges': 'bytes',
            'Content-Type': item.content_type,
            'Content-Length': str(content_length),
            'x-amz-meta-ctime': str(int(time.mktime(last_modified.timetuple()))),
            'x-amz-meta-mode': '493',
            'x-amz-meta-gid': '1001',
            'x-amz-meta-uid': '1000',
            'x-amz-meta-mtime': str(int(time.mktime(last_modified.timetuple()))),
        })
    elif request.method == 'HEAD':
        return Response('', status_code=200, headers={
            'Last-Modified': last_modified_str,
            'Etag': item.md5,
            'Accept-Ranges': 'bytes',
            'Content-Type': item.content_type,
            'Content-Length': str(content_length),
        })
    elif request.method == 'GET':
        stream = vars.store.read_item(item, 0, content_length)
        return StreamingResponse(stream, status_code=200, media_type=item.content_type, headers={
            'Last-Modified': last_modified_str,
            'Etag': item.md5,
            'Accept-Ranges': 'bytes',
            'Content-Type': item.content_type,
            'Content-Length': str(content_length),
        })


def delete_items(request : Request, response : Response, bucket_name, keys):
    xml = ''
    for key in keys:
        delete_item(request, response, bucket_name, key)
        xml += xml_templates.deleted_deleted_xml.format(key=key)
    xml = xml_templates.deleted_xml.format(contents=xml)
    return Response(xml, status_code=200, headers={'Content-Type': 'application/xml'})

def create_multipart_upload(request : Request, response : Response, bucket_name, key, headers):
    uploadId = vars.store.create_multipart_upload(bucket_name, key, persistent_headers(headers))
    xml = xml_templates.create_multipart_upload_xml.format(
        bucket_name=bucket_name, key=key, upload_id=uploadId)
    return Response(xml, status_code=200, headers={'Content-Type': 'application/xml'})

def persistent_headers(headers):
    return {k: v for k, v in headers.items() if k in {
        'cache-control',
        'content-disposition',
        'content-encoding',
        'content-language',
        'content-type',
        'expires',
    }}

async def complete_multipart_upload(request : Request, response : Response, bucket_name, key, uploadId):
    # TODO: we should also parse req xml here and validate/merge only selected parts, but for now we do all known parts
    digest = await vars.store.complete_multipart_upload(bucket_name, key, uploadId)
    xml = xml_templates.complete_multipart_upload_xml.format(
        location='TODO',
        bucket=bucket_name,
        key=key,
        etag=digest,
    )
    return Response(xml, status_code=200, headers={'Content-Type': 'application/xml'})


async def upload_part(request : Request, response : Response, key, partNumber, uploadId, size, data):
    digest = await vars.store.upload_part(key, int(partNumber), uploadId, data, size)
    return Response('', status_code=200, headers={
        'Etag': '"%s"' % digest,
        'Content-Length': '0',
    })
