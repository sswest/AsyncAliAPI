"""
=================================================
@Project -> File   ：aliyun -> auth.py
@IDE    ：PyCharm
@Author ：sw
@Date   ：2021/1/21 10:02 上午
@Desc   ：阿里云OSS签名验证 文档时间：2020-07-07 10:15
https://help.aliyun.com/document_detail/31950.html
==================================================
"""

import hmac
import base64
import hashlib
from datetime import datetime
from httpx import Request


class Auth:
    SubResource = ('acl', 'uploads', 'location', 'cors', 'logging', 'website',
                   'referer', 'lifecycle', 'delete', 'append', 'tagging',
                   'objectMeta', 'uploadId', 'partNumber', 'security-token',
                   'position', 'img', 'style', 'styleName', 'replication',
                   'replicationProgress', 'replicationLocation', 'cname',
                   'bucketInfo', 'comp', 'qos', 'live', 'status', 'vod',
                   'startTime', 'endTime', 'symlink', 'x-oss-process',
                   'response-content-type', 'response-content-language',
                   'response-expires', 'response-cache-control',
                   'response-content-disposition', 'response-content-encoding')

    def __init__(self, accessKeyId: str, accessKeySecret: str,
                 bucket: str, endpoint: str):
        self.accessKeyId = accessKeyId
        self.accessKeySecret = accessKeySecret
        self.bucket = bucket
        self.endpoint = endpoint

    def signature(self, request: Request) -> None:
        """对OSS请求进行签名"""
        ct_type = request.headers.get('content-type', '')
        ct_md5 = request.headers.get('content-md5', '')
        oss_headers = {}
        for key in sorted(request.headers.keys()):
            if key.startswith('x-oss-'):
                oss_headers[key] = request.headers.get(key)
        oss_headers = '\n'.join(f'{k.strip()}:{v.strip()}' for k, v in oss_headers.items())
        if oss_headers:
            oss_headers += '\n'
        date = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        path = f'{request.url.path}?'
        _query = {}
        for query in request.url.query.decode("utf8").split('&'):
            if '=' not in query:
                if query in self.SubResource:
                    path += query
                continue
            k, v = query.split('=', 1)
            if k in self.SubResource:
                _query[k] = v
        path += '&'.join([f'{k}={v}' for k, v in _query.items()])
        path = path.strip('?')
        sign = hmac.new(
            bytes(self.accessKeySecret, 'utf8'),
            bytes(f'{request.method}\n{ct_md5}\n{ct_type}\n{date}\n{oss_headers}/{self.bucket}{path}', 'utf8'),
            hashlib.sha1
        )
        sign = base64.b64encode(sign.digest()).decode('utf8')
        request.headers['Authorization'] = f'OSS {self.accessKeyId}:{sign}'
        request.headers['Date'] = date
