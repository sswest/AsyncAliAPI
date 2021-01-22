"""
=================================================
@Project -> File   ：aliyun -> bucket
@IDE    ：PyCharm
@Author ：sw
@Date   ：2021/1/21 10:05 上午
@Desc   ： 储存桶相关API 阿里云文档时间 2020-10-14 13:45
https://help.aliyun.com/document_detail/177682.html
==================================================
"""
import httpx
import asyncio
from httpx import Response
from oss.auth import Auth


class Bucket:
    """object行为的同步方法"""

    def __init__(self, auth: Auth):
        self.auth = auth
        self.client = httpx.Client()

    def __del__(self):
        if not self.client.is_closed:
            self.client.close()

    def build_request(self, *args, **kwargs) -> httpx.Request:
        return self.client.build_request(*args, **kwargs)

    def put_bucket(self, name: str, *, storage_class: str = 'Standard',
                   redundancy_type: str = 'LRS', alc: str = 'private',
                   **kwargs) -> Response:
        """用于创建存储空间（Bucket）阿里云文档时间 2020-12-11 11:43
        :param name: 储存桶名称
        :param storage_class: 指定Bucket的存储类型 有效值如下
                Standard（标准存储，默认值）
                IA（低频访问）
                Archive（归档存储）
                ColdArchive（冷归档存储）
        :param redundancy_type: 指定Bucket的数据容灾类型 有效值如下
                LRS（默认值）
                本地冗余LRS，将您的数据冗余存储在同一个可用区的不同存储设备上，可支持两个存储设备并发损坏时，
                仍维持数据不丢失，可正常访问。
                ZRS
                同城冗余ZRS采用多可用区（AZ）机制，将您的数据冗余存储在同一地域（Region）的3个可用区。
                可支持单个可用区（机房）整体故障时（如断电、火灾等），仍然能够保障数据的正常访问。
        :param alc: 指定Bucket的访问权限ACL 有效值如下
                public-read-write：公共读写
                public-read：公共读
                private：私有（默认值）
        :param kwargs: 用于传递其他request参数
        :return:
        """
        headers = {'x-oss-acl': alc}
        if 'headers' in kwargs:
            headers.update(kwargs.pop('headers'))
        data = f'''<?xml version="1.0" encoding="UTF-8"?>
<CreateBucketConfiguration>
    <StorageClass>{storage_class}</StorageClass>
    <DataRedundancyType>{redundancy_type}</DataRedundancyType>
</CreateBucketConfiguration>'''
        url = f'https://{name}.{self.auth.endpoint}/'
        r = self.build_request('PUT', url, data=data, **kwargs)
        self.auth.signature(r, bucket=name)
        resp = self.client.send(r)
        return resp

    def delete_bucket(self, name: str, **kwargs) -> Response:
        """用于删除某个存储空间（Bucket）阿里云文档时间 2019-12-05 17:51
        :param name: 要删除bucket名称
        :param kwargs: 用于传递其他request参数
        :return: 正确响应码204
        """
        url = f'https://{name}.{self.auth.endpoint}/'
        r = self.build_request('DELETE', url, **kwargs)
        self.auth.signature(r, bucket=name)
        resp = self.client.send(r)
        return resp

    def get_bucket(self, name: str = None, prefix: str = None, max_count: int = 100,
                   delimiter: str = None, marker: str = None, encoding: str = None,
                   version: str = None, **kwargs) -> Response:
        """用于列举存储空间（Bucket）中所有文件（Object）的信息
        阿里云文档时间 2020-11-20 13:25
        :param name: 可以指定目标储存桶名称
        :param prefix: 限定返回文件的Key必须以prefix作为前缀
        :param max_count: 指定返回Object的最大数 大于0小于1000
        :param delimiter: 对Object名字进行分组的字符
        :param marker: 设定从marker之后按字母排序开始返回Object
        :param encoding: 对返回的内容进行编码并指定编码的类型 可选值 URL
        :param version: 接口版本 可选值 2 详见文档
        :param kwargs: 用于传递其他request参数
        :return:
        """
        params = {}
        if 'params' in kwargs:
            params = kwargs.pop('params')
        _params = {'delimiter': delimiter, 'marker': marker, 'encoding-type': encoding,
                   'prefix': prefix, 'max-keys': max_count, 'list-type': version}
        for k, v in _params.items():
            if v:
                params[k] = str(v)
        bucket = name if name else self.auth.bucket
        url = f'https://{bucket}.{self.auth.endpoint}/'
        r = self.build_request('GET', url, params=params, **kwargs)
        self.auth.signature(r, bucket=bucket)
        resp = self.client.send(r)
        return resp

    def get_bucket_info(self, name: str = None, **kwargs) -> Response:
        """用于查看存储空间（Bucket）的相关信息
        阿里云文档时间 2020-09-18 16:29
        :param name: 指定bucket名称
        :param kwargs: 用于传递其他request参数
        :return:
        """
        bucket = name if name else self.auth.bucket
        url = f'https://{bucket}.{self.auth.endpoint}/'
        url += '?bucketInfo'
        r = self.build_request('GET', url, **kwargs)
        self.auth.signature(r, bucket=bucket)
        resp = self.client.send(r)
        return resp

    def get_bucket_location(self, name: str = None, **kwargs) -> Response:
        """用于查看存储空间（Bucket）的位置信息
        阿里云文档时间 2020-08-05 09:49
        :param name: 指定bucket名称
        :param kwargs: 用于传递其他request参数
        :return:
        """
        bucket = name if name else self.auth.bucket
        url = f'https://{bucket}.{self.auth.endpoint}/'
        url += '?location'
        r = self.build_request('GET', url, **kwargs)
        self.auth.signature(r, bucket=bucket)
        resp = self.client.send(r)
        return resp

    def initiate_bucket_worm(self, retention_period: int, name: str = None,
                             **kwargs) -> Response:
        """用于新建一条合规保留策略 阿里云文档时间 2020-09-01 17:55
        :param retention_period: 指定Object保留天数
        :param name: 指定bucket名称
        :param kwargs: 用于传递其他request参数
        :return: 返回的x-oss-worm-id在响应头中
        """
        bucket = name if name else self.auth.bucket
        url = f'https://{bucket}.{self.auth.endpoint}/'
        url += '?worm'
        data = f'''<InitiateWormConfiguration>
  <RetentionPeriodInDays>{retention_period}</RetentionPeriodInDays>
</InitiateWormConfiguration>'''
        r = self.build_request('POST', url, data=data, **kwargs)
        self.auth.signature(r, bucket=bucket)
        resp = self.client.send(r)
        return resp

    def abort_bucket_worm(self, name: str = None, **kwargs) -> Response:
        """用于删除未锁定的合规保留策略 阿里云文档时间 2020-08-06 18:05
        :param name: 指定bucket名称
        :param kwargs: 用于传递其他request参数
        :return:
        """
        bucket = name if name else self.auth.bucket
        url = f'https://{bucket}.{self.auth.endpoint}/'
        url += '?worm'
        r = self.build_request('DELETE', url, **kwargs)
        self.auth.signature(r, bucket=bucket)
        resp = self.client.send(r)
        return resp

    def complete_bucket_worm(self, worm_id: str, name: str = None,
                             **kwargs) -> Response:
        """用于锁定合规保留策略 阿里云文档时间 2020-06-19 12:52
        :param worm_id: 合规策略ID
        :param name: 指定bucket名称
        :param kwargs: 用于传递其他request参数
        :return:
        """
        bucket = name if name else self.auth.bucket
        params = kwargs.pop('params') if 'params' in kwargs else {}
        params['wormId'] = worm_id
        url = f'https://{bucket}.{self.auth.endpoint}/'
        r = self.build_request('POST', url, params=params, **kwargs)
        self.auth.signature(r, bucket=bucket)
        resp = self.client.send(r)
        return resp

    def extend_bucket_worm(self):
        # TODO
        raise NotImplemented

    def get_bucket_worm(self, name: str = None, **kwargs) -> Response:
        """获取指定存储空间（Bucket）的合规保留策略信息
        阿里云文档时间 2020-10-15 16:12
        :param name: 指定bucket名称
        :param kwargs: 用于传递其他request参数
        :return:
        """
        bucket = name if name else self.auth.bucket
        url = f'https://{bucket}.{self.auth.endpoint}/'
        url += '?worm'
        r = self.build_request('GET', url, **kwargs)
        self.auth.signature(r, bucket=bucket)
        resp = self.client.send(r)
        return resp

    def put_bucket_acl(self, alc: str, name: str = None, **kwargs) -> Response:
        """用于设置或修改存储空间（Bucket）的访问权限（ACL）
        阿里云文档时间 2020-11-03 14:03
        :param alc: 指定Bucket的访问权限ACL 可选值如下
                public-read-write   公共读写
                public-read         公共读
                private             私有
        :param name: 指定bucket名称
        :param kwargs: 用于传递其他request参数
        :return:
        """
        headers = {'x-oss-acl': alc}
        if 'headers' in kwargs:
            headers.update(kwargs.pop('headers'))
        bucket = name if name else self.auth.bucket
        url = f'https://{bucket}.{self.auth.endpoint}/'
        url += '?acl'
        r = self.build_request('PUT', url, headers=headers, **kwargs)
        self.auth.signature(r, bucket=bucket)
        resp = self.client.send(r)
        return resp

    def get_bucket_acl(self, name: str = None, **kwargs) -> Response:
        """用于获取某个存储空间（Bucket）的访问权限（ACL）
        阿里云文档时间 2020-05-07 17:09
        :param name: 指定bucket名称
        :param kwargs: 用于传递其他request参数
        :return:
        """
        bucket = name if name else self.auth.bucket
        url = f'https://{bucket}.{self.auth.endpoint}/'
        url += '?acl'
        r = self.build_request('GET', url, **kwargs)
        self.auth.signature(r, bucket=bucket)
        resp = self.client.send(r)
        return resp

    def put_bucket_lifecycle(self):
        raise NotImplemented

    def get_bucket_lifecycle(self):
        raise NotImplemented

    def delete_bucket_lifecycle(self):
        raise NotImplemented


class AsyncBucket(Bucket):
    """object行为的异步方法"""

    def __init__(self, auth: Auth):
        self.auth = auth
        self.client = httpx.AsyncClient()

    def __del__(self):
        try:
            if not self.client.is_closed:
                task = asyncio.create_task(self.client.aclose())
                task.result()
        except RuntimeError:
            pass
        except asyncio.exceptions.InvalidStateError:
            pass

    async def put_bucket(self, name: str, *, storage_class: str = 'Standard',
                         redundancy_type: str = 'LRS', alc: str = 'private',
                         **kwargs) -> Response:
        corn = super().put_bucket(name, storage_class=storage_class,
                                  redundancy_type=redundancy_type,
                                  alc=alc, **kwargs)
        return await corn

    async def delete_bucket(self, name: str, **kwargs) -> Response:
        corn = super().delete_bucket(name, **kwargs)
        return await corn

    async def get_bucket(self, name: str = None, prefix: str = None, max_count: int = 100,
                         delimiter: str = None, marker: str = None, encoding: str = None,
                         version: str = None, **kwargs) -> Response:
        corn = super().get_bucket(name, prefix, max_count, delimiter,
                                  marker, encoding, version, **kwargs)
        return await corn

    async def get_bucket_info(self, name: str = None, **kwargs) -> Response:
        corn = super().get_bucket_info(name, **kwargs)
        return await corn

    async def get_bucket_location(self, name: str = None, **kwargs) -> Response:
        corn = super().get_bucket_location(name, **kwargs)
        return await corn

    async def initiate_bucket_worm(self, retention_period: int, name: str = None,
                                   **kwargs) -> Response:
        corn = super().initiate_bucket_worm(retention_period, name, **kwargs)
        return await corn

    async def abort_bucket_worm(self, name: str = None, **kwargs) -> Response:
        corn = super().abort_bucket_worm(name, **kwargs)
        return await corn

    async def complete_bucket_worm(self, worm_id: str, name: str = None,
                                   **kwargs) -> Response:
        corn = super().complete_bucket_worm(worm_id, name, **kwargs)
        return await corn

    def extend_bucket_worm(self):
        # TODO
        raise NotImplemented

    async def get_bucket_worm(self, name: str = None, **kwargs) -> Response:
        corn = super().get_bucket_worm(name, **kwargs)
        return await corn

    async def put_bucket_acl(self, alc: str, name: str = None, **kwargs) -> Response:
        corn = super().put_bucket_acl(alc, name, **kwargs)
        return await corn

    async def get_bucket_acl(self, name: str = None, **kwargs) -> Response:
        corn = super().get_bucket_acl(name, **kwargs)
        return await corn

    def put_bucket_lifecycle(self):
        raise NotImplemented

    def get_bucket_lifecycle(self):
        raise NotImplemented

    def delete_bucket_lifecycle(self):
        raise NotImplemented
