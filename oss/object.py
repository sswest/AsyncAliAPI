"""
=================================================
@Project -> File   ：aliyun -> object
@IDE    ：PyCharm
@Author ：sw
@Date   ：2021/1/21 11:38 上午
@Desc   ：
==================================================
"""
import re
import httpx
import asyncio
from typing import Union
from httpx import Response
from oss.auth import Auth


class ObjectClient:
    """object行为的同步方法"""

    def __init__(self, auth: Auth):
        self.auth = auth
        self.client = httpx.Client()

    def __del__(self):
        if not self.client.is_closed:
            self.client.close()

    def build_request(self, *args, **kwargs) -> httpx.Request:
        return self.client.build_request(*args, **kwargs)

    def put_object(self, target: str, file: Union[str, bytes],
                   **kwargs) -> Response:
        """用于上传文件，阿里云文档时间2020-11-30 09:53
        :param target: 上传至储存桶路径
        :param file: 要上传的文件路径或字节数据
        :param kwargs: 用于构建request请求的其他参数
        :return:
        """
        auth = self.auth
        url = f'https://{auth.bucket}.{auth.endpoint}/{target.lstrip("/")}'
        if isinstance(file, str):
            with open(file, 'rb') as f:
                r = self.build_request('PUT', url, data=f.read(), **kwargs)
        elif isinstance(file, bytes):
            r = self.build_request('PUT', url, data=file, **kwargs)
        else:
            raise TypeError(f'file 不支持 {type(file)} 类型')
        auth.signature(r)
        resp = self.client.send(r)
        return resp

    def get_object(self, target: str, _range: str = None, **kwargs) -> Response:
        """用于获取某个文件（Object）,此操作需要对此Object有读权限
        阿里云文档时间2020-07-07 10:18
        :param target: 文件路径
        :param _range: 用于获取大文件部分内容
        传参示例 "bytes=0-499" 表示第0~499字节范围的内容
        :param kwargs: 用于构建request请求的其他参数
        :return:
        """
        headers = {'Range': _range} if _range else {}
        if 'headers' in kwargs:
            headers.update(kwargs.pop('headers'))
        url = f'https://{self.auth.bucket}.{self.auth.endpoint}/{target.lstrip("/")}'
        r = self.build_request('GET', url, headers=headers, **kwargs)
        self.auth.signature(r)
        resp = self.client.send(r)
        return resp

    def copy_object(self, source: str, target: str, **kwargs) -> Response:
        """于拷贝同一地域下相同或不同存储空间（Bucket）之间的文件（Object）
        阿里云文档时间2021-01-07 18:35
        警告: 该方法在涉及文件名非ascii编码时会错误
        :param source 指定拷贝的源地址 格式"/BucketName/ObjectName"
        :param target 指定拷贝的目标地址
        :param kwargs: 用于构建request请求的其他参数
        :return:
        """
        headers = {'x-oss-copy-source': source}
        if 'headers' in kwargs:
            headers.update(kwargs.pop('headers'))
        url = f'https://{self.auth.bucket}.{self.auth.endpoint}/{target.lstrip("/")}'
        r = self.build_request('PUT', url, headers=headers, **kwargs)
        self.auth.signature(r)
        resp = self.client.send(r)
        return resp

    def append_object(self, target: str, data: bytes, position: int = 0,
                      **kwargs) -> Response:
        """用于以追加写的方式上传文件 阿里云文档时间 2020-11-20 13:28
        通过AppendObject操作创建的Object类型为Appendable Object
        通过PutObject上传的Object是Normal Object
        该方法只适用于Appendable Object
        :param target OSS文件路径
        :param data 要上传的数据
        :param position 上传的位置
        :param kwargs: 用于构建request请求的其他参数
        """
        url = f'https://{self.auth.bucket}.{self.auth.endpoint}/{target.lstrip("/")}'
        url += f'?append&position={position}'
        r = self.build_request('POST', url, data=data, **kwargs)
        self.auth.signature(r)
        resp = self.client.send(r)
        return resp

    def delete_object(self, target: str, **kwargs) -> Response:
        """用于删除某个文件
        阿里云文档时间 2020-11-20 15:28
        :param target: 要删除文件的路径
        :param kwargs: 用于构建request请求的其他参数
        """
        url = f'https://{self.auth.bucket}.{self.auth.endpoint}/{target.lstrip("/")}'
        r = self.build_request('DELETE', url, **kwargs)
        self.auth.signature(r)
        resp = self.client.send(r)
        return resp

    def delete_objects(self) -> Response:
        """用于删除同一个存储空间（Bucket）中的多个文件（Object）"""
        raise NotImplemented

    def head_object(self, target: str, **kwargs) -> Response:
        """用于获取某个文件（Object）的元信息
        阿里云文档时间 2020-12-31 16:10
        :param target 目标文件路径
        :param kwargs 用于构建request请求的其他参数
        :return: 该接口返回的在响应头中
        """
        url = f'https://{self.auth.bucket}.{self.auth.endpoint}/{target.lstrip("/")}'
        r = self.build_request('HEAD', url, **kwargs)
        self.auth.signature(r)
        resp = self.client.send(r)
        return resp

    def get_object_meta(self, target: str, **kwargs) -> Response:
        """用于获取一个文件的元数据信息，包括该Object的ETag、Size、LastModified信息
        阿里云文档时间 2020-02-28 17:00
        :param target 目标文件路径
        :param kwargs 用于构建request请求的其他参数
        :return: 该接口返回的在响应头中
        """
        url = f'https://{self.auth.bucket}.{self.auth.endpoint}/{target.lstrip("/")}'
        url += '?objectMeta'
        r = self.build_request('HEAD', url, **kwargs)
        self.auth.signature(r)
        resp = self.client.send(r)
        return resp

    def post_object(self):
        """用于通过HTML表单上传的方式将文件（Object）上传至指定存储空间"""
        raise NotImplemented('Please use method put_object')

    def restore_object(self, target: str, **kwargs) -> Response:
        """用于解冻归档类型（Archive）或冷归档（Cold Archive）的文件（Object）
        阿里云文档时间 2020-09-23 10:25
        :param target 目标文件路径
        :param kwargs 用于构建request请求的其他参数
        """
        url = f'https://{self.auth.bucket}.{self.auth.endpoint}/{target.lstrip("/")}'
        url += '?restore'
        r = self.build_request('POST', url, **kwargs)
        self.auth.signature(r)
        resp = self.client.send(r)
        return resp

    def select_object(self, target: str, data: str, type_: str = 'json',
                      **kwargs) -> Response:
        """用于对目标文件执行SQL语句 阿里云文档时间 2020-05-13 14:34
        :param target: 目标文件路径
        :param data: 包含sql语句的xml todo 由sql生成xml
        :param type_: 请求语法分为 csv 和 json 两种格式
        :param kwargs 用于构建request请求的其他参数
        """
        url = f'https://{self.auth.bucket}.{self.auth.endpoint}/{target.lstrip("/")}'
        url += f'?x-oss-process={type_.lower()}/select'
        r = self.build_request('POST', url, data=data, **kwargs)
        self.auth.signature(r)
        resp = self.client.send(r)
        return resp

    def put_object_acl(self, target: str, acl: str = 'default', **kwargs) -> Response:
        """用于修改文件（Object）的访问权限（ACL）
        阿里云文档时间 2020-04-23 15:44
        :param target 目标文件路径
        :param acl 应当是以下四种访问权限的其中一种
            private	            Object是私有资源
            public-read	        Object是公共读资源
            public-read-write	Object是公共读写资源
            default	            Object遵循其所在Bucket的读写权限
        """
        url = f'https://{self.auth.bucket}.{self.auth.endpoint}/{target.lstrip("/")}'
        url += '?acl'
        headers = {'x-oss-object-acl': acl}
        if 'headers' in kwargs:
            headers.update(kwargs.pop('headers'))
        r = self.build_request('PUT', url, headers=headers, **kwargs)
        self.auth.signature(r)
        resp = self.client.send(r)
        return resp

    def get_object_acl(self, target: str, **kwargs) -> Response:
        """用来获取某个存储空间（Bucket）下的某个文件（Object）的访问权限（ACL）
        阿里云文档时间 2020-02-28 17:05
        :param target 目标文件路径
        :param kwargs 用于构建request请求的其他参数
        """
        url = f'https://{self.auth.bucket}.{self.auth.endpoint}/{target.lstrip("/")}'
        url += '?acl'
        r = self.build_request('GET', url, **kwargs)
        self.auth.signature(r)
        resp = self.client.send(r)
        return resp

    def put_symlink(self, source: str, target: str, **kwargs) -> Response:
        """用于为OSS的目标文件（TargetObject）创建软链接（Symlink）
        阿里云文档时间 2020-06-19 12:54
        :param source 源文件
        :param target 目标文件路径
        :param kwargs 用于构建request请求的其他参数
        """
        url = f'https://{self.auth.bucket}.{self.auth.endpoint}/{target.lstrip("/")}'
        url += '?symlink'
        headers = {'x-oss-symlink-target': source}
        if 'headers' in kwargs:
            headers.update(kwargs.pop('headers'))
        r = self.build_request('PUT', url, headers=headers, **kwargs)
        self.auth.signature(r)
        resp = self.client.send(r)
        return resp

    def get_symlink(self, target: str, **kwargs) -> Response:
        """用于获取软链接 阿里云文档时间 2020-04-17 14:02
        :param target 目标文件路径
        :param kwargs 用于构建request请求的其他参数
        """
        url = f'https://{self.auth.bucket}.{self.auth.endpoint}/{target.lstrip("/")}'
        url += '?symlink'
        r = self.build_request('GET', url, **kwargs)
        self.auth.signature(r)
        resp = self.client.send(r)
        return resp

    def put_object_tag(self, target: str, data: str, **kwargs) -> Response:
        """用于设置或更新对象（Object）的标签（Tagging）信息
        阿里云文档时间 2020-10-15 16:46
        :param target 目标文件路径
        :param data 对象标签 todo 由dict生成data
        :param kwargs 用于构建request请求的其他参数
        """
        url = f'https://{self.auth.bucket}.{self.auth.endpoint}/{target.lstrip("/")}'
        url += '?tagging'
        r = self.build_request('PUT', url, data=data, **kwargs)
        self.auth.signature(r)
        resp = self.client.send(r)
        return resp

    def get_object_tagging(self, target: str, **kwargs) -> Response:
        """用于获取对象（Object）的标签（Tagging）信息
        阿里云文档时间 2020-10-15 16:57
        :param target: 目标文件路径
        :param kwargs: 用于构建request请求的其他参数
        :return:
        """
        url = f'https://{self.auth.bucket}.{self.auth.endpoint}/{target.lstrip("/")}'
        url += '?tagging'
        r = self.build_request('GET', url, **kwargs)
        self.auth.signature(r)
        resp = self.client.send(r)
        return resp

    def delete_object_tagging(self, target: str, **kwargs) -> Response:
        """用于删除指定对象（Object）的标签（Tagging）信息
        阿里云文档时间 2020-10-15 16:57
        :param target: 目标文件路径
        :param kwargs: 用于构建request请求的其他参数
        :return: 成功响应码204
        """
        url = f'https://{self.auth.bucket}.{self.auth.endpoint}/{target.lstrip("/")}'
        url += '?tagging'
        r = self.build_request('DELETE', url, **kwargs)
        self.auth.signature(r)
        resp = self.client.send(r)
        return resp

    def _initiate_multipart_upload(self, target: str, **kwargs) -> str:
        """初始化一个Multipart Upload事件
        todo 增加对文件分块上传的支持
        阿里云文档时间 2020-11-16 10:52
        :param target: 目标文件路径
        :param kwargs: 用于构建request请求的其他参数
        :return:
        """
        url = f'https://{self.auth.bucket}.{self.auth.endpoint}/{target.lstrip("/")}'
        url += '?uploads'
        r = self.build_request('POST', url, **kwargs)
        self.auth.signature(r)
        resp = self.client.send(r)
        return re.search(r'<UploadId>(.+)?</UploadId>', resp.text).group(1)


class ObjectAsyncClient(ObjectClient):
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

    async def put_object(self, target: str, file: Union[str, bytes],
                         **kwargs) -> httpx.Response:
        corn = super().put_object(target, file, **kwargs)
        return await corn

    async def get_object(self, target: str, _range: str = None, **kwargs) -> Response:
        corn = super().get_object(target, _range, **kwargs)
        return await corn

    async def copy_object(self, source: str, target: str, **kwargs) -> Response:
        corn = super().copy_object(source, target, **kwargs)
        return await corn

    async def append_object(self, target: str, data: bytes, position: int = 0,
                            **kwargs) -> Response:
        corn = super().append_object(target, data, position, **kwargs)
        return await corn

    async def delete_object(self, target: str, **kwargs) -> Response:
        corn = super().delete_object(target, **kwargs)
        return await corn

    async def head_object(self, target: str, **kwargs) -> Response:
        corn = super().head_object(target, **kwargs)
        return await corn

    async def get_object_meta(self, target: str, **kwargs) -> Response:
        corn = super().head_object(target, **kwargs)
        return await corn

    async def restore_object(self, target: str, **kwargs) -> Response:
        corn = super().restore_object(target, **kwargs)
        return await corn

    async def select_object(self, target: str, data: str, type_: str = 'json',
                            **kwargs) -> Response:
        corn = super().select_object(target, data, type_, **kwargs)
        return await corn

    async def put_object_acl(self, target: str, acl: str = 'default', **kwargs) -> Response:
        corn = super().put_object_acl(target, acl, **kwargs)
        return await corn

    async def get_object_acl(self, target: str, **kwargs) -> Response:
        corn = super().get_object_acl(target, **kwargs)
        return await corn

    async def put_symlink(self, source: str, target: str, **kwargs) -> Response:
        corn = super().put_symlink(source, target, **kwargs)
        return await corn

    async def get_symlink(self, target: str, **kwargs) -> Response:
        corn = super().get_symlink(target, **kwargs)
        return await corn

    async def put_object_tag(self, target: str, data: str, **kwargs) -> Response:
        corn = super().put_object_tag(target, data, **kwargs)
        return await corn

    async def get_object_tagging(self, target: str, **kwargs) -> Response:
        corn = super().get_object_tagging(target, **kwargs)
        return await corn
