"""
=================================================
@Project -> File   ：aliyun -> service
@IDE    ：PyCharm
@Author ：sw
@Date   ：2021/1/21 10:05 上午
@Desc   ：
==================================================
"""
import httpx
import asyncio
from oss.auth import Auth


class Service:
    def __init__(self, auth: Auth):
        self.auth = auth
        self.client = httpx.Client()

    def __del__(self):
        if not self.client.is_closed:
            self.client.close()

    def build_request(self, *args, **kwargs) -> httpx.Request:
        return self.client.build_request(*args, **kwargs)

    def get_service(self, prefix: str = '', marker: str = '',
                    max_keys: int = 0, **kwargs) -> httpx.Response:
        """返回请求者拥有的所有存储空间
        :param prefix: 限定返回的bucket name必须以prefix作为前缀
        :param marker: 设定结果从marker之后按字母排序的第一个开始返回
        :param max_keys: 限定此次返回bucket的最大数 默认100 不能超过1000
        :param kwargs: 用于构建request请求的其他参数
        :return:
        """
        params = kwargs.pop('params') if 'params' in kwargs else {}
        if prefix:
            params['prefix'] = prefix
        if marker:
            params['marker'] = marker
        if max_keys:
            params['max-keys'] = max_keys
        url = f'https://{self.auth.bucket}.{self.auth.endpoint}'
        r = self.build_request('GET', url, params=params, **kwargs)
        self.auth.signature(r)
        resp = self.client.send(r)
        return resp


class AsyncService(Service):
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

    async def get_service(self, prefix: str = '', marker: str = '',
                          max_keys: int = 0, **kwargs) -> httpx.Response:
        corn = super().get_service(prefix, marker, max_keys, **kwargs)
        return await corn
