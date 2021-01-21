"""
=================================================
@Project -> File   ：aliyun -> test_service
@IDE    ：PyCharm
@Author ：sw
@Date   ：2021/1/21 2:43 下午
@Desc   ：
==================================================
"""
from unittest import IsolatedAsyncioTestCase
from oss.auth import Auth
from oss.service import AsyncService

auth = Auth(
    'YouAccessKeyId', 'YouAccessKeySecret',
    'bucket', 'endpoing'
)


class TestAsyncService(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.client = AsyncService(auth)

    async def test_get_service(self):
        r = await self.client.get_service(prefix='se', max_keys=1000)
        print(r.text)
        self.assertEqual(r.status_code, 200)
