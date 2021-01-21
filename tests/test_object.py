"""
=================================================
@Project -> File   ：aliyun -> test_object
@IDE    ：PyCharm
@Author ：sw
@Date   ：2021/1/21 12:19 下午
@Desc   ：
==================================================
"""
from unittest import TestCase, IsolatedAsyncioTestCase
from oss.auth import Auth
from oss.object import ObjectAsyncClient, ObjectClient

auth = Auth(
    'YouAccessKeyId', 'YouAccessKeySecret',
    'bucket', 'endpoing'
)


class TestObjectAsyncClient(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.client = ObjectAsyncClient(auth)

    # async def test_put_object(self):
    #     path = 'settings.py'
    #     file = '/Users/sw/PycharmProjects/获客牛/settings.py'
    #     r = await self.client.put_object(path, file)
    #     self.assertEqual(r.status_code, 200)
    #
    # async def test_get_object(self):
    #     path = '人脉go.py'
    #     r = await self.client.get_object(path)
    #     self.assertEqual(r.status_code, 200)

    # async def test_copy_object(self):
    #     source = '/api-test369/rm.py'
    #     r = await self.client.copy_object(source, '/rm.cp')
    #     self.assertEqual(r.status_code, 200)

    async def test_head_object(self):
        r = await self.client.head_object('/settings.py')
        print(r.headers)
        self.assertEqual(r.status_code, 200)

    async def test_get_object_meta(self):
        r = await self.client.get_object_meta('/settings.py')
        print(f'get_object_meta {r.headers}')
        self.assertEqual(r.status_code, 200)

    async def test_put_object_acl(self):
        r = await self.client.get_object_acl('/settings.py')
        print(r.text)
        self.assertEqual(r.status_code, 200)


class TestObjectClient(TestCase):
    def setUp(self) -> None:
        self.client = ObjectClient(auth)

    # def test_put_object(self):
    #     path = 'settings.py'
    #     file = '/Users/sw/PycharmProjects/获客牛/settings.py'
    #     r = self.client.put_object(path, file)
    #     self.assertEqual(r.status_code, 200)

    # def test_get_object(self):
    #     path = '人脉go.py'
    #     r = self.client.get_object(path)
    #     self.assertEqual(r.status_code, 200)

    # def test_copy_object(self):
    #     source = '/api-test369/test.png'
    #     r = self.client.copy_object(source, '/test.cp')
    #     self.assertEqual(r.status_code, 200)

    # def test_append_object(self):
    #     r = self.client.append_object('/test.cp', bytes('\n哈哈哈\n', 'utf8'), 0)
    #     print(r.text)
    #     print(r.headers)
    #     self.assertEqual(r.status_code, 200)

    # def test_delete_object(self):
    #     r = self.client.delete_object('/requestments.txt')
    #     self.assertEqual(r.status_code, 204)

    def test_head_object(self):
        r = self.client.head_object('/settings.py')
        self.assertEqual(r.status_code, 200)

    def test_get_object_meta(self):
        r = self.client.get_object_meta('/settings.py')
        self.assertEqual(r.status_code, 200)

    def test_put_object_acl(self):
        r = self.client.put_object_acl('/settings.py', 'private')
        self.assertEqual(r.status_code, 200)

    def test_get_object_tagging(self):
        r = self.client.get_object_tagging('/settings.py')
        print(r.text)
        self.assertEqual(r.status_code, 200)

    def test_delete_object_tagging(self):
        r = self.client.delete_object_tagging('/settings.py')
        self.assertEqual(r.status_code, 204)

    def test__initiate_multipart_upload(self):
        uid = self.client._initiate_multipart_upload('test.py')
        print(uid)
