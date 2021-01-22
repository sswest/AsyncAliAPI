"""
=================================================
@Project -> File   ：aliyun -> test_bucket
@IDE    ：PyCharm
@Author ：sw
@Date   ：2021/1/22 10:18 上午
@Desc   ：
==================================================
"""
from unittest import TestCase
from oss.auth import Auth
from oss.bucket import Bucket

auth = Auth(
    'YouAccessKeyId', 'YouAccessKeySecret',
    'bucket', 'endpoing'
)


class TestBucket(TestCase):
    def setUp(self) -> None:
        self.client = Bucket(auth)

    def test_put_bucket(self):
        r = self.client.put_bucket(
            'xiaosdq32', storage_class='IA',
            redundancy_type='ZRS', alc='public-read-write')
        self.assertIn(r.status_code, [200, 400, 409])

    def test_delete_bucket(self):
        r = self.client.delete_bucket('xiaosdq32')
        self.assertIn(r.status_code, [204, 404, 409])

    def test_get_bucket(self):
        r = self.client.get_bucket('xiaosdq31', max_count=2, prefix='xxx', version='2')
        self.assertIn(r.status_code, [200, 404, 400])

    def test_get_bucket_info(self):
        r = self.client.get_bucket_info('xiaosdq31')
        self.assertIn(r.status_code, [200, 404])

    def test_get_bucket_worm(self):
        r = self.client.get_bucket_info('xiaosdq31')
        self.assertIn(r.status_code, [200, ])
