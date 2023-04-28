import os
import unittest

import sufycore.session
import vcr
import yaml
from botocore.exceptions import ClientError
from sufycore.config import Config

from config import TestConfig
from resources import test_config_file_path
from util.cass import CassetteRequest, CassetteResponse


class BaseObjectTest(unittest.TestCase):
    def setUp(self) -> None:
        with open(test_config_file_path) as f:
            test_config = TestConfig.from_dict(yaml.load(f, Loader=yaml.FullLoader))
            self.test_config = test_config

        # logging.basicConfig(level=logging.DEBUG)

        self.session = sufycore.session.Session()

        proxies_arg = None
        if test_config.proxy.enable:
            proxies_arg = {
                'http': f'{test_config.proxy.type}://{test_config.proxy.host}:{test_config.proxy.port}',
            }
        self.object_service = self.session.create_client(
            service_name='object',
            sufy_access_key_id=test_config.auth.accessKey,
            sufy_secret_access_key=test_config.auth.secretKey,
            region_name=test_config.object.region,
            endpoint_url=test_config.object.endpoint,
            config=Config(proxies=proxies_arg)
        )

        self.vcr = vcr.VCR(
            cassette_library_dir=test_config.vcr.cassette_library_dir,
            serializer=test_config.vcr.serializer,
            record_mode=test_config.vcr.record_mode,
            match_on=test_config.vcr.match_on,
        )

    def check_public_request_header(self, request: CassetteRequest):
        host = request.url.hostname
        if not self.test_config.object.forcePathStyle:
            self.assertEqual(host, f'{self.test_config.object.bucket}.{self.test_config.object.endpoint}')

        auth = request.get_header_value('authorization')
        self.assertTrue(auth.startswith('Sufy '))

        date = request.get_header_value('x-sufy-date')
        self.assertIsNotNone(date)
        self.assertRegex(date, r'^\d{4}\d{2}\d{2}T\d{2}\d{2}\d{2}Z$')

        ua = request.get_header_value('user-agent')
        self.assertIsNotNone(ua)
        # print(ua)

    def check_public_response_header(self, response: CassetteResponse):
        self.assertIsNotNone(response.get_header_value('x-sufy-request-id'))
        self.assertIsNotNone(response.get_header_value('X-Reqid'))

        date = response.get_header_value('date')
        self.assertIsNotNone(date)
        self.assertRegex(date, r'^\w{3}, \d{2} \w{3} \d{4} \d{2}:\d{2}:\d{2} GMT$')

    def prepare_test_file(self, key: str, content: str):
        self.object_service.put_object(
            bucket=self.bucket_name,
            key=key,
            body=content,
        )

    @property
    def bucket_name(self):
        return self.test_config.object.bucket

    def make_sure_bucket_exists(self):
        try:
            self.object_service.head_bucket(
                bucket=self.bucket_name,
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                self.object_service.create_bucket(
                    bucket=self.bucket_name,
                    locationConstraint=self.test_config.object.region,
                )
            else:
                raise

    def random_bytes(self, size: int) -> bytes:
        return os.urandom(size)

    def clean_all_files(self):
        for key in self.object_service.list_objects(bucket=self.bucket_name):
            self.object_service.delete_object(bucket=self.bucket_name, key=key)

    def force_delete_bucket(self):
        try:
            self.clean_all_files()
            self.object_service.delete_bucket(bucket=self.bucket_name)
        except ClientError:
            pass


__all__ = [
    'BaseObjectTest',
]
