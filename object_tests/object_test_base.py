import json
import os
import unittest
from typing import Dict, Any, Tuple, Optional, List

import sufycore.session
import yaml
from botocore.exceptions import ClientError
from sufycore.config import Config
from vcr.cassette import Cassette

from config import TestConfig
from resources import test_config_file_path


class CassetteTestUtils(unittest.TestCase):
    def assert_http_request_exists_header(
            self,
            cass: Cassette,
            header: str,
            index: int = 0,
            expected_value: Optional[str] = None,
    ) -> str:
        val = self.get_http_request_header_value(cass, header, index)
        self.assertIsNotNone(val)
        if expected_value is not None:
            self.assertEqual(expected_value, val)
        return val

    def assert_http_response_exists_header(
            self,
            cass: Cassette,
            header: str,
            index: int = 0,
            expected_value: Optional[str] = None,
    ) -> Optional[List[str]]:
        val = self.get_http_response_header_value(cass, header, index)
        self.assertIsNotNone(val)
        if expected_value is not None:
            self.assertEqual(expected_value, val)
        return val

    def assert_http_response_status_equals(
            self,
            cass: Cassette,
            expect_code: int,
            index: int = 0,
            expect_text: Optional[str] = None,
    ):
        actual_code, actual_text = self.get_http_response_status(cass, index)
        self.assertEqual(expect_code, actual_code)
        if expect_text is not None:
            self.assertEqual(expect_text, actual_text)

    def get_http_request_header_value(self, cass: Cassette, header: str, index: int = 0) -> Optional[str]:
        try:
            val = cass.requests[index].headers.get(header)
            if isinstance(val, str):
                return val
            if isinstance(val, bytes):
                return val.decode('utf-8')
        except KeyError:
            return None

    def get_http_response_header_value(self, cass: Cassette, header: str, index: int = 0) -> Optional[List[str]]:
        def normalize_header(s):
            return '-'.join([w.lower() for w in s.split('-')])

        headers = cass.responses[index]['headers']
        for k, v in headers.items():
            if normalize_header(k) == normalize_header(header):
                return v
        return None

    def get_http_request_body(self, cass: Cassette, index: int = 0) -> bytes:
        return cass.requests[index].body

    def get_http_request_body_as_json(self, cass: Cassette, index: int = 0) -> Dict[str, Any]:
        return json.loads(self.get_http_request_body(cass, index))

    def get_http_request_body_as_str(self, cass: Cassette, index: int = 0) -> str:
        return self.get_http_request_body(cass, index).decode('utf-8')

    def get_http_response_status(self, cass: Cassette, index: int = 0) -> Tuple[int, str]:
        return (
            cass.responses[index]['status']['code'],
            cass.responses[index]['status']['message'],
        )

    def get_http_response_body(self, cass: Cassette, index: int = 0) -> bytes:
        return cass.responses[index]['body']['string']

    def get_http_response_body_as_json(self, cass: Cassette, index: int = 0) -> Dict[str, Any]:
        return json.loads(self.get_http_request_body_as_str(cass, index))

    def get_http_response_body_as_str(self, cass: Cassette, index: int = 0) -> str:
        return self.get_http_response_body(cass, index).decode('utf-8')


class BaseObjectTest(CassetteTestUtils):

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

    def check_public_request_header(self, cassette: Cassette, index: int = 0):
        # host = self.assert_http_request_exists_header(cassette, 'host', index)
        # if not self.test_config.object.forcePathStyle:
        #     self.assertEqual(host, f'{self.test_config.object.bucket}.{self.test_config.object.endpoint}')

        auth = self.assert_http_request_exists_header(cassette, 'authorization', index)
        self.assertTrue(auth.startswith('Sufy '))

        self.assert_http_request_exists_header(cassette, 'x-sufy-date', index)

        date = self.assert_http_request_exists_header(cassette, 'x-sufy-date', index)
        self.assertRegex(date, r'^\d{4}\d{2}\d{2}T\d{2}\d{2}\d{2}Z$')

        ua = self.assert_http_request_exists_header(cassette, 'user-agent', index)
        print(ua)

    def check_public_response_header(self, cassette: Cassette, index: int = 0):
        self.assert_http_response_exists_header(cassette, 'x-sufy-request-id', index)
        self.assert_http_response_exists_header(cassette, 'X-Reqid', index)
        date, = self.assert_http_response_exists_header(cassette, 'date', index)
        self.assertRegex(date, r'^\w{3}, \d{2} \w{3} \d{4} \d{2}:\d{2}:\d{2} GMT$')

    def prepare_test_file(self, key: str, content: str):
        self.object_service.put_object(
            bucket=self.get_bucket_name(),
            key=key,
            body=content,
        )

    def get_bucket_name(self):
        return self.test_config.object.bucket

    def make_sure_bucket_exists(self):
        try:
            self.object_service.head_bucket(
                bucket=self.get_bucket_name(),
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                self.object_service.create_bucket(
                    bucket=self.get_bucket_name(),
                    locationConstraint=self.test_config.object.region,
                )
            else:
                raise

    def random_bytes(self, size: int) -> bytes:
        return os.urandom(size)

    def clean_all_files(self):
        for key in self.object_service.list_objects(bucket=self.get_bucket_name()):
            self.object_service.delete_object(
                bucket=self.get_bucket_name(),
                key=key,
            )

    def force_delete_bucket(self):
        try:
            self.clean_all_files()
            self.object_service.delete_bucket(
                bucket=self.get_bucket_name(),
            )
        except ClientError:
            pass


__all__ = [
    'BaseObjectTest',
]
