from botocore.exceptions import ClientError

from object_tests.object_test_base import BaseObjectTest
from util.cass import CassetteUtils


class TestLifecycle(BaseObjectTest):
    def setUp(self):
        super().setUp()
        self.__lifecycle_configuration = {
            'rules': [
                {
                    'id': 'test',
                    'status': 'Enabled',
                    'filter': {
                        'prefix': 'test',
                    },
                    'expiration': {
                        'days': 3,
                    },
                    'transitions': [
                        {
                            'days': 1,
                            'storageClass': 'DEEP_ARCHIVE',
                        },
                    ],
                },
            ],
        }

    def test_put_lifecycle_configuration(self):
        def run():
            self.object_service.put_bucket_lifecycle_configuration(
                Bucket=self.bucket_name,
                LifecycleConfiguration=self.__lifecycle_configuration,
            )

        with self.vcr.use_cassette('test_put_lifecycle.yaml') as cass:
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual('PUT', req.method)
            self.assertEqual('/' + self.bucket_name, req.url.path)
            self.assertEqual('lifecycle', req.url.query)

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(200, resp.status_code)
            self.assertEqual('OK', resp.status_message)

    def test_get_lifecycle(self):
        # 先删除原来的生命周期配置
        self.object_service.delete_bucket_lifecycle(Bucket=self.bucket_name)
        # 再创建新的生命周期配置
        self.object_service.put_bucket_lifecycle_configuration(
            Bucket=self.bucket_name,
            LifecycleConfiguration=self.__lifecycle_configuration,
        )
        def run():
            get_bucket_lifecycle_resp = self.object_service.get_bucket_lifecycle_configuration(Bucket=self.bucket_name)
            # TODO: 服务器暂时不支持status属性
            actual_rule = get_bucket_lifecycle_resp['rules'][0]
            expected_rule = self.__lifecycle_configuration['rules'][0]
            expected_rule.pop('status')
            self.assertDictEqual(expected_rule, actual_rule)

        with self.vcr.use_cassette('test_get_lifecycle.yaml') as cass:
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual('GET', req.method)
            self.assertEqual('/' + self.bucket_name, req.url.path)
            self.assertEqual('lifecycle', req.url.query)

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(200, resp.status_code)
            self.assertEqual('OK', resp.status_message)

    def test_get_lifecycle_when_no_lifecycle(self):
        self.object_service.delete_bucket_lifecycle(Bucket=self.bucket_name)

        def run():
            with self.assertRaises(ClientError):
                self.object_service.get_bucket_lifecycle(Bucket=self.bucket_name)

        with self.vcr.use_cassette('test_get_lifecycle_when_no_lifecycle.yaml') as cass:
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual('GET', req.method)
            self.assertEqual('/' + self.bucket_name, req.url.path)
            self.assertEqual('lifecycle', req.url.query)

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(404, resp.status_code)
            self.assertEqual('Not Found', resp.status_message)

    def test_delete_lifecycle(self):
        def run():
            self.object_service.delete_bucket_lifecycle(Bucket=self.bucket_name)
            with self.assertRaises(ClientError):
                self.object_service.get_bucket_lifecycle(Bucket=self.bucket_name)

        with self.vcr.use_cassette('test_delete_lifecycle.yaml') as cass:
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual('DELETE', req.method)
            self.assertEqual('/' + self.bucket_name, req.url.path)
            self.assertEqual('lifecycle', req.url.query)

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(204, resp.status_code)
            self.assertEqual('No Content', resp.status_message)
