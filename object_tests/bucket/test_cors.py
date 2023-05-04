from botocore.exceptions import ClientError

from object_tests.object_test_base import BaseObjectTest
from util.cass import CassetteUtils


class TestCors(BaseObjectTest):
    def setUp(self):
        super().setUp()
        self.__cors_configuration = {
            'CORSRules': [
                {
                    'id': 'test-cors-1',
                    'allowedHeaders': ["x-sufy-meta-abc", "x-sufy-meta-data"],
                    'allowedMethods': ['GET', 'PUT', 'POST', 'DELETE', 'HEAD'],
                    'allowedOrigins': ['http://www.a.com'],
                    'exposeHeaders': ["x-sufy-meta-abc", "x-sufy-meta-data"],
                    'maxAgeSeconds': 100,
                },
            ],
        }

    def test_put_cors(self):
        def run():
            self.object_service.put_bucket_cors(
                Bucket=self.bucket_name,
                CORSConfiguration=self.__cors_configuration,
            )

        with self.vcr.use_cassette('test_put_cors.yaml') as cass:
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual('PUT', req.method)
            self.assertEqual('/' + self.bucket_name, req.url.path)
            self.assertEqual('cors', req.url.query)
            self.assertIsNotNone(req.get_header_value('Content-MD5'))
            self.assertEqual(req.get_header_value('Content-Type'), 'application/json')

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(200, resp.status_code)
            self.assertEqual('OK', resp.status_message)

    def test_get_cors(self):
        self.object_service.put_bucket_cors(
            Bucket=self.bucket_name,
            CORSConfiguration=self.__cors_configuration,
        )

        def run():
            get_bucket_cors_resp = self.object_service.get_bucket_cors(Bucket=self.bucket_name)
            self.assertListEqual(self.__cors_configuration['CORSRules'], get_bucket_cors_resp['CORSRules'])

        with self.vcr.use_cassette('test_get_cors.yaml') as cass:
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual('GET', req.method)
            self.assertEqual('/' + self.bucket_name, req.url.path)
            self.assertEqual('cors', req.url.query)

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(200, resp.status_code)
            self.assertEqual('OK', resp.status_message)

    def test_get_cors_when_no_cors(self):
        # 先删除CORS
        self.object_service.delete_bucket_cors(Bucket=self.bucket_name)

        def run():
            with self.assertRaises(ClientError):
                self.object_service.get_bucket_cors(Bucket=self.bucket_name)

        with self.vcr.use_cassette('test_get_cors_when_no_cors.yaml') as cass:
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual('GET', req.method)
            self.assertEqual('/' + self.bucket_name, req.url.path)
            self.assertEqual('cors', req.url.query)

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(404, resp.status_code)
            self.assertEqual('Not Found', resp.status_message)

    def test_delete_cors(self):
        def run():
            self.object_service.delete_bucket_cors(Bucket=self.bucket_name)

        with self.vcr.use_cassette('test_delete_cors.yaml') as cass:
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual('DELETE', req.method)
            self.assertEqual('/' + self.bucket_name, req.url.path)
            self.assertEqual('cors', req.url.query)

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(204, resp.status_code)
            self.assertEqual('No Content', resp.status_message)
