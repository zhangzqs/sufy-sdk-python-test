from botocore.exceptions import ClientError

from object_tests.object_test_base import BaseObjectTest
from util.cass import CassetteUtils


class BucketManageTest(BaseObjectTest):
    def test_create_bucket(self):
        self.force_delete_bucket()

        def run():
            resp = self.object_service.create_bucket(
                Bucket=self.bucket_name,
                CreateBucketConfiguration={
                    'locationConstraint': self.test_config.object.region,
                },
            )
            self.assertIsNotNone(resp['location'])

        with self.vcr.use_cassette('test_create_bucket.yaml') as cass:
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual('PUT', req.method)
            self.assertEqual('/' + self.bucket_name, req.url.path)
            self.assertIsNotNone(req.get_header_value('Content-Length'))
            self.assertIsNotNone(req.get_header_value('Content-Type'))

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(200, resp.status_code)
            self.assertEqual('OK', resp.status_message)
            self.assertIsNotNone(resp.get_header_value('Location'))

    def test_head_bucket(self):
        def run():
            resp = self.object_service.head_bucket(
                Bucket=self.bucket_name,
            )
            # TODO：缺字段
            # self.assertIsNotNone(resp['location'])

        with self.vcr.use_cassette('test_head_bucket.yaml') as cass:
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual('HEAD', req.method)
            self.assertEqual('/' + self.bucket_name, req.url.path)

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(200, resp.status_code)
            self.assertEqual('OK', resp.status_message)
            self.assertEqual(self.test_config.object.region, resp.get_header_value('X-Sufy-Bucket-Region'))

    def test_get_bucket_location(self):
        def run():
            resp = self.object_service.get_bucket_location(
                Bucket=self.bucket_name,
            )
            self.assertIsNotNone(resp['locationConstraint'])

        with self.vcr.use_cassette('test_get_bucket_location.yaml') as cass:
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual('GET', req.method)
            self.assertEqual('/' + self.bucket_name, req.url.path)
            self.assertEqual('location', req.url.query)

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(200, resp.status_code)
            self.assertEqual('OK', resp.status_message)

    def test_delete_bucket(self):
        # 先确保存在该bucket
        self.make_sure_bucket_exists()

        def run():
            self.object_service.delete_bucket(
                Bucket=self.bucket_name,
            )

        with self.vcr.use_cassette('test_delete_bucket.yaml') as cass:
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual('DELETE', req.method)
            self.assertEqual('/' + self.bucket_name, req.url.path)

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(204, resp.status_code)
            self.assertEqual('No Content', resp.status_message)

        # 此时应当不存在该bucket
        with self.assertRaises(ClientError) as e:
            self.object_service.head_bucket(Bucket=self.bucket_name)

