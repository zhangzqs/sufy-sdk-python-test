import json

from botocore.exceptions import ClientError

from object_tests.object_test_base import BaseObjectTest
from util.cass import CassetteUtils


class TestBucketPolicy(BaseObjectTest):
    def __init__(self):
        super().__init__()
        self.__policy = """
    "Version": "sufy",
    "Id": "public",
    "Statement": [
      {
        "Sid": "publicGet",
        "Effect": "Allow",
        "Principal": "*",
        "Action": ["miku:MOSGetObject"],
        "Resource": ["srn:miku:::%s/*"]
      }
    ]
}
        """ % self.bucket_name

    def test_put_bucket_policy(self):
        def run():
            r = self.object_service.put_bucket_policy(
                bucket=self.bucket_name,
                policy=self.__policy,
            )
            self.assertIsNotNone(r)

        with self.vcr.use_cassette('test_put_bucket_policy.yaml') as cass:
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual('PUT', req.method)
            self.assertEqual('/' + self.bucket_name, req.url.path)
            self.assertEqual('policy', req.url.query)

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(204, resp.status_code)
            self.assertEqual('OK', resp.status_message)

    def test_get_bucket_policy(self):
        def run():
            self.object_service.put_bucket_policy(
                bucket=self.bucket_name,
                policy=self.__policy,
            )
            r = self.object_service.get_bucket_policy(bucket=self.bucket_name)
            self.assertIsNotNone(r)
            self.assertDictEqual(json.loads(r['policy']), json.loads(self.__policy))

        with self.vcr.use_cassette('test_get_bucket_policy.yaml') as cass:
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual('GET', req.method)
            self.assertEqual('/' + self.bucket_name, req.url.path)
            self.assertEqual('policy', req.url.query)

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(200, resp.status_code)
            self.assertEqual('OK', resp.status_message)

    def test_get_bucket_policy_when_no_policy(self):
        def run():
            self.object_service.delete_bucket_policy(bucket=self.bucket_name)
            with self.assertRaises(ClientError):
                self.object_service.get_bucket_policy(bucket=self.bucket_name)

        with self.vcr.use_cassette('test_get_bucket_policy_when_no_policy.yaml') as cass:
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual('GET', req.method)
            self.assertEqual('/' + self.bucket_name, req.url.path)
            self.assertEqual('policy', req.url.query)

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(404, resp.status_code)
            self.assertEqual('Not Found', resp.status_message)

    def test_delete_bucket_policy(self):
        def run():
            self.object_service.put_bucket_policy(
                bucket=self.bucket_name,
                policy=self.__policy,
            )
            r = self.object_service.delete_bucket_policy(bucket=self.bucket_name)
            self.assertIsNotNone(r)

        with self.vcr.use_cassette('test_delete_bucket_policy.yaml') as cass:
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual('DELETE', req.method)
            self.assertEqual('/' + self.bucket_name, req.url.path)
            self.assertEqual('policy', req.url.query)

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(204, resp.status_code)
            self.assertEqual('OK', resp.status_message)
