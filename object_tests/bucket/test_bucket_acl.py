from botocore.exceptions import ClientError
from vcr.cassette import Cassette

from object_tests.object_test_base import BaseObjectTest
from util.cass import CassetteUtils


class TestBucketAcl(BaseObjectTest):
    def test_put_bucket_acl(self):
        def run():
            with self.assertRaises(ClientError):
                self.object_service.put_bucket_acl(
                    Bucket=self.bucket_name,
                )

        with self.vcr.use_cassette('test_put_bucket_acl.yaml') as cass:  # type: Cassette
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual('PUT', req.method)
            self.assertEqual('/' + self.bucket_name, req.url.path)
            self.assertEqual('acl', req.url.query)

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(501, resp.status_code)
            self.assertEqual('Not Implemented', resp.status_message)

    def test_get_bucket_acl(self):
        def run():
            try:
                self.object_service.get_bucket_acl(Bucket=self.bucket_name)
                self.fail('Expected ClientError to be raised')
            except ClientError as e:
                pass

        with self.vcr.use_cassette('test_get_bucket_acl.yaml') as cass:  # type: Cassette
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual('GET', req.method)
            self.assertEqual('/' + self.bucket_name, req.url.path)
            self.assertEqual('acl', req.url.query)

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(501, resp.status_code)
            self.assertEqual('Not Implemented', resp.status_message)
