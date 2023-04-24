import vcr
from botocore.exceptions import ClientError
from vcr.cassette import Cassette

from object_tests.object_test_base import BaseObjectTest


class TestBucketAcl(BaseObjectTest):
    def test_put_bucket_acl(self):
        def run():
            try:
                self.object_service.put_bucket_acl(
                    bucket=self.get_bucket_name(),
                )
                self.fail('Expected ClientError to be raised')
            except ClientError:
                pass

        with vcr.use_cassette('test_put_bucket_acl.yaml') as cass:  # type: Cassette
            run()
            self.check_public_request_header(cass)
            self.check_public_response_header(cass)
            code, text = self.get_http_response_status(cass)
            self.assertEqual(501, code)
            self.assertEqual('Not Implemented', text)

    def test_get_bucket_acl(self):
        try:
            self.object_service.get_bucket_acl(
                bucket=self.get_bucket_name(),
            )
            self.fail('Expected ClientError to be raised')
        except ClientError as e:
            self.assertEqual(501, self.get_http_status_code(e.response))
            self.assertEqual('NotImplemented', self.get_s3_error_code(e.response))
