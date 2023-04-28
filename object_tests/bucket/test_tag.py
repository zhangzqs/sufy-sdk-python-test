from object_tests.object_test_base import BaseObjectTest
from util.cass import CassetteUtils


class TestTag(BaseObjectTest):
    def __init__(self):
        super().__init__()
        self.__tagging = {
            'tagSet': [
                {
                    'key': 'test-tag-key-1',
                    'value': 'test-tag-value-1',
                },
                {
                    'key': 'test-tag-key-2',
                    'value': 'test-tag-value-2',
                },
            ],
        }

    def test_get_bucket_tagging_when_no_tagging(self):
        def run():
            self.object_service.delete_bucket_tagging(bucket=self.bucket_name)
            with self.assertRaises(Exception) as e:
                self.object_service.get_bucket_tagging(bucket=self.bucket_name)

        with self.vcr.use_cassette('test_get_bucket_tagging_when_no_tagging.yaml') as cass:
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual('GET', req.method)
            self.assertEqual('/' + self.bucket_name, req.url.path)
            self.assertEqual('tagging', req.url.query)

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(404, resp.status_code)
            self.assertEqual('Not Found', resp.status_message)

    def test_delete_bucket_tagging(self):
        def run():
            self.object_service.delete_bucket_tagging(bucket=self.bucket_name)

        with self.vcr.use_cassette('test_delete_bucket_tagging.yaml') as cass:
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual('DELETE', req.method)
            self.assertEqual('/' + self.bucket_name, req.url.path)
            self.assertEqual('tagging', req.url.query)

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(204, resp.status_code)
            self.assertEqual('No Content', resp.status_message)