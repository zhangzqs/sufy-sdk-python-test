from botocore.exceptions import ParamValidationError
from botocore.response import StreamingBody

from object_tests.object_test_base import BaseObjectTest
from util.cass import CassetteUtils


class PutGetObjectTest(BaseObjectTest):
    def test_put_empty_key_object(self):
        try:
            self.object_service.put_object(
                key='',
                bucket=self.bucket_name,
                body='',
            )
            self.fail('Expected ParamValidationError to be raised')
        except ParamValidationError as e:
            pass

    def test_put_object(self):
        key = 'testKey1'
        content = 'HelloWorld'
        metadata = {
            'test-key1': 'test-value1',
            'test-key2': 'test-value2',
        }
        storage_class = 'STANDARD'

        def run():
            put_object_response = self.object_service.put_object(
                key=key,
                bucket=self.bucket_name,
                body=content,
                metadata=metadata,
                storageClass=storage_class,
            )
            self.assertIsNotNone(put_object_response)
            self.assertIsNotNone(put_object_response['eTag'])

        with self.vcr.use_cassette('test_put_object.yaml') as cass:
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual('PUT', req.method)
            self.assertEqual('/' + self.bucket_name + '/' + key, req.url.path)
            self.assertEqual(str(len(content)), req.get_header_value('Content-Length'))
            storage_class1 = req.get_header_value('x-sufy-storage-class')
            if storage_class1 is not None:
                self.assertEqual(storage_class, storage_class1)
            self.assertIsNotNone(req.get_header_value('Content-Type'))

            for k, v in metadata.items():
                ev = req.get_header_value('x-sufy-meta' + k)
                self.assertIsNotNone(ev)
                self.assertEqual(v, ev)

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(200, resp.status_code)
            self.assertEqual('OK', resp.status_message)
            self.assertIsNotNone(resp.get_header_value('ETag'))

    def test_get_object(self):
        key = 'test_get_object'
        content = 'HelloWorld'
        metadata = {
            'test-key1': 'test-value1',
            'test-key2': 'test-value2',
        }

        def run():
            # 先上传
            put_object_response = self.object_service.put_object(
                key=key,
                bucket=self.bucket_name,
                body=content,
                metadata=metadata,
            )

            # 再下载
            get_object_response = self.object_service.get_object(
                key=key,
                bucket=self.bucket_name,
            )
            self.assertIsNotNone(get_object_response)
            self.assertIsNotNone(get_object_response['body'])
            self.assertEqual(content, get_object_response['body'].read().decode('utf-8'))

        response = self.object_service.get_object(
            key='key1',
            bucket=self.bucket_name,
        )
        body: StreamingBody = response['body']
        print(body.read())

    def test_head_object(self):

        response = self.object_service.head_object(
            key='key1',
            bucket=self.get_bucket_name(),
        )
        # print(response)
