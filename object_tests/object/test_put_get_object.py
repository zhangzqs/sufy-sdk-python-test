from botocore.exceptions import ParamValidationError
from botocore.response import StreamingBody

from object_tests.object_test_base import BaseObjectTest


class PutGetObjectTest(BaseObjectTest):
    def test_put_empty_key_object(self):
        try:
            self.object_service.put_object(
                key='',
                bucket=self.get_bucket_name(),
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
        resp = self.object_service.put_object(
            key=key,
            bucket=self.get_bucket_name(),
            body=content,
            metadata=metadata,
            storageClass=storage_class,
        )
        self.check_public_response_header(resp)
        self.assertIsNotNone(resp['eTag'])

    def test_get_object(self):
        response = self.object_service.get_object(
            key='key1',
            bucket=self.get_bucket_name(),
        )
        body: StreamingBody = response['body']
        print(body.read())

    def test_head_object(self):

        response = self.object_service.head_object(
            key='key1',
            bucket=self.get_bucket_name(),
        )
        # print(response)
