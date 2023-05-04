from botocore.exceptions import ParamValidationError

from object_tests.object_test_base import BaseObjectTest
from util.cass import CassetteUtils


class PutGetObjectTest(BaseObjectTest):
    def test_put_empty_key_object(self):
        def run():
            with self.assertRaises(ParamValidationError):
                self.object_service.put_object(
                    Key='',
                    Bucket=self.bucket_name,
                    Body='',
                )

        run()

    def test_put_object(self):
        key = 'testKey1'
        content = 'HelloWorld'
        metadata = {
            'test-key1': 'test-value1',
            'test-key2': 'test-value2',
        }
        storage_class = 'STANDARD'
        content_type = 'text/plain'

        def run():
            put_object_response = self.object_service.put_object(
                Key=key,
                Bucket=self.bucket_name,
                Body=content,
                metadata=metadata,
                StorageClass=storage_class,
                contentType=content_type,
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
                ev = req.get_header_value('x-sufy-meta-' + k)
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
        self.object_service.put_object(
            Key=key,
            Bucket=self.bucket_name,
            Body=content,
            metadata=metadata,
        )

        def run():
            get_object_response = self.object_service.get_object(
                Key=key,
                Bucket=self.bucket_name,
            )
            self.assertIsNotNone(get_object_response)
            self.assertIsNotNone(get_object_response['eTag'])
            self.assertIsNotNone(get_object_response['contentLength'])
            self.assertEqual(get_object_response['contentLength'], len(content))
            self.assertIsNotNone(get_object_response['lastModified'])
            for k, v in metadata.items():
                self.assertEqual(v, get_object_response['metadata'][k])
            self.assertEqual(content, get_object_response['body'].read().decode('utf-8'))

        with self.vcr.use_cassette('test_get_object.yaml') as cass:
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual('GET', req.method)
            self.assertEqual('/' + self.bucket_name + '/' + key, req.url.path)

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(200, resp.status_code)
            self.assertEqual('OK', resp.status_message)
            self.assertIsNotNone(resp.get_header_value('ETag'))

            last_modified = resp.get_header_value('Last-Modified')
            self.assertIsNotNone(last_modified)
            # 校验是否符合RFC822时间标准
            self.assertIsNotNone(last_modified)
            self.assertRegex(last_modified, r'^[a-zA-Z]{3}, \d{2} [a-zA-Z]{3} \d{4} \d{2}:\d{2}:\d{2} GMT$')

            self.assertEqual(resp.get_header_value('Content-Length'), str(len(content)))
            for k, v in metadata.items():
                ev = resp.get_header_value('x-sufy-meta-' + k)
                self.assertIsNotNone(ev)
                self.assertEqual(v, ev)

            self.assertEqual(content, resp.body.as_str)

    def test_head_object(self):
        key = 'test_head_object'
        content = 'HelloWorld'
        metadata = {
            'test-key1': 'test-value1',
            'test-key2': 'test-value2',
        }
        self.object_service.put_object(
            Key=key,
            Bucket=self.bucket_name,
            Body=content,
            metadata=metadata,
        )

        def run():
            get_object_response = self.object_service.head_object(
                Key=key,
                Bucket=self.bucket_name,
            )
            self.assertIsNotNone(get_object_response)
            self.assertIsNotNone(get_object_response['eTag'])
            self.assertIsNotNone(get_object_response['contentLength'])
            self.assertEqual(get_object_response['contentLength'], len(content))
            self.assertIsNotNone(get_object_response['lastModified'])
            for k, v in metadata.items():
                self.assertEqual(v, get_object_response['metadata'][k])

        with self.vcr.use_cassette('test_head_object.yaml') as cass:
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual('HEAD', req.method)
            self.assertEqual('/' + self.bucket_name + '/' + key, req.url.path)

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(200, resp.status_code)
            self.assertEqual('OK', resp.status_message)
            self.assertIsNotNone(resp.get_header_value('ETag'))

            last_modified = resp.get_header_value('Last-Modified')
            self.assertIsNotNone(last_modified)
            # 校验是否符合RFC822时间标准
            self.assertIsNotNone(last_modified)
            self.assertRegex(last_modified, r'^[a-zA-Z]{3}, \d{2} [a-zA-Z]{3} \d{4} \d{2}:\d{2}:\d{2} GMT$')

            self.assertEqual(resp.get_header_value('Content-Length'), str(len(content)))
            for k, v in metadata.items():
                ev = resp.get_header_value('x-sufy-meta-' + k)
                self.assertIsNotNone(ev)
                self.assertEqual(v, ev)
