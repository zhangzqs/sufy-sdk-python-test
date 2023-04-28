from typing import Dict

from object_tests.object_test_base import BaseObjectTest
from util.cass import CassetteUtils


class MultipartUploadTest(BaseObjectTest):
    def test_create_multipart_upload(self):
        key = 'test_create_multipart_upload'
        content_type = 'text/plain'

        def run():
            resp = self.object_service.create_multipart_upload(
                bucket=self.bucket_name,
                key=key,
                contentType=content_type,
            )
            self.assertIsNotNone(resp)
            self.assertEqual(resp['key'], key)
            self.assertEqual(resp['bucket'], self.bucket_name)
            self.assertIsNotNone(resp['uploadId'])

        with self.vcr.use_cassette('test_create_multipart_upload.yaml') as cass:
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual('POST', req.method)
            self.assertEqual('/' + self.bucket_name + '/' + key, req.url.path)
            self.assertEqual('uploads', req.url.query)

            self.assertIsNotNone(req.get_header_value('Content-Type'))

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(200, resp.status_code)
            self.assertEqual('OK', resp.status_message)

    def test_upload_part(self):
        key = 'test_upload_part'
        content_type = 'application/octet-stream'
        upload_id = self.object_service.create_multipart_upload(
            bucket=self.bucket_name,
            key=key,
            contentType=content_type,
        )['uploadId']

        def run():
            put_object_response = self.object_service.upload_part(
                key=key,
                bucket=self.bucket_name,
                uploadId=upload_id,
                partNumber=1,
                body=self.random_bytes(1024),
            )
            self.assertIsNotNone(put_object_response)
            self.assertIsNotNone(put_object_response['eTag'])

        with self.vcr.use_cassette('test_upload_part.yaml') as cass:
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual('PUT', req.method)
            self.assertEqual('/' + self.bucket_name + '/' + key, req.url.path)
            self.assertEqual('uploadId=' + upload_id + '&partNumber=1', req.url.query)

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(200, resp.status_code)
            self.assertEqual('OK', resp.status_message)
            self.assertIsNotNone(resp.get_header_value('ETag'))

    def test_complete_multipart_upload(self):
        key = "testCompleteMultipartUploadFile"
        content_type = 'application/octet-stream'
        parts = 2
        part_size = 5 * 1024 * 1024
        upload_id = self.object_service.create_multipart_upload(
            bucket=self.bucket_name,
            key=key,
            contentType=content_type,
        )['uploadId']

        part_number2_etag: Dict[int, str] = {}
        contents = b''
        for part_number in range(1, parts + 1):
            part_content = self.random_bytes(part_size)
            contents += part_content
            part_number2_etag[part_number] = self.object_service.upload_part(
                key=key,
                bucket=self.bucket_name,
                uploadId=upload_id,
                partNumber=part_number,
                body=part_content,
            )['eTag']

        def run():
            resp = self.object_service.complete_multipart_upload(
                bucket=self.bucket_name,
                key=key,
                uploadId=upload_id,
                multipartUpload={
                    'part': list(map(
                        lambda x: {'partNumber': x, 'eTag': part_number2_etag[x]},
                        range(1, 1 + parts),
                    )),
                }
            )
            self.assertIsNotNone(resp)
            self.assertEqual(resp['key'], key)
            self.assertEqual(resp['bucket'], self.bucket_name)
            self.assertIsNotNone(resp['eTag'])

        with self.vcr.use_cassette('test_complete_multipart_upload.yaml') as cass:
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual('POST', req.method)
            self.assertEqual('/' + self.bucket_name + '/' + key, req.url.path)
            self.assertEqual('uploadId=' + upload_id, req.url.query)

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(200, resp.status_code)
            self.assertEqual('OK', resp.status_message)
            etag = resp.get_header_value('ETag')
            self.assertIsNotNone(etag)

        # 获取文件内容，判断是否与上传的内容一致
        resp = self.object_service.get_object(
            bucket=self.bucket_name,
            key=key,
        )
        self.assertEqual(resp['contentLength'], parts * part_size)
        self.assertEqual(resp['contentType'], content_type)
        self.assertEqual(resp['eTag'], etag)
        self.assertEqual(resp['body'].read(), contents)
