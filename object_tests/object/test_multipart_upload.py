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

        part_number_2_etag: Dict[int, str] = {}
        contents = b''
        for part_number in range(1, parts + 1):
            part_content = self.random_bytes(part_size)
            contents += part_content
            part_number_2_etag[part_number] = self.object_service.upload_part(
                key=key,
                bucket=self.bucket_name,
                uploadId=upload_id,
                partNumber=part_number,
                body=part_content,
            )['eTag']

        e_tag = ''

        def run():
            nonlocal e_tag
            resp = self.object_service.complete_multipart_upload(
                bucket=self.bucket_name,
                key=key,
                uploadId=upload_id,
                multipartUpload={
                    'parts': list(map(
                        lambda x: {'partNumber': x, 'eTag': part_number_2_etag[x]},
                        range(1, 1 + parts),
                    )),
                }
            )
            self.assertIsNotNone(resp)
            self.assertEqual(resp['key'], key)
            self.assertEqual(resp['bucket'], self.bucket_name)
            e_tag = resp['eTag']
            self.assertIsNotNone(e_tag)

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

        # 获取文件内容，判断是否与上传的内容一致
        resp = self.object_service.get_object(
            bucket=self.bucket_name,
            key=key,
        )
        self.assertEqual(resp['contentLength'], parts * part_size)
        self.assertEqual(resp['contentType'], content_type)
        self.assertEqual(resp['eTag'], e_tag)
        self.assertEqual(resp['body'].read(), contents)

    def test_multipart_copy_upload(self):
        key = "testMultipartCopyUploadFile"
        content_type = 'application/octet-stream'
        parts = 2
        part_size = 5 * 1024 * 1024

        upload_id = self.object_service.create_multipart_upload(
            bucket=self.bucket_name,
            key=key,
            contentType=content_type,
        )['uploadId']

        bs = self.random_bytes(part_size)

        # 上传一个文件
        self.object_service.put_object(
            bucket=self.bucket_name,
            key=key,
            contentType=content_type,
            body=bs,
        )

        part_number_2_etag: Dict[int, str] = {}

        for i in range(1, parts + 1):
            resp = self.object_service.upload_part_copy(
                key=key,
                bucket=self.bucket_name,
                uploadId=upload_id,
                partNumber=i,
                copySource=f'{self.bucket_name}/{key}',
                copySourceRange=f'bytes={0}-{part_size - 1}',
            )
            self.assertIsNotNone(resp)
            etag = resp['copyPartResult']['eTag']
            self.assertIsNotNone(etag)
            part_number_2_etag[i] = etag

        complete_resp = self.object_service.complete_multipart_upload(
            bucket=self.bucket_name,
            key=key,
            uploadId=upload_id,
            multipartUpload={
                'parts': list(map(
                    lambda x: {'partNumber': x, 'eTag': part_number_2_etag[x]},
                    range(1, 1 + parts),
                )),
            },
        )

        # 获取文件内容，判断是否与上传的内容一致
        resp = self.object_service.get_object(
            bucket=self.bucket_name,
            key=key,
        )

        self.assertEqual(resp['contentLength'], parts * part_size)
        self.assertEqual(resp['contentType'], content_type)
        self.assertEqual(resp['body'].read(), bs * parts)
        self.assertEqual(resp['eTag'], complete_resp['eTag'])

    def test_abort_multipart_upload(self):
        key = 'testAbortMultipartUploadFile'
        content_type = 'application/octet-stream'
        part_size = 5 * 1024 * 1024

        upload_id = self.object_service.create_multipart_upload(
            bucket=self.bucket_name,
            key=key,
            contentType=content_type,
        )['uploadId']

        bs = self.random_bytes(part_size)

        # 上传一个文件
        self.object_service.put_object(
            bucket=self.bucket_name,
            key=key,
            contentType=content_type,
            body=bs,
        )

        # 拷贝分片
        resp = self.object_service.upload_part_copy(
            key=key,
            bucket=self.bucket_name,
            uploadId=upload_id,
            partNumber=1,
            copySource=f'{self.bucket_name}/{key}',
            copySourceRange=f'bytes={0}-{part_size - 1}',
        )

        self.assertIsNotNone(resp)
        etag = resp['copyPartResult']['eTag']
        self.assertIsNotNone(etag)
        last_modified = resp['copyPartResult']['lastModified']
        self.assertIsNotNone(last_modified)

        # 中止分片上传
        self.object_service.abort_multipart_upload(
            bucket=self.bucket_name,
            key=key,
            uploadId=upload_id,
        )

    def test_list_parts(self):
        """
        测试列出文件级别的分片
        """
        key = "testListPartsFile"
        content_type = 'application/octet-stream'
        parts = 3
        part_size = 5 * 1024 * 1024

        upload_id = self.object_service.create_multipart_upload(
            bucket=self.bucket_name,
            key=key,
            contentType=content_type,
        )['uploadId']

        bs = self.random_bytes(part_size)

        # 上传一个文件
        self.object_service.put_object(
            bucket=self.bucket_name,
            key=key,
            contentType=content_type,
            body=bs,
        )

        part_number_2_etag: Dict[int, str] = {}

        # 拷贝这么多分片
        for i in range(1, parts):
            resp = self.object_service.upload_part_copy(
                key=key,
                bucket=self.bucket_name,
                uploadId=upload_id,
                partNumber=i,
                copySource=f'{self.bucket_name}/{key}',
                copySourceRange=f'bytes={0}-{part_size - 1}',
            )
            self.assertIsNotNone(resp)
            etag = resp['copyPartResult']['eTag']
            self.assertIsNotNone(etag)
            part_number_2_etag[i] = etag

        # 再上传一个分片
        resp = self.object_service.upload_part(
            key=key,
            bucket=self.bucket_name,
            uploadId=upload_id,
            partNumber=parts,
            body=bs,
        )
        self.assertIsNotNone(resp)
        etag = resp['eTag']
        self.assertIsNotNone(etag)
        part_number_2_etag[parts] = etag

        # 列出所有分片
        resp = self.object_service.list_parts(
            bucket=self.bucket_name,
            key=key,
            uploadId=upload_id,
            maxParts=parts,
        )
        self.assertEqual(resp['bucket'], self.bucket_name)
        self.assertEqual(resp['key'], key)
        self.assertEqual(resp['uploadId'], upload_id)
        self.assertEqual(resp['maxParts'], parts)
        self.assertEqual(resp['isTruncated'], False)
        self.assertEqual(len(resp['parts']), parts)
        for part in resp['parts']:
            self.assertEqual(part['size'], part_size)
            self.assertEqual(part['eTag'], part_number_2_etag[part['partNumber']])
            self.assertIsNotNone(part['lastModified'])
            self.assertEqual(part['partNumber'] in part_number_2_etag, True)

    def test_list_multipart_uploads(self):
        """
        创建两个不同key的分片上传任务，第一个任务上传一个分片，第二个任务上传两个分片，然后列举bucket级别正在进行的分片上传的文件
        """
        key = "testKey"
        prefix = "testListMultipartUploadsFile-"
        key1 = prefix + "1"
        key2 = prefix + "2"
        content_type = 'application/octet-stream'
        part_size = 5 * 1024 * 1024
        bs = self.random_bytes(part_size)

        # 先上传一个文件
        self.object_service.put_object(
            bucket=self.bucket_name,
            key=key,
            contentType=content_type,
            body=bs,
        )

        # 创建第一个分片上传任务
        upload_id1 = self.object_service.create_multipart_upload(
            bucket=self.bucket_name,
            key=key1,
            contentType=content_type,
        )['uploadId']

        # 拷贝分片
        self.object_service.upload_part_copy(
            key=key1,
            bucket=self.bucket_name,
            uploadId=upload_id1,
            partNumber=1,
            copySource=f'{self.bucket_name}/{key}',
            copySourceRange=f'bytes={0}-{part_size - 1}',
        )

        # 创建第二个分片上传任务
        upload_id2 = self.object_service.create_multipart_upload(
            bucket=self.bucket_name,
            key=key2,
            contentType=content_type,
        )['uploadId']

        # 拷贝分片
        for i in range(1, 3):
            self.object_service.upload_part_copy(
                key=key2,
                bucket=self.bucket_name,
                uploadId=upload_id2,
                partNumber=i,
                copySource=f'{self.bucket_name}/{key}',
                copySourceRange=f'bytes={0}-{part_size - 1}',
            )

        # 列举bucket级别正在进行的分片
        resp = self.object_service.list_multipart_uploads(
            bucket=self.bucket_name,
            maxUploads=2,
            prefix=prefix,
        )
        multipart_uploads = resp['uploads']
        while resp['isTruncated']:
            resp = self.object_service.list_multipart_uploads(
                bucket=self.bucket_name,
                maxUploads=2,
                prefix=prefix,
                keyMarker=resp['nextKeyMarker'],
                uploadIdMarker=resp['nextUploadIdMarker'],
            )
            multipart_uploads.extend(resp['uploads'])

        upload1 = list(filter(lambda x: x['uploadId'] == upload_id1, multipart_uploads))
        upload2 = list(filter(lambda x: x['uploadId'] == upload_id2, multipart_uploads))
        self.assertEqual(len(upload1), 1)
        self.assertEqual(len(upload2), 1)