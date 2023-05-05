from typing import Dict

from object_tests.object_test_base import BaseObjectTest
from util.cass import CassetteUtils


class MultipartUploadTest(BaseObjectTest):
    def test_create_multipart_upload(self):
        key = 'test_create_multipart_upload'
        content_type = 'text/plain'

        def run():
            resp = self.object_service.create_multipart_upload(
                Bucket=self.bucket_name,
                Key=key,
                ContentType=content_type,
            )
            self.assertIsNotNone(resp)
            self.assertEqual(resp['Key'], key)
            self.assertEqual(resp['Bucket'], self.bucket_name)
            self.assertIsNotNone(resp['UploadId'])

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
            Bucket=self.bucket_name,
            Key=key,
            ContentType=content_type,
        )['UploadId']

        def run():
            put_object_response = self.object_service.upload_part(
                Key=key,
                Bucket=self.bucket_name,
                UploadId=upload_id,
                PartNumber=1,
                Body=self.random_bytes(1024),
            )
            self.assertIsNotNone(put_object_response)
            self.assertIsNotNone(put_object_response['ETag'])

        with self.vcr.use_cassette('test_upload_part.yaml') as cass:
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual('PUT', req.method)
            self.assertEqual('/' + self.bucket_name + '/' + key, req.url.path)
            self.assertEqual('UploadId=' + upload_id + '&PartNumber=1', req.url.query)

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
            Bucket=self.bucket_name,
            Key=key,
            ContentType=content_type,
        )['UploadId']

        part_number_2_etag: Dict[int, str] = {}
        contents = b''
        for part_number in range(1, parts + 1):
            part_content = self.random_bytes(part_size)
            contents += part_content
            part_number_2_etag[part_number] = self.object_service.upload_part(
                Key=key,
                Bucket=self.bucket_name,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=part_content,
            )['ETag']

        e_tag = ''

        def run():
            nonlocal e_tag
            resp = self.object_service.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={
                    'Parts': list(map(
                        lambda x: {'PartNumber': x, 'ETag': part_number_2_etag[x]},
                        range(1, 1 + parts),
                    )),
                }
            )
            self.assertIsNotNone(resp)
            self.assertEqual(resp['Key'], key)
            self.assertEqual(resp['Bucket'], self.bucket_name)
            e_tag = resp['ETag']
            self.assertIsNotNone(e_tag)

        with self.vcr.use_cassette('test_complete_multipart_upload.yaml') as cass:
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual('POST', req.method)
            self.assertEqual('/' + self.bucket_name + '/' + key, req.url.path)
            self.assertEqual('UploadId=' + upload_id, req.url.query)

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(200, resp.status_code)
            self.assertEqual('OK', resp.status_message)

        # 获取文件内容，判断是否与上传的内容一致
        resp = self.object_service.get_object(
            Bucket=self.bucket_name,
            Key=key,
        )
        self.assertEqual(resp['ContentLength'], parts * part_size)
        self.assertEqual(resp['ContentType'], content_type)
        self.assertEqual(resp['ETag'], e_tag)
        self.assertEqual(resp['Body'].read(), contents)

    def test_multipart_copy_upload(self):
        key = "testMultipartCopyUploadFile"
        content_type = 'application/octet-stream'
        parts = 2
        part_size = 5 * 1024 * 1024

        upload_id = self.object_service.create_multipart_upload(
            Bucket=self.bucket_name,
            Key=key,
            ContentType=content_type,
        )['UploadId']

        bs = self.random_bytes(part_size)

        # 上传一个文件
        self.object_service.put_object(
            Bucket=self.bucket_name,
            Key=key,
            ContentType=content_type,
            Body=bs,
        )

        part_number_2_etag: Dict[int, str] = {}

        for i in range(1, parts + 1):
            resp = self.object_service.upload_part_copy(
                Key=key,
                Bucket=self.bucket_name,
                UploadId=upload_id,
                PartNumber=i,
                CopySource=f'{self.bucket_name}/{key}',
                CopySourceRange=f'bytes={0}-{part_size - 1}',
            )
            self.assertIsNotNone(resp)
            etag = resp['CopyPartResult']['ETag']
            self.assertIsNotNone(etag)
            part_number_2_etag[i] = etag

        complete_resp = self.object_service.complete_multipart_upload(
            Bucket=self.bucket_name,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={
                'Parts': list(map(
                    lambda x: {'PartNumber': x, 'ETag': part_number_2_etag[x]},
                    range(1, 1 + parts),
                )),
            },
        )

        # 获取文件内容，判断是否与上传的内容一致
        resp = self.object_service.get_object(
            Bucket=self.bucket_name,
            Key=key,
        )

        self.assertEqual(resp['ContentLength'], parts * part_size)
        self.assertEqual(resp['ContentType'], content_type)
        self.assertEqual(resp['Body'].read(), bs * parts)
        self.assertEqual(resp['ETag'], complete_resp['ETag'])

    def test_abort_multipart_upload(self):
        key = 'testAbortMultipartUploadFile'
        content_type = 'application/octet-stream'
        part_size = 5 * 1024 * 1024

        upload_id = self.object_service.create_multipart_upload(
            Bucket=self.bucket_name,
            Key=key,
            ContentType=content_type,
        )['UploadId']

        bs = self.random_bytes(part_size)

        # 上传一个文件
        self.object_service.put_object(
            Bucket=self.bucket_name,
            Key=key,
            ContentType=content_type,
            Body=bs,
        )

        # 拷贝分片
        resp = self.object_service.upload_part_copy(
            Key=key,
            Bucket=self.bucket_name,
            UploadId=upload_id,
            PartNumber=1,
            CopySource=f'{self.bucket_name}/{key}',
            CopySourceRange=f'bytes={0}-{part_size - 1}',
        )

        self.assertIsNotNone(resp)
        etag = resp['CopyPartResult']['ETag']
        self.assertIsNotNone(etag)
        last_modified = resp['CopyPartResult']['LastModified']
        self.assertIsNotNone(last_modified)

        # 中止分片上传
        self.object_service.abort_multipart_upload(
            Bucket=self.bucket_name,
            Key=key,
            UploadId=upload_id,
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
            Bucket=self.bucket_name,
            Key=key,
            ContentType=content_type,
        )['UploadId']

        bs = self.random_bytes(part_size)

        # 上传一个文件
        self.object_service.put_object(
            Bucket=self.bucket_name,
            Key=key,
            ContentType=content_type,
            Body=bs,
        )

        part_number_2_etag: Dict[int, str] = {}

        # 拷贝这么多分片
        for i in range(1, parts):
            resp = self.object_service.upload_part_copy(
                Key=key,
                Bucket=self.bucket_name,
                UploadId=upload_id,
                PartNumber=i,
                CopySource=f'{self.bucket_name}/{key}',
                CopySourceRange=f'bytes={0}-{part_size - 1}',
            )
            self.assertIsNotNone(resp)
            etag = resp['CopyPartResult']['ETag']
            self.assertIsNotNone(etag)
            part_number_2_etag[i] = etag

        # 再上传一个分片
        resp = self.object_service.upload_part(
            Key=key,
            Bucket=self.bucket_name,
            UploadId=upload_id,
            PartNumber=parts,
            Body=bs,
        )
        self.assertIsNotNone(resp)
        etag = resp['ETag']
        self.assertIsNotNone(etag)
        part_number_2_etag[parts] = etag

        # 列出所有分片
        resp = self.object_service.list_parts(
            Bucket=self.bucket_name,
            Key=key,
            UploadId=upload_id,
            MaxParts=parts,
        )
        self.assertEqual(resp['Bucket'], self.bucket_name)
        self.assertEqual(resp['Key'], key)
        self.assertEqual(resp['UploadId'], upload_id)
        self.assertEqual(resp['MaxParts'], parts)
        self.assertEqual(resp['IsTruncated'], False)
        self.assertEqual(len(resp['Parts']), parts)
        for part in resp['Parts']:
            self.assertEqual(part['Size'], part_size)
            self.assertEqual(part['ETag'], part_number_2_etag[part['PartNumber']])
            self.assertIsNotNone(part['LastModified'])
            self.assertEqual(part['PartNumber'] in part_number_2_etag, True)

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
            Bucket=self.bucket_name,
            Key=key,
            ContentType=content_type,
            Body=bs,
        )

        # 创建第一个分片上传任务
        upload_id1 = self.object_service.create_multipart_upload(
            Bucket=self.bucket_name,
            Key=key1,
            ContentType=content_type,
        )['UploadId']

        # 拷贝分片
        self.object_service.upload_part_copy(
            Key=key1,
            Bucket=self.bucket_name,
            UploadId=upload_id1,
            PartNumber=1,
            CopySource=f'{self.bucket_name}/{key}',
            CopySourceRange=f'bytes={0}-{part_size - 1}',
        )

        # 创建第二个分片上传任务
        upload_id2 = self.object_service.create_multipart_upload(
            Bucket=self.bucket_name,
            Key=key2,
            ContentType=content_type,
        )['UploadId']

        # 拷贝分片
        for i in range(1, 3):
            self.object_service.upload_part_copy(
                Key=key2,
                Bucket=self.bucket_name,
                UploadId=upload_id2,
                PartNumber=i,
                CopySource=f'{self.bucket_name}/{key}',
                CopySourceRange=f'bytes={0}-{part_size - 1}',
            )

        # 列举bucket级别正在进行的分片
        resp = self.object_service.list_multipart_uploads(
            Bucket=self.bucket_name,
            MaxUploads=2,
            Prefix=prefix,
        )
        multipart_uploads = resp['uploads']
        while resp['IsTruncated']:
            resp = self.object_service.list_multipart_uploads(
                Bucket=self.bucket_name,
                MaxUploads=2,
                Prefix=prefix,
                KeyMarker=resp['NextKeyMarker'],
                UploadIdMarker=resp['NextUploadIdMarker'],
            )
            multipart_uploads.extend(resp['uploads'])

        upload1 = list(filter(lambda x: x['UploadId'] == upload_id1, multipart_uploads))
        upload2 = list(filter(lambda x: x['UploadId'] == upload_id2, multipart_uploads))
        self.assertEqual(len(upload1), 1)
        self.assertEqual(len(upload2), 1)
