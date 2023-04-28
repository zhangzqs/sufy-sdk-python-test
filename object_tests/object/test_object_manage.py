from object_tests.object_test_base import BaseObjectTest
from util.cass import CassetteUtils


class ObjectManageTest(BaseObjectTest):
    def test_copy_object(self):
        src_key = "testCopyObjectFileKeySrc"
        dest_key = "testCopyObjectFileKeyDest"
        content = "testCopyObjectFileContent"
        metadata_directive = "REPLACE"

        self.prepare_test_file(src_key, content)

        def run():
            self.object_service.copy_object(
                bucket=self.bucket_name,
                key=dest_key,
                copySource=f"{self.bucket_name}/{src_key}",
                metadataDirective=metadata_directive,
            )

        with self.vcr.use_cassette("object_copy_object.yaml") as cass:
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual("PUT", req.method)
            self.assertEqual(f"/{self.bucket_name}/{dest_key}", req.url.path)
            self.assertEqual(metadata_directive, req.get_header_value("x-sufy-metadata-directive"))

            content_length = req.get_header_value("Content-Length")
            if content_length is not None:
                self.assertEqual('0', content_length)
            self.assertEqual(f"{self.bucket_name}/{src_key}", req.get_header_value("x-sufy-copy-source"))

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(200, resp.status_code)
            self.assertEqual("OK", resp.status_message)

        # 复制文件后，目标文件和源文件都存在
        self.object_service.head_object(bucket=self.bucket_name, key=dest_key)
        self.object_service.head_object(bucket=self.bucket_name, key=src_key)

    def test_delete_object(self):
        key = "testDeleteObjectFileKey"
        content = "testDeleteObjectFileContent"

        self.prepare_test_file(key, content)

        def run():
            self.object_service.delete_object(
                bucket=self.bucket_name,
                key=key,
            )

        with self.vcr.use_cassette("object_delete_object.yaml") as cass:
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual("DELETE", req.method)
            self.assertEqual(f"/{self.bucket_name}/{key}", req.url.path)

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(204, resp.status_code)
            self.assertEqual("No Content", resp.status_message)

        # 删除文件后，文件不存在
        with self.assertRaises(Exception):
            self.object_service.head_object(
                bucket=self.bucket_name,
                key=key,
            )

    def test_delete_objects(self):
        keys = []
        for i in range(0, 10):
            keys.append(f"testDeleteObjectsFileKey{i}")

        for key in keys:
            self.prepare_test_file(key, key + "-content")

        def run():
            self.object_service.delete_objects(
                bucket=self.bucket_name,
                delete={
                    'objects': list(map(lambda x: {'key': x}, keys)),
                    'quiet': False,
                },
            )

        with self.vcr.use_cassette("object_delete_objects.yaml") as cass:
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual("POST", req.method)
            self.assertEqual(f"/{self.bucket_name}", req.url.path)
            self.assertEqual("delete", req.url.query)

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(200, resp.status_code)
            self.assertEqual("OK", resp.status_message)

        # 删除文件后，文件不存在
        for key in keys:
            with self.assertRaises(Exception):
                self.object_service.head_object(
                    bucket=self.bucket_name,
                    key=key,
                )

    def test_restore_object(self):
        key = "testRestoreObjectFileKey"
        content = "testRestoreObjectFileContent"

        self.object_service.put_object(
            bucket=self.bucket_name,
            key=key,
            content=content,
            storageClass="DEEP_ARCHIVE",
        )

        def run():
            self.object_service.restore_object(
                bucket=self.bucket_name,
                key=key,
                restoreRequest={
                    'days': 1,
                },
            )

        with self.vcr.use_cassette("object_restore_object.yaml") as cass:
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual("POST", req.method)
            self.assertEqual(f"/{self.bucket_name}/{key}", req.url.path)
            self.assertEqual("restore", req.url.query)

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(202, resp.status_code)
            self.assertEqual("Accepted", resp.status_message)
