from object_tests.object_test_base import BaseObjectTest
from util.cass import CassetteUtils


class ListObjectTest(BaseObjectTest):
    def test_list_object(self):
        prefix = 'dir1/'
        subdir = prefix + 'subdir/'
        delimiter = '/'
        n = 10
        keys = []
        for i in range(n):
            keys.append(f'{prefix}test-list-objects-v1-{i}')
        for i in range(n):
            keys.append(f'{subdir}test-list-objects-v1-{i}')
        for key in keys:
            self.prepare_test_file(key, key)

        def run():
            resp1 = self.object_service.list_objects(
                Bucket=self.bucket_name,
                Prefix=prefix,
                Delimiter=delimiter,
                MaxKeys=7,
            )

            self.assertEqual(self.bucket_name, resp1['Name'])
            self.assertEqual(prefix, resp1['Prefix'])
            self.assertEqual(delimiter, resp1['Delimiter'])
            self.assertEqual(7, resp1['MaxKeys'])
            self.assertGreater(len(resp1['NextMarker']), 0)
            self.assertTrue(resp1['IsTruncated'])

            contents = resp1['Contents']
            self.assertEqual(6, len(contents))
            for content in contents:
                self.assertGreater(len(content['Key']), 0)
                self.assertGreater(len(content['ETag']), 0)
                self.assertIn(content['Key'], keys)
                self.assertIsNotNone(content['LastModified'])
                self.assertEqual('STANDARD', content['StorageClass'])

            common_prefixes = resp1['CommonPrefixes']
            self.assertEqual(1, len(common_prefixes))
            self.assertEqual(subdir, common_prefixes[0]['Prefix'])

            resp2 = self.object_service.list_objects(
                Bucket=self.bucket_name,
                Prefix=prefix,
                Delimiter=delimiter,
                MaxKeys=7,
                Marker=resp1['NextMarker'],
            )

            self.assertEqual(self.bucket_name, resp2['Name'])
            self.assertEqual(prefix, resp2['Prefix'])
            self.assertEqual(delimiter, resp2['Delimiter'])
            self.assertEqual(7, resp2['MaxKeys'])
            self.assertFalse(resp2['IsTruncated'])

            contents = resp2['Contents']
            self.assertEqual(4, len(contents))
            for content in contents:
                self.assertGreater(len(content['Key']), 0)
                self.assertGreater(len(content['ETag']), 0)
                self.assertIn(content['Key'], keys)
                self.assertIsNotNone(content['LastModified'])
                self.assertEqual('STANDARD', content['StorageClass'])

            try:
                common_prefixes = resp2['CommonPrefixes']
                self.assertEqual(0, len(common_prefixes))
            except KeyError as e:
                pass

        with self.vcr.use_cassette('test_list_object.yaml') as cass:
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual('GET', req.method)
            self.assertEqual('/' + self.bucket_name, req.url.path)

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(200, resp.status_code)
            self.assertEqual('OK', resp.status_message)

    def test_list_object_v2(self):
        prefix = 'dir2/'
        subdir = prefix + 'subdir/'
        delimiter = '/'
        n = 10
        keys = []
        for i in range(n):
            keys.append(f'{prefix}test-list-objects-v2-{i}')
        for i in range(n):
            keys.append(f'{subdir}test-list-objects-v2-{i}')
        for key in keys:
            self.prepare_test_file(key, key)

        def run():
            resp1 = self.object_service.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                Delimiter=delimiter,
                MaxKeys=7,
            )

            self.assertEqual(self.bucket_name, resp1['Name'])
            self.assertEqual(prefix, resp1['Prefix'])
            self.assertEqual(delimiter, resp1['Delimiter'])
            self.assertEqual(7, resp1['MaxKeys'])
            print(resp1)
            self.assertGreater(len(resp1['NextContinuationToken']), 0)
            self.assertTrue(resp1['IsTruncated'])

            contents = resp1['Contents']
            self.assertEqual(6, len(contents))
            for content in contents:
                self.assertGreater(len(content['Key']), 0)
                self.assertGreater(len(content['ETag']), 0)
                self.assertIn(content['Key'], keys)
                self.assertIsNotNone(content['LastModified'])
                self.assertEqual('STANDARD', content['StorageClass'])

            common_prefixes = resp1['CommonPrefixes']
            self.assertEqual(1, len(common_prefixes))
            self.assertEqual(subdir, common_prefixes[0]['Prefix'])

            resp2 = self.object_service.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                Delimiter=delimiter,
                MaxKeys=7,
                ContinuationToken=resp1['NextContinuationToken'],
            )

            self.assertEqual(self.bucket_name, resp2['Name'])
            self.assertEqual(prefix, resp2['Prefix'])
            self.assertEqual(delimiter, resp2['Delimiter'])
            self.assertEqual(7, resp2['MaxKeys'])
            self.assertFalse(resp2['IsTruncated'])

            contents = resp2['Contents']
            self.assertEqual(4, len(contents))
            for content in contents:
                self.assertGreater(len(content['Key']), 0)
                self.assertGreater(len(content['ETag']), 0)
                self.assertIn(content['Key'], keys)
                self.assertIsNotNone(content['LastModified'])
                self.assertEqual('STANDARD', content['StorageClass'])

            try:
                common_prefixes = resp2['CommonPrefixes']
                self.assertEqual(0, len(common_prefixes))
            except KeyError as e:
                pass

        with self.vcr.use_cassette('test_list_object_v2.yaml') as cass:
            run()
            cu = CassetteUtils(cass)

            req = cu.request()
            self.check_public_request_header(req)
            self.assertEqual('GET', req.method)
            self.assertEqual('/' + self.bucket_name, req.url.path)

            resp = cu.response()
            self.check_public_response_header(resp)
            self.assertEqual(200, resp.status_code)
            self.assertEqual('OK', resp.status_message)
