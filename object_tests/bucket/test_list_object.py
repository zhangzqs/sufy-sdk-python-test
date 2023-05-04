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

            self.assertEqual(self.bucket_name, resp1['name'])
            self.assertEqual(prefix, resp1['prefix'])
            self.assertEqual(delimiter, resp1['delimiter'])
            self.assertEqual(7, resp1['maxKeys'])
            self.assertGreater(len(resp1['nextMarker']), 0)
            self.assertTrue(resp1['isTruncated'])

            contents = resp1['contents']
            self.assertEqual(6, len(contents))
            for content in contents:
                self.assertGreater(len(content['key']), 0)
                self.assertGreater(len(content['eTag']), 0)
                self.assertIn(content['key'], keys)
                self.assertIsNotNone(content['lastModified'])
                self.assertEqual('STANDARD', content['storageClass'])

            common_prefixes = resp1['commonPrefixes']
            self.assertEqual(1, len(common_prefixes))
            self.assertEqual(subdir, common_prefixes[0]['prefix'])

            resp2 = self.object_service.list_objects(
                Bucket=self.bucket_name,
                Prefix=prefix,
                Delimiter=delimiter,
                MaxKeys=7,
                Marker=resp1['nextMarker'],
            )

            self.assertEqual(self.bucket_name, resp2['name'])
            self.assertEqual(prefix, resp2['prefix'])
            self.assertEqual(delimiter, resp2['delimiter'])
            self.assertEqual(7, resp2['maxKeys'])
            self.assertFalse(resp2['isTruncated'])

            contents = resp2['contents']
            self.assertEqual(4, len(contents))
            for content in contents:
                self.assertGreater(len(content['key']), 0)
                self.assertGreater(len(content['eTag']), 0)
                self.assertIn(content['key'], keys)
                self.assertIsNotNone(content['lastModified'])
                self.assertEqual('STANDARD', content['storageClass'])

            try:
                common_prefixes = resp2['commonPrefixes']
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

            self.assertEqual(self.bucket_name, resp1['name'])
            self.assertEqual(prefix, resp1['prefix'])
            self.assertEqual(delimiter, resp1['delimiter'])
            self.assertEqual(7, resp1['maxKeys'])
            print(resp1)
            self.assertGreater(len(resp1['nextContinuationToken']), 0)
            self.assertTrue(resp1['isTruncated'])

            contents = resp1['contents']
            self.assertEqual(6, len(contents))
            for content in contents:
                self.assertGreater(len(content['key']), 0)
                self.assertGreater(len(content['eTag']), 0)
                self.assertIn(content['key'], keys)
                self.assertIsNotNone(content['lastModified'])
                self.assertEqual('STANDARD', content['storageClass'])

            common_prefixes = resp1['commonPrefixes']
            self.assertEqual(1, len(common_prefixes))
            self.assertEqual(subdir, common_prefixes[0]['prefix'])

            resp2 = self.object_service.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                Delimiter=delimiter,
                MaxKeys=7,
                ContinuationToken=resp1['nextContinuationToken'],
            )

            self.assertEqual(self.bucket_name, resp2['name'])
            self.assertEqual(prefix, resp2['prefix'])
            self.assertEqual(delimiter, resp2['delimiter'])
            self.assertEqual(7, resp2['maxKeys'])
            self.assertFalse(resp2['isTruncated'])

            contents = resp2['contents']
            self.assertEqual(4, len(contents))
            for content in contents:
                self.assertGreater(len(content['key']), 0)
                self.assertGreater(len(content['eTag']), 0)
                self.assertIn(content['key'], keys)
                self.assertIsNotNone(content['lastModified'])
                self.assertEqual('STANDARD', content['storageClass'])

            try:
                common_prefixes = resp2['commonPrefixes']
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
