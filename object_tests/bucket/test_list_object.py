from object_tests.object_test_base import BaseObjectTest


class ListObjectTest(BaseObjectTest):
    def test_list_a(self):
        with self.vcr.use_cassette('test_list_objects_cass.yaml') as cass:
            response = self.object_service.list_objects(
                bucket=self.get_bucket_name(),
            )
            # print(response)
            print(cass.requests[0])
            # print(cass.responses)
            # print(cass.play_count)

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

        resp = self.object_service.list_objects(
            bucket=self.get_bucket_name(),
            prefix=prefix,
            delimiter=delimiter,
            maxKeys=7,
        )

        self.assertEqual(self.get_bucket_name(), resp['name'])
        self.assertEqual(prefix, resp['prefix'])
        self.assertEqual(delimiter, resp['delimiter'])
        self.assertEqual(7, resp['maxKeys'])
        self.assertGreater(len(resp['nextMarker']), 0)
        self.assertTrue(resp['isTruncated'])

        contents = resp['contents']
        self.assertEqual(6, len(contents))
        for content in contents:
            self.assertGreater(len(content['key']), 0)
            self.assertGreater(len(content['eTag']), 0)
            self.assertIn(content['key'], keys)
            self.assertIsNotNone(content['lastModified'])
            self.assertEqual('STANDARD', content['storageClass'])

        common_prefixes = resp['commonPrefixes']
        self.assertEqual(1, len(common_prefixes))
        self.assertEqual(subdir, common_prefixes[0]['prefix'])

        resp = self.object_service.list_objects(
            bucket=self.get_bucket_name(),
            prefix=prefix,
            delimiter=delimiter,
            maxKeys=7,
            marker=resp['nextMarker'],
        )

        self.assertEqual(self.get_bucket_name(), resp['name'])
        self.assertEqual(prefix, resp['prefix'])
        self.assertEqual(delimiter, resp['delimiter'])
        self.assertEqual(7, resp['maxKeys'])
        self.assertFalse(resp['isTruncated'])

        contents = resp['contents']
        self.assertEqual(4, len(contents))
        for content in contents:
            self.assertGreater(len(content['key']), 0)
            self.assertGreater(len(content['eTag']), 0)
            self.assertIn(content['key'], keys)
            self.assertIsNotNone(content['lastModified'])
            self.assertEqual('STANDARD', content['storageClass'])

        try:
            common_prefixes = resp['commonPrefixes']
            self.assertEqual(0, len(common_prefixes))
        except KeyError as e:
            pass
