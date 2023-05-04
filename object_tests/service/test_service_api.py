from object_tests.object_test_base import BaseObjectTest


class ServiceApiTest(BaseObjectTest):
    def test_list_buckets(self):
        resp = self.object_service.list_buckets()
        buckets = resp['buckets']
        for bucket in buckets:
            self.assertIsNotNone(bucket['name'])
            self.assertIsNotNone(bucket['creationDate'])
            self.assertIsNotNone(bucket['locationConstraint'])

        owner = resp['owner']
        self.assertIn('id', owner.keys())
        self.assertIn('displayName', owner.keys())
