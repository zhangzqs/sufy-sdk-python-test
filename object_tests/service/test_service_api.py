from object_tests.object_test_base import BaseObjectTest


class ServiceApiTest(BaseObjectTest):
    def test_list_buckets(self):
        resp = self.object_service.list_buckets()
        buckets = resp['Buckets']
        for bucket in buckets:
            self.assertIsNotNone(bucket['Name'])
            self.assertIsNotNone(bucket['CreationDate'])
            # self.assertIsNotNone(bucket['LocationConstraint'])

        owner = resp['Owner']
        self.assertIn('ID', owner.keys())
        self.assertIn('DisplayName', owner.keys())
