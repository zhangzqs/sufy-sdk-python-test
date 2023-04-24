from object_tests.object_test_base import BaseObjectTest


class ServiceApiTest(BaseObjectTest):
    def test_list_buckets(self):
        resp = self.object_service.list_buckets()
        buckets = resp['buckets']
        for bucket in buckets:
            self.assertIsNotNone(bucket['name'])
            self.assertIsNotNone(bucket['creationDate'])
            # TODO: 缺失字段
            # assert bucket['locationConstraint'] is not None
        owner = resp['owner']
        print(owner)
        # TODO: 找不到id
        # self.assertIn('id', owner.keys())
        self.assertIn('displayName', owner.keys())
