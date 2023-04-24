from object_tests.object_test_base import BaseObjectTest


class BucketManageTest(BaseObjectTest):
    def test_create_bucket(self):
        bucket_name = 'test-bucket-1'
        resp = self.object_service.create_bucket(
            bucket=bucket_name,
        )
