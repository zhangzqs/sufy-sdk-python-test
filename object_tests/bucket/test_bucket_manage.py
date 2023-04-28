from object_tests.object_test_base import BaseObjectTest


class BucketManageTest(BaseObjectTest):
    def test_create_bucket(self):
        self.force_delete_bucket()

        def run():
            resp = self.object_service.create_bucket(
                bucket=self.bucket_name,
                location=self.test_config.object.region,
            )
