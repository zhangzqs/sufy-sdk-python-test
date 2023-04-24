from botocore.exceptions import ClientError

from object_tests.object_test_base import BaseObjectTest


class TestBucketAcl(BaseObjectTest):
    def test_put_bucket_acl(self):
        try:
            response = self.object_service.put_bucket_acl(
                bucket=self.get_bucket_name(),
            )
            self.fail('Expected ClientError to be raised')
        except ClientError as e:
            return


a = {
    'name': 'bucket test',
    'params': {
        'bucket_name': {'type': 'const', 'content': {'value': 'test-bucket'}},
        'access_key': {'type': 'external', 'content': {'name': 'access_key'}},
        'secret_key': {'type': 'env', 'content': {'env': 'SECRET_KEY'}},
    },
    'import': [
        'makeSureBucketExists',
        'clearAllFiles',
        'checkPublicRequestHeader',
        'checkPublicResponseHeader',
    ],
    'setUp': [
        'makeSureBucketExists',
        'clearAllFiles',
    ],
    'tearDown': [],
    'cases': {
        'putBucketAcl': {
            'params': [],
            'setUp': [],  # list of functions to call before test
            'tearDown': [],  # list of functions to call after test
            'input': [],  # list of functions to call to get input
            'expected': [],  # list of functions to call to get expected output
        }
    }
}
