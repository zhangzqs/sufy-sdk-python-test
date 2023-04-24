import logging
import unittest

import sufycore.session
import yaml

from config import TestConfig
from resources import test_config_file_path


class BaseObjectTest(unittest.TestCase):

    def setUp(self) -> None:
        with open(test_config_file_path) as f:
            test_config = TestConfig.from_dict(yaml.load(f, Loader=yaml.FullLoader))
            self.test_config = test_config

        logging.basicConfig(level=logging.DEBUG)

        self.session = sufycore.session.Session()

        self.object_service = self.session.create_client(
            service_name='object',
            sufy_access_key_id=test_config.auth.accessKey,
            sufy_secret_access_key=test_config.auth.secretKey,
            region_name=test_config.object.region,
            endpoint_url=test_config.object.endpoint,
        )

    def get_bucket_name(self):
        return self.test_config.object.bucket


__all__ = [
    'BaseObjectTest',
]