#!/usr/bin/env python3

import logging

import sufycore.session
import yaml

from config import TestConfig

with open('resources/test-config.yaml') as f:
    testConfig = TestConfig.from_dict(yaml.load(f, Loader=yaml.FullLoader))

logging.basicConfig(level=logging.DEBUG)

session = sufycore.session.Session()

objectService = session.create_client(
    service_name='object',
    sufy_access_key_id=testConfig.auth.accessKey,
    sufy_secret_access_key=testConfig.auth.secretKey,
    region_name=testConfig.object.region,
    endpoint_url=testConfig.object.endpoint,
)

response = objectService.head_bucket(
    bucket=testConfig.object.bucket,
)

print('create bucket response:', response)
