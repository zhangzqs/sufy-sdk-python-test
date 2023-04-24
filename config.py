from dataclasses import dataclass
from typing import Dict, Any


@dataclass()
class AuthConfig:
    accessKey: str
    secretKey: str

    @staticmethod
    def from_dict(dict_: Dict[str, Any]):
        return AuthConfig(**dict_)


@dataclass()
class ObjectConfig:
    bucket: str
    region: str
    endpoint: str
    forcePathStyle: bool

    @staticmethod
    def from_dict(dict_: Dict[str, Any]):
        return ObjectConfig(**dict_)


@dataclass()
class ProxyConfig:
    enable: bool
    host: str
    port: int
    type: str

    @staticmethod
    def from_dict(dict_: Dict[str, Any]):
        return ProxyConfig(**dict_)


@dataclass()
class TestConfig:
    auth: AuthConfig
    object: ObjectConfig
    proxy: ProxyConfig

    @staticmethod
    def from_dict(dict_: Dict[str, Any]):
        return TestConfig(
            auth=AuthConfig.from_dict(dict_['auth']),
            object=ObjectConfig.from_dict(dict_['object']),
            proxy=ProxyConfig.from_dict(dict_['proxy']),
        )


__all__ = [
    'TestConfig',
    'AuthConfig',
    'ObjectConfig',
    'ProxyConfig',
]


def test_load_config_file():
    dic = {
        'auth': {
            'accessKey': '<accessKey>',
            'secretKey': '<secretKey>',
        },
        'object': {
            'bucket': '<bucket>',
            'region': '<region>',
            'endpoint': '<endpoint>',
            'forcePathStyle': True,
        },
        'proxy': {
            'enable': True,
            'host': '<host>',
            'port': 8080,
            'type': 'http',
        },
    }
    config = TestConfig.from_dict(dic)
    assert config.auth.accessKey == '<accessKey>'
    assert config.auth.secretKey == '<secretKey>'
    assert config.object.bucket == '<bucket>'
    assert config.object.region == '<region>'
    assert config.object.endpoint == '<endpoint>'
    assert config.object.forcePathStyle is True
    assert config.proxy.enable is True
    assert config.proxy.host == '<host>'
    assert config.proxy.port == 8080
    assert config.proxy.type == 'http'
