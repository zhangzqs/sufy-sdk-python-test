import json
from typing import Optional, Dict, Any, Union
from urllib.parse import urlparse, ParseResult

from vcr.cassette import Cassette
from vcr.request import Request


class RequestBody:
    def __init__(self, body: Union[bytes]):
        self.__body = body

    @property
    def as_json(self) -> Dict[str, Any]:
        return json.loads(self.as_str)

    @property
    def as_str(self) -> str:
        return self.__body.decode('utf-8')


class CassetteRequest:

    def __init__(self, request: Request):
        self.__request = request

    @property
    def method(self) -> str:
        return self.__request.method

    @property
    def body(self) -> Optional[RequestBody]:
        return self.__request.body

    def get_header_value(self, header: str) -> Optional[str]:
        for k, v in self.__request.headers.items():
            if k.lower() == header.lower():
                if isinstance(v, str):
                    return v
                if isinstance(v, bytes):
                    return v.decode('utf-8')
                raise Exception('Unknown type')
        return None

    @property
    def url(self) -> ParseResult:
        return urlparse(self.__request.url)


class ResponseBody:
    def __init__(self, body: Any):
        self.__body = body

    @property
    def as_str(self) -> str:
        try:
            return self.__body['string'].decode('utf-8')
        except TypeError:
            return self.__body.decode('utf-8')

    @property
    def as_json(self) -> Dict[str, Any]:
        return json.loads(self.as_str)


class CassetteResponse:
    def __init__(self, response: Dict[str, Any]):
        self.__response = response

    @property
    def status_code(self) -> int:
        return self.__response['status']['code']

    @property
    def status_message(self) -> str:
        return self.__response['status']['message']

    def get_header_value(self, header: str) -> Optional[str]:
        for k, v in self.__response['headers'].items():
            if k.lower() == header.lower():
                return v[0]
        return None

    @property
    def body(self) -> Optional[ResponseBody]:
        return ResponseBody(self.__response['body']['string'])


class CassetteUtils:
    def __init__(self, cassette: Cassette):
        self.__cassette = cassette

    def request(self, index: int = 0) -> CassetteRequest:
        return CassetteRequest(self.__cassette.requests[index])

    def response(self, index: int = 0) -> CassetteResponse:
        return CassetteResponse(self.__cassette.responses[index])


__all__ = [
    'CassetteUtils',
    'CassetteRequest',
    'CassetteResponse',
    'RequestBody',
    'ResponseBody',
]
