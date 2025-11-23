from typing import *

import requests

from utils import assert_type


class MinerError(Exception):
    pass


class BaseMiner:
    base_url: str = NotImplemented

    def _get(self, path: str) -> dict:
        assert_type(path, str)

        url = self.base_url.rstrip('/') + '/' + path.lstrip('/')
        resp = requests.get(url, timeout=10)
        if not resp.ok:
            raise MinerError(f'GET {url} failed: {resp.status_code} {resp.text}')
        # endif

        try:
            return resp.json()
        except Exception:
            raise MinerError(f'GET {url} returned non-JSON body')
        # endtry
    # enddef

    def _post(self, path: str, data: Optional[dict]) -> dict:
        assert_type(path, str)

        url = self.base_url.rstrip('/') + '/' + path.lstrip('/')
        resp = requests.post(url, json=data or {}, timeout=10)
        if not resp.ok:
            raise MinerError(f'POST {url} failed: {resp.status_code} {resp.text}')
        # endif

        try:
            return resp.json()
        except Exception:
            raise MinerError(f'POST {url} returned non-JSON body')
        # endtry
    # enddef
