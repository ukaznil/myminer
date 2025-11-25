from enum import Enum

from utils import assert_type


class Project(Enum):
    Midnight = ('https://scavenger.prod.gd.midnighttge.io')
    Defensio = ('https://mine.defensio.io/api')

    def __init__(self, base_url: str):
        assert_type(base_url, str)

        self.base_url = base_url
    # enddef
