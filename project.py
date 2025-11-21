from enum import Enum


class Project(Enum):
    MidNight = ('https://scavenger.prod.gd.midnighttge.io')
    Defensio = ('https://mine.defensio.io/api')

    def __init__(self, base_url):
        self.base_url = base_url
    # enddef
