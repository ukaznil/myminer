import sys
import threading

from midnight.ashmaize import PyAshMaize, PyRom
from utils import assert_type

try:
    from midnight import ashmaize_loader
    ashmaize_py = ashmaize_loader.init()  # type: PyAshMaize
except ImportError:
    sys.exit(1)


class AshMaizeROMManager:
    _lock = threading.Lock()
    _cache = {}  # type: dict[str, PyRom]

    ROM_SIZE = 1_073_741_824

    @classmethod
    def get_rom(cls, key: str) -> PyRom:
        assert_type(key, str)

        with cls._lock:
            rom = cls._cache.get(key)
            if rom is None:
                rom = ashmaize_py.build_rom_twostep(key=key,
                                                    size=cls.ROM_SIZE,
                                                    pre_size=16_777_216,
                                                    mixing_numbers=4,
                                                    )
                cls._cache[key] = rom
            # endif
        # endwith

        return rom
    # enddef

    @classmethod
    def clear_all(cls):
        with cls._lock:
            cls._cache.clear()
        # endwith
    # enddef

    @classmethod
    def drop(cls, *keys: tuple[str]):
        assert_type(keys, tuple, str)

        with cls._lock:
            for key in keys:
                cls._cache.pop(key, None)
            # endfor
        # endwith
    # enddef

    @classmethod
    def keys(cls) -> tuple[str]:
        with cls._lock:
            return tuple(cls._cache.keys())
        # endwith
    # enddef

    @classmethod
    def status(cls) -> dict[str, int]:
        with cls._lock:
            return {key: cls.ROM_SIZE for key, rom in cls._cache.items()}
        # endwith
    # enddef
