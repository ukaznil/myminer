import secrets
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

from challenge import Challenge
from logger import LogType, Logger, measure_time
from solution import Solution
from utils import assert_type

try:
    import ashmaize_loader
    ashmaize_py = ashmaize_loader.init()
except ImportError:
    ashmaize_py = None


class _RomManager:
    _lock = threading.Lock()
    _cache = {}

    ROM_SIZE = 1_073_741_824

    @classmethod
    def get_rom(cls, key: str):
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


@dataclass
class SolvingInfo:
    challenge: Challenge
    tries: int
    hashrate: float


@dataclass
class WorkingInfo:
    solving_info: Optional[SolvingInfo] = None
    best_batch_size: Optional[int] = None
    batch_size_search: dict[int, float] = field(default_factory=dict)

    def clear(self):
        self.solving_info = None
        self.best_batch_size = None
        self.batch_size_search.clear()
    # enddef


class AshMaizeMiner:
    def __init__(self, logger: Logger):
        self.random_buffer = bytearray(8192)
        self.random_buffer_pos = len(self.random_buffer)

        self.logger = logger

        self.dict__address__workinginfo = defaultdict(WorkingInfo)  # type: dict[str, WorkingInfo]
        self.preimage_base_cache = dict()

        self._stop_event = threading.Event()
    # enddef

    @measure_time
    def start(self):
        self._stop_event.clear()
    # enddef

    @measure_time
    def stop(self):
        self._stop_event.set()
    ##enddef

    # @measure_time
    def is_running(self) -> bool:
        return not self._stop_event.is_set()
    # enddef

    # @measure_time
    def get_fast_nonce(self) -> int:
        # return secrets.randbits(64)
        # return random.getrandbits(64)

        if self.random_buffer_pos >= len(self.random_buffer):
            self.random_buffer = bytearray(secrets.token_bytes(8192))
            self.random_buffer_pos = 0
        # endif

        nonce_bytes = self.random_buffer[self.random_buffer_pos:self.random_buffer_pos + 8]
        self.random_buffer_pos += 8
        nonce = int.from_bytes(nonce_bytes, 'big')

        return nonce
    # enddef

    @measure_time
    def maintain_rom_cache(self, list__challenge: list[Challenge]):
        assert_type(list__challenge, list, Challenge)

        set__key_needed = set([ch.no_pre_mine for ch in list__challenge])
        list__key_to_drop = [key for key in _RomManager.keys() if key not in set__key_needed]

        _RomManager.drop(*list__key_to_drop)
    # enddef

    @measure_time
    def release_rom_cache(self):
        _RomManager.clear_all()
    # enddef

    @measure_time
    def rom_cache_info(self) -> dict[str, int]:
        return _RomManager.status()
    # enddef

    @measure_time
    def mine(self, address: str, challenge: Challenge) -> Optional[Solution]:
        assert_type(challenge, Challenge)
        assert_type(address, str)

        rom = _RomManager.get_rom(challenge.no_pre_mine)
        difficulty_value = int(challenge.difficulty[:8], 16)
        workinfo = self.dict__address__workinginfo[address]
        workinfo.solving_info = SolvingInfo(challenge=challenge, tries=0, hashrate=None)

        list__batch_size = [10, 100, 1_000, 10_000, 100_000]

        # batch_size search
        for batch_size in list__batch_size:
            solution = self.mine_once_with_batch(address=address, challenge=challenge, rom=rom, difficulty_value=difficulty_value, batch_size=batch_size, is_search=True)

            if solution:
                workinfo.clear()

                return solution
            # endif
        # endfor

        # find the best_batch_size
        best_batch_size = max(workinfo.batch_size_search, key=workinfo.batch_size_search.get, default=None)
        workinfo.best_batch_size = batch_size

        msg = [
            '=== Batch-size Search ===',
            f'address: {address}',
            f'challenge: {challenge.challenge_id}',
            f'(batch-size, hashrate): {", ".join([f"({bs:,}, {hr:,.0f} H/s)" for bs, hr in workinfo.batch_size_search.items()])}',
            f'-> best batch-size = {best_batch_size:,}'
            ]
        self.logger.log('\n'.join(msg), log_type=LogType.Batch_Size_Search)

        # mine-loop with the best batch_size
        while self.is_running():
            solution = self.mine_once_with_batch(address=address, challenge=challenge, rom=rom, difficulty_value=difficulty_value, batch_size=best_batch_size, is_search=False)

            if solution:
                workinfo.clear()

                return solution
            # endif

            if not challenge.is_valid():
                self.logger.log('\n'.join([
                    f'=== Challenge Expire ===',
                    f'address: {address}',
                    f'challenge: {challenge.challenge_id}',
                    ]), log_type=LogType.Challenge_Expire, sufix=address)

                break
            # endif

            time.sleep(0.5)
        # endwhile

        return None
    # enddef

    @measure_time
    def mine_once_with_batch(self, address: str, challenge: Challenge, rom, difficulty_value: int, batch_size: int, is_search: bool) -> Optional[Solution]:
        assert_type(address, str)
        assert_type(challenge, Challenge)
        assert_type(difficulty_value, int)
        assert_type(batch_size, int)
        assert_type(is_search, bool)

        if not challenge.is_valid():
            return None
        # endif

        if (address, challenge.challenge_id) in self.preimage_base_cache.keys():
            preimage_base = self.preimage_base_cache[(address, challenge.challenge_id)]
        else:
            preimage_base = (
                    address
                    + challenge.challenge_id
                    + challenge.difficulty
                    + challenge.no_pre_mine
                    + challenge.latest_submission
                    + challenge.no_pre_mine_hour
            )
            self.preimage_base_cache[(address, challenge.challenge_id)] = preimage_base
        # endif

        time_start = time.time()

        preimages = [f'{self.get_fast_nonce():016x}' + preimage_base for _ in range(batch_size)]
        list__hash_hex = rom.hash_batch(preimages)
        for idx_hash_hex, hash_hex in enumerate(list__hash_hex):
            if self.meets_difficulty(hash_hex=hash_hex, difficulty_value=difficulty_value):
                nonce_hex = preimages[idx_hash_hex][:16]

                return Solution(nonce_hex=nonce_hex, hash_hex=hash_hex, tries=0)
            # endif
        # endfor

        time_end = time.time()
        time_elapse = time_end - time_start

        hashrate = batch_size / time_elapse

        workinfo = self.dict__address__workinginfo[address]
        if is_search:
            workinfo.batch_size_search[batch_size] = hashrate
        else:
            solving_info = workinfo.solving_info
            solving_info.tries += batch_size
            solving_info.hashrate = hashrate
        # endif

        return None
    # enddef

    # ------------------------------
    #  難易度チェック
    # ------------------------------
    # difficulty: 4 バイトのビットマスク (hex)
    # 「マスク中の 0bit の位置は、ハッシュの先頭 4 バイトでも 0 でなければならない」
    # → int 化して
    #     (hash_prefix & ~difficulty_mask) == 0
    # なら条件を満たす  [oai_citation:3‡45047878.fs1.hubspotusercontent-na1.net](https://45047878.fs1.hubspotusercontent-na1.net/hubfs/45047878/Midnight%20-%20Whitepaper%20treatment%20for%20Scavenger%20Mine%20API%20V3.pdf)
    @staticmethod
    def meets_difficulty(hash_hex: str, difficulty_value: int) -> bool:
        assert_type(hash_hex, str)
        assert_type(difficulty_value, int)

        hash_value = int(hash_hex[:8], 16)

        return (hash_value | difficulty_value) == difficulty_value
    # enddef
