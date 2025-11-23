import random
import threading
import time
from typing import Optional

from challenge import Challenge
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


class AshMaizeMiner:
    def __init__(self):
        # self.random_buffer = bytearray(8192)
        # self.random_buffer_pos = len(self.random_buffer)

        self._hashrate = dict()
        self._tries = dict()
        self._challenge = dict()

        self._stop_event = threading.Event()
    # enddef

    def start(self):
        self._stop_event.clear()
    # enddef

    def stop(self):
        self._stop_event.set()
    ##enddef

    def is_running(self) -> bool:
        return not self._stop_event.is_set()
    # enddef

    def get_fast_nonce(self) -> int:
        # return secrets.randbits(64)
        return random.getrandbits(64)

        # if self.random_buffer_pos >= len(self.random_buffer):
        #     self.random_buffer = bytearray(secrets.token_bytes(8192))
        #     self.random_buffer_pos = 0
        # # endif
        #
        # nonce_bytes = self.random_buffer[self.random_buffer_pos:self.random_buffer_pos + 8]
        # self.random_buffer_pos += 8
        # nonce = int.from_bytes(nonce_bytes, 'big')
        #
        # return nonce
    # enddef

    def get_hashrate(self, address: str) -> Optional[float]:
        assert_type(address, str)

        return self._hashrate.get(address, None)
    # enndef

    def get_tries(self, address: str) -> Optional[int]:
        assert_type(address, str)

        return self._tries.get(address, None)
    # enddef

    def get_challenge(self, address: str) -> Optional[Challenge]:
        assert_type(address, str)

        return self._challenge.get(address, None)
    # enddef

    def maintain_rom_cache(self, list__challenge: list[Challenge]):
        assert_type(list__challenge, list, Challenge)

        set__key_needed = set([ch.no_pre_mine for ch in list__challenge])
        list__key_to_drop = [key for key in _RomManager.keys() if key not in set__key_needed]

        _RomManager.drop(*list__key_to_drop)
    # enddef

    def release_rom_cache(self):
        _RomManager.clear_all()
    # enddef

    def rom_cache_info(self) -> dict[str, int]:
        return _RomManager.status()
    # enddef

    def mine(self, challenge: Challenge, address: str) -> Optional[Solution]:
        assert_type(challenge, Challenge)
        assert_type(address, str)

        rom = _RomManager.get_rom(challenge.no_pre_mine)

        NUM_BATCHES = 10_000
        preimage_base = self.build_preimage(address=address, challenge=challenge)
        time_start = time.time()
        last_display = 0
        tries = 0
        while self.is_running() and challenge.is_valid():
            nonces = [self.get_fast_nonce() for _ in range(NUM_BATCHES)]
            preimages = [f'{nonce:016x}' + preimage_base for nonce in nonces]
            list__hash_hex = rom.hash_batch(preimages)

            for idx_nonce, hash_hex in enumerate(list__hash_hex):
                if self.meets_difficulty(hash_hex=hash_hex, difficulty_hex=challenge.difficulty):
                    nonce = nonces[idx_nonce]
                    nonce_hex = f'{nonce:016x}'

                    return Solution(nonce_hex=nonce_hex, hash_hex=hash_hex, tries=tries + idx_nonce + 1)
                # endif
            # endfor

            tries += NUM_BATCHES
            now = time.time()
            if now - last_display > 60 * 5 or tries % 100_000 == 0:
                self._hashrate[address] = tries / (now - time_start)
                self._tries[address] = tries
                self._challenge[address] = challenge

                last_display = now
            # endif

            time.sleep(0.5)
        # endwhile

        return None
    # enddef

    @staticmethod
    def build_preimage(address: str, challenge: Challenge) -> str:
        assert_type(address, str)
        assert_type(challenge, Challenge)

        preimage_str = (
                address
                + challenge.challenge_id
                + challenge.difficulty
                + challenge.no_pre_mine
                + challenge.latest_submission
                + challenge.no_pre_mine_hour
        )

        return preimage_str
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
    def meets_difficulty(hash_hex: str, difficulty_hex: str) -> bool:
        assert_type(hash_hex, str)
        assert_type(difficulty_hex, str)

        hash_value = int(hash_hex[:8], 16)
        difficulty_value = int(difficulty_hex[:8], 16)

        return (hash_value | difficulty_value) == difficulty_value
    # enddef


def demo_mine():
    """
    テスト用：固定値の challenge でマイニングしてみるデモ。
    実際には Defensio / Midnight API から challenge を取得して
    parse_challenge_from_api_json() に渡してください。
    """
    # ここはダミー。実戦では API から取る
    challenge = Challenge({
        'challenge_id': "**D21C10",
        'difficulty': "0000FFFF",
        'no_pre_mine': "cddba7b592e3133393c16194fac7431abf2f5485ed711db282183c819e08ebaa",
        'no_pre_mine_hour': "548571128",
        'latest_submission': "2025-11-20T23:59:59Z",
        })

    address = "addr_test1qq4dl3nhr0axurgcrpun9xyp04pd2r2dwu5x7eeam98psv6dhxlde8ucclv2p46hm077ds4vzelf5565fg3ky794uhrq5up0he"

    print('=== Challenge ===')
    print(challenge)

    print("Start mining with AshMaize...")
    miner = AshMaizeMiner()
    solution = miner.mine(
        challenge=challenge,
        address=address,
        )
    print("=== SOLUTION FOUND ===")
    print(f'solution = {solution}')


if __name__ == "__main__":
    demo_mine()
