import secrets
import time

from challenge import Challenge
from solution import Solution

try:
    import ashmaize_loader
    ashmaize_py = ashmaize_loader.init()
except ImportError:
    ashmaize_py = None


class AshMaizeMiner:
    def __init__(self):
        self.rom_cache = {}
        self.random_buffer = bytearray(8192)
        self.random_buffer_pos = len(self.random_buffer)
    # enddef

    def get_fast_nonce(self) -> int:
        # return secrets.randbits(64)

        if self.random_buffer_pos >= len(self.random_buffer):
            self.random_buffer = bytearray(secrets.token_bytes(8192))
            self.random_buffer_pos = 0
        # endif

        nonce_bytes = self.random_buffer[self.random_buffer_pos:self.random_buffer_pos + 8]
        self.random_buffer_pos += 8
        nonce = int.from_bytes(nonce_bytes, 'big')

        return nonce
    # enddef

    def mine(self, challenge: Challenge, address: str, addr_short: str) -> Solution:
        if challenge.no_pre_mine not in self.rom_cache.keys():
            rom = ashmaize_py.build_rom_twostep(key=challenge.no_pre_mine,
                                                size=1_073_741_824,
                                                pre_size=16_777_216,
                                                mixing_numbers=4,
                                                )
            self.rom_cache[challenge.no_pre_mine] = rom
        # endif
        rom = self.rom_cache[challenge.no_pre_mine]

        NUM_BATCHES = 10_000
        preimage_base = self.build_preimage(address=address, challenge=challenge)
        time_start = time.time()
        last_display = None
        tries = 0
        while True:
            nonces = [self.get_fast_nonce() for _ in range(NUM_BATCHES)]
            preimages = [f'{nonce:016x}' + preimage_base for nonce in nonces]
            list__hash_hex = rom.hash_batch(preimages)

            for idx_nonce, hash_hex in enumerate(list__hash_hex):
                if self.meets_difficulty(hash_hex=hash_hex, difficulty_hex=challenge.difficulty):
                    nonce = nonces[idx_nonce]
                    nonce_hex = f'{nonce:016x}'
                    self.rom_cache.clear()

                    return Solution(nonce_hex=nonce_hex, hash_hex=hash_hex, tries=tries + idx_nonce + 1)
                # endif
            # endfor

            tries += NUM_BATCHES
            sec = int(time.time() - time_start)
            if ((sec - 1) % (60 * 5) == 0 and last_display != sec) or (tries % 100_000 == 0):
                hashrate = tries / (sec + 1e-9)
                print(f'=== [{addr_short}/{challenge.challenge_id}] Stats ===')
                print(f'Hashrate={hashrate:,.0f} H/s, tries={tries:,}')
                print()

                last_display = sec
            # endif

            # nonces = [(nonce + 1) & ((1 << 64) - 1) for nonce in nonces]
        # endwhile
    # enddef

    @staticmethod
    def build_preimage(address: str, challenge: Challenge) -> str:
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
    def meets_difficulty(hash_hex: bytes, difficulty_hex: str) -> bool:
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
