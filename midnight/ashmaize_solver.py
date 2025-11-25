import secrets
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

from logger import LogType, Logger, measure_time
from midnight.ashmaize_rom_manager import AshMaizeROMManager
from midnight.challenge import Challenge
from midnight.solution import Solution
from utils import assert_type


@dataclass
class JobStats:
    challenge: Challenge
    tries: int
    hashrate: float
    updated_at: float


@dataclass
class WorkerProfile:
    job_stats: Optional[JobStats] = None
    best_batch_size: Optional[int] = None
    batch_size_search: dict[int, list[float]] = field(default_factory=lambda: defaultdict(list))

    def clear(self):
        self.job_stats = None
        self.best_batch_size = None
        self.batch_size_search.clear()
    # enddef


class AshMaizeSolver:
    RANDOM_BUFFER_SIZE = 65_536

    def __init__(self, addr2nickname: dict[str, str], logger: Logger):
        self.addr2nickname = addr2nickname
        self.logger = logger

        # -------------------------
        # event handling
        # -------------------------
        self._stop_event = threading.Event()
        self.wp_by_address = defaultdict(WorkerProfile)  # type: dict[str, WorkerProfile]

        # -------------------------
        # generate nonces
        # -------------------------
        self.random_buffer = bytearray(self.RANDOM_BUFFER_SIZE)
        self.random_buffer_pos = len(self.random_buffer)
        self.preimage_base_cache = dict()
    # enddef

    # -------------------------
    # running
    # -------------------------
    @measure_time
    def start(self):
        self._stop_event.clear()
    # enddef

    @measure_time
    def stop(self):
        self._stop_event.set()
    ##enddef

    def is_running(self) -> bool:
        return not self._stop_event.is_set()
    # enddef

    # -------------------------
    # solve
    # -------------------------
    @measure_time
    def solve(self, address: str, challenge: Challenge) -> Optional[Solution]:
        assert_type(address, str)
        assert_type(challenge, Challenge)

        nickname = f'[{self.addr2nickname[address]}]'
        worker_profile = self.wp_by_address[address]
        worker_profile.job_stats = JobStats(challenge=challenge, tries=0, hashrate=None, updated_at=time.time())

        # -------------------------
        # pre compute:
        # ROM, preimage_base, difficuly_value
        # -------------------------
        key_cache = (address, challenge.challenge_id)
        preimage_base = self.preimage_base_cache.get(key_cache)
        if preimage_base is None:
            preimage_base = (
                    address
                    + challenge.challenge_id
                    + challenge.difficulty
                    + challenge.no_pre_mine
                    + challenge.latest_submission
                    + challenge.no_pre_mine_hour
            )
            self.preimage_base_cache[key_cache] = preimage_base
        # endif
        rom = AshMaizeROMManager.get_rom(challenge.no_pre_mine)
        difficulty_value = int(challenge.difficulty[:8], 16)

        # -------------------------
        # try to find a solution
        # -------------------------
        try:
            list__batch_size = [100, 500, 1_000, 5_000, 10_000]

            # -------------------------
            # search for the best batch-size
            # -------------------------
            for _ in range(3):
                for batch_size in list__batch_size:
                    if not challenge.is_valid():
                        break
                    # endif

                    solution = self.try_once_with_batch(worker_profile=worker_profile, preimage_base=preimage_base,
                                                        rom=rom, difficulty_value=difficulty_value, batch_size=batch_size,
                                                        is_search=True)

                    if solution:
                        return solution
                    # endif
                # endfor
            # endtry

            # -------------------------
            # choose the best batch-size
            # -------------------------
            avg_by_bs = {bs: (sum(scores) / len(scores)) for bs, scores in worker_profile.batch_size_search.items() if scores}
            best_batch_size = max(avg_by_bs, key=avg_by_bs.get, default=None)
            worker_profile.best_batch_size = best_batch_size

            msg = [
                f'=== {nickname} Batch-size Search ===',
                f'address: {address}',
                f'challenge: {challenge.challenge_id}',
                f'(batch-size, hashrate): {", ".join([f"({bs:,}, {hr:,.0f} H/s)" for bs, hr in avg_by_bs.items()])}',
                f'-> best batch-size = {best_batch_size:,} (~{avg_by_bs[best_batch_size]:,.0f} H/s) through {worker_profile.job_stats.tries:,} tries.'
                ]
            self.logger.log('\n'.join(msg), log_type=LogType.Batch_Size_Search, sufix=nickname)

            # -------------------------
            # find a solution
            # -------------------------
            while self.is_running():
                if not challenge.is_valid():
                    break
                # endif

                solution = self.try_once_with_batch(worker_profile=worker_profile, preimage_base=preimage_base,
                                                    rom=rom, difficulty_value=difficulty_value, batch_size=best_batch_size,
                                                    is_search=False)

                if solution:
                    return solution
                # endif
            # endwhile

            return None
        finally:
            worker_profile.clear()
        # endtry
    # enddef

    @measure_time
    def try_once_with_batch(self, worker_profile: WorkerProfile, preimage_base: str, rom, difficulty_value: int, batch_size: int, is_search: bool) -> Optional[Solution]:
        assert_type(difficulty_value, int)
        assert_type(batch_size, int)
        assert_type(is_search, bool)

        # -------------------------
        # prep
        # -------------------------
        job_stats = worker_profile.job_stats
        get_fast_nonce = self.get_fast_nonce
        meets_difficulty = self.meets_difficulty

        # -------------------------
        # hash compute
        # -------------------------
        time_start = time.time()

        preimages = [f'{get_fast_nonce():016x}' + preimage_base for _ in range(batch_size)]
        list__hash_hex = rom.hash_batch(preimages)
        for idx_hash_hex, hash_hex in enumerate(list__hash_hex):
            if meets_difficulty(hash_hex=hash_hex, difficulty_value=difficulty_value):
                nonce_hex = preimages[idx_hash_hex][:16]

                job_stats.tries += (idx_hash_hex + 1)
                job_stats.updated_at = time.time()

                return Solution(nonce_hex=nonce_hex, hash_hex=hash_hex, tries=worker_profile.job_stats.tries)
            # endif
        # endfor

        time_end = time.time()
        time_elapse = time_end - time_start

        # -------------------------
        # save the data
        # -------------------------
        job_stats.tries += batch_size
        job_stats.updated_at = time_end
        hashrate = batch_size / time_elapse
        if is_search:
            worker_profile.batch_size_search[batch_size].append(hashrate)
        # endif
        job_stats.hashrate = hashrate

        return None
    # enddef

    @staticmethod
    def meets_difficulty(hash_hex: str, difficulty_value: int) -> bool:
        assert_type(hash_hex, str)
        assert_type(difficulty_value, int)

        hash_value = int(hash_hex[:8], 16)

        return (hash_value | difficulty_value) == difficulty_value
    # enddef

    def get_fast_nonce(self) -> int:
        # return secrets.randbits(64)
        # return random.getrandbits(64)

        if self.random_buffer_pos >= len(self.random_buffer):
            self.random_buffer = bytearray(secrets.token_bytes(self.RANDOM_BUFFER_SIZE))
            self.random_buffer_pos = 0
        # endif

        nonce_bytes = self.random_buffer[self.random_buffer_pos:self.random_buffer_pos + 8]
        self.random_buffer_pos += 8
        nonce = int.from_bytes(nonce_bytes, 'big')

        return nonce
    # enddef
