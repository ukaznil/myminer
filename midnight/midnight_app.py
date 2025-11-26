import sys
import threading
import time
from typing import *

from base_app import BaseApp
from logger import LogType, Logger, measure_time
from midnight.ashmaize_rom_manager import AshMaizeROMManager
from midnight.ashmaize_solver import AshMaizeSolver
from midnight.challenge import Challenge
from midnight.solution import Solution
from midnight.tracker import SolutionStatus, Tracker
from project import Project
from system_metrics import SystemMetrics
from utils import assert_type, async_run_func, print_with_time, safefstr, timestamp_to_str


class MidnightApp(BaseApp):
    def __init__(self, project: Project):
        self.project = project
        self.base_url = self.project.base_url
        self.logger = Logger(project=self.project)
        self.tracker = Tracker(project=self.project, logger=self.logger)

        # workers
        self.list__address = self.tracker.get_wallets()
        self.nickname_of_address = {address: f'ADDR-#{idx_addr}' for idx_addr, address in enumerate(self.list__address)}

        # solver
        self.solver = AshMaizeSolver(addr2nickname=self.nickname_of_address, logger=self.logger)
        self.address_active_events = dict()  # type: dict[str, threading.Event]
    # enddef

    # -------------------------
    # called from CLI
    # -------------------------
    @measure_time
    def handle_register(self, address: str):
        assert_type(address, str)

        # -------------------------
        # Terms & Conditions
        # -------------------------
        data = self.get_tandc()
        print_with_time('\n'.join([
            f'== Terms and Conditions ===',
            f'Version: {data.get("version")}',
            f'{data.get("content", "")}',
            '',
            f'=== Message to sign (wallet CIP-30)',
            f'{data.get("message", "")}',
            ]))

        # -------------------------
        # register
        # -------------------------
        signature = input('Signature: ')
        pubkey = input('Public Key: ')
        resp = self.register_address(address=address, signature=signature, pubkey=pubkey)
        print_with_time('\n'.join([
            f'=== Registration response ===',
            f'{resp}',
            ]))

        # save
        if 'registrationReceipt' in resp.keys():
            if self.tracker.add_wallet(address):
                print(f'-> Saved a new wallet. address={address}')
            else:
                print(f'-> This wallet already exists. Skipped. address={address}')
            # endif
        else:
            print(f'-> Failed to register. address={address}')
        # endif
    # enddef

    @measure_time
    def handle_list_wallets(self):
        msg = ['=== Wallet List ===']

        sum_receipts = 0
        sum_allocation = 0
        for address in self.list__address:
            nickname = f'[{self.nickname_of_address[address]}]'

            resp = self.get_statistics(address=address)
            time.sleep(0.1)

            donation_address = resp['local_with_donate']['donation_address']
            receipts = resp['local']['crypto_receipts']
            sum_receipts += receipts
            if self.project == Project.Midnight:
                allocation = None
            elif self.project == Project.Defensio:
                allocation = resp['local']['dfo_allocation'] / 1_000_000
                sum_allocation += allocation
            # endif

            is_mine = self.nickname_of_address[donation_address] if donation_address in self.list__address else False

            msg.append(f'{nickname}')
            msg.append(f'- address     : {address}')
            msg.append(f'- donated_to  : {donation_address}')
            msg.append(f'  - is owned? : {is_mine}' + (' (self)' if address == donation_address else ''))
            msg.append(f'- receipts    : {receipts}')
            msg.append(f'- allocation  : {safefstr(allocation, ",")}')
        # endfor

        # sum
        msg.append('-' * 21)
        msg.append(f'- receipts    : {sum_receipts:,}')
        msg.append(f'- allocation  : {sum_allocation:,}')

        self.logger.log('\n'.join(msg), log_type=LogType.Wallet_List)
    # enddef

    @measure_time
    def handle_donate(self, address: str, to: str):
        assert_type(address, str)
        assert_type(to, str)

        if address == to:
            print(f'Cannot be donated to itself. Skipped.')

            return
        # endif

        if address not in self.list__address:
            raise ValueError(f'Given address={address} is not included in registered wallets.')
        # endif

        nickname = f'[{self.nickname_of_address[address]}]'
        message_to_sign = f'Assign accumulated Scavenger rights to: {to}'
        print_with_time('\n'.join([
            f'{nickname} {address}',
            f'=== Message to sign (wallet CIP-30) ===',
            f'{message_to_sign}'
            ]))
        signature = input(f'Signature: ')

        try:
            resp = self.donate_to(destionation_address=to, original_address=address, signature=signature)
            status = resp.get('status')
            msg = [
                '=== Donation Response ===',
                f'{resp}'
                ]
            if status == 'success':
                msg.append(f'-> Donation Validated !!!')
            else:
                status_code = resp.get('statusCode')
                error = resp.get('error')
                message = resp.get('message')

                msg.append(f'-> Donation Invalid. code={status_code}, error={error}, message={message}')
            # endif

            self.logger.log('\n'.join(msg), log_type=LogType.Donate_To, sufix=nickname)
        except Exception as e:
            self.logger.log('\n'.join([
                f'=== {nickname} Donation Error ===',
                f'address    : {address}',
                f'donated_to : {to}',
                f'error      : {e}'
                ]), log_type=LogType.Donate_To_Error, sufix=nickname)
        # endtry
    # enddef

    @measure_time
    def handle_donate_all(self, to: str):
        assert_type(to, str)

        for address in self.list__address:
            self.handle_donate(address=address, to=to)
        # endfor

        self.handle_list_wallets()
    # enddef

    @measure_time
    def handle_mine(self, num_threads: Optional[int]):
        try:
            # -------------------------
            # prepare threads
            # -------------------------
            threads = [threading.Thread(target=self.input_loop, daemon=True)]
            for address in self.list__address:
                run_event = threading.Event()
                if num_threads:
                    run_event.clear()  # stop
                else:
                    run_event.set()  # run
                # endif
                self.address_active_events[address] = run_event

                threads.append(threading.Thread(
                    target=self.mine_loop,
                    args=(address, num_threads),
                    daemon=True,
                    ))
            # endfor

            # -------------------------
            # start mining !!
            # -------------------------
            self.set_active_addresses(num_threads=num_threads)
            self.solver.start()
            for thread in threads:
                thread.start()
            # endfor
            time.sleep(3)

            # -------------------------
            # interactive commands
            # -------------------------
            now = time.time()
            last_retrieve_new_challenge = 0
            last_show_worklist = 0
            last_show_hashrate = now
            last_maintain_cache = now
            while self.solver.is_running():
                now = time.time()

                if now - last_retrieve_new_challenge > 60 * 1:
                    async_run_func(self.retrieve_new_challenge)
                    last_retrieve_new_challenge = now
                # endif

                if now - last_show_worklist > 60 * 20:
                    async_run_func(self.show_worklist)
                    last_show_worklist = now
                # endif

                if now - last_show_hashrate > 60 * 10:
                    async_run_func(self.show_hashrate)
                    last_show_hashrate = now
                # endif

                if now - last_maintain_cache > 60 * 30:
                    async_run_func(self.maintain_rom_cache)
                    last_maintain_cache = now
                # endif

                time.sleep(0.5)
            # endwhile

            self.logger.log('=== Miner Stopped ===', log_type=LogType.System)
        finally:
            self.tracker.close()
        # endtry
    # enddef

    # -------------------------
    # API
    # -------------------------
    @measure_time
    def get_tandc(self) -> dict:
        """
        GET /TandC

        Returns:
            {
              "version": "1-0",
              "content": "... terms ...",
              "message": "I agree to ... <hash>"
            }
        """
        if self.project == Project.Midnight:
            return self._get('TandC')
        elif self.project == Project.Defensio:
            return {
                'message': 'I agree to abide by the terms and conditions as described in version 1-0 of the Defensio DFO mining process: 2da58cd94d6ccf3d933c4a55ebc720ba03b829b84033b4844aafc36828477cc0',
                }
        else:
            raise NotImplementedError(self.project)
        # endif
    # enddef

    @measure_time
    def register_address(self, address: str, signature: str, pubkey: str) -> dict:
        """
        POST /register/{address}/{signature}/{pubkey}

        All params are passed in the URL path, body is {}.
        """
        assert_type(address, str)
        assert_type(signature, str)
        assert_type(pubkey, str)

        path = f'register/{address}/{signature}/{pubkey}'

        return self._post(path, {})
    # enddef

    @measure_time
    def get_statistics(self, address: str) -> dict:
        assert_type(address, str)

        path = f'statistics/{address}'

        return self._get(path)
    # enddef

    @measure_time
    def donate_to(self, destionation_address: str, original_address: str, signature: str) -> dict:
        assert_type(destionation_address, str)
        assert_type(original_address, str)
        assert_type(signature, str)

        path = f'donate_to/{destionation_address}/{original_address}/{signature}'

        return self._post(path, {})
    # enddef

    @measure_time
    def get_challenge(self) -> dict:
        """
        GET /challenge

        Returns an object like:
            {
              "code": "active" | "before" | "after",
              "challenge": {...},
              "mining_period_ends": "...",
              ...
            }
        """

        return self._get('challenge')
    # enddef

    @measure_time
    def submit_solution(self, address: str, challenge: Challenge, solution: Solution) -> dict:
        """
        POST /solution/{address}/{challenge_id}/{nonce}

        Body is {}.
        """
        assert_type(address, str)
        assert_type(challenge, Challenge)
        assert_type(solution, Solution)

        path = f'/solution/{address}/{challenge.challenge_id}/{solution.nonce_hex}'

        return self._post(path, {})
    # enddef

    # -------------------------
    # mine / helper
    # -------------------------
    @measure_time
    def retrieve_new_challenge(self) -> None:
        try:
            challenge_resp = self.get_challenge()
        except Exception as e:
            self.logger.log('\n'.join([
                f'=== Fetch a new Challenge: Error ===',
                f'error: {e}'
                ]), log_type=LogType.Fetch_New_Challenge_Error, stdout=False)

            return
        # endtry

        if self.project == Project.Defensio:
            code = 'active'
        else:
            code = challenge_resp.get('code')
        # endif

        if code != 'active':
            print(f'/challenge code = {code}')
            if code == 'before':
                print('Mining has not started yet.')
            elif code == 'after':
                print('Mining has ended.')
            else:
                raise NotImplementedError(code)
            # endif

            return
        # endif

        challenge = challenge_resp.get('challenge', {})
        if challenge:
            challenge = Challenge.from_dict(challenge)

            # save
            if self.tracker.add_challenge(challenge):
                self.logger.log('\n'.join([
                    '=== New Challenge ===',
                    f'{challenge}',
                    ]), log_type=LogType.Fetch_New_Challenge)
            else:
                pass
            # endif
        # endif
    # enddef

    @measure_time
    def solve_challenge(self, address: str, challenge: Challenge) -> None:
        assert_type(address, str)
        assert_type(challenge, Challenge)

        nickname = f'[{self.nickname_of_address[address]}]'

        self.logger.log('\n'.join([
            f'=== {nickname} Start this Challenge ===',
            f'address: {address}',
            f'{challenge}',
            ]), log_type=LogType.Start_New_Challenge, sufix=nickname)

        # -------------------------
        # Find a solution
        # -------------------------
        solution = self.tracker.get_found_solution(address=address, challenge=challenge)
        is_solutoin_cached = (solution is not None)
        if not is_solutoin_cached:
            solution = self.solver.solve(address=address, challenge=challenge)

            if not challenge.is_valid():
                self.logger.log('\n'.join([
                    f'=== {nickname} Challenge Expired ===',
                    f'address   : {address}',
                    f'challenge : {challenge.challenge_id}',
                    ]), log_type=LogType.Challenge_Expired, sufix=nickname)
            # endif

            if solution is None:
                return
            # endif

            self.tracker.add_solution_found(address=address, challenge=challenge, solution=solution)
        # endif

        self.logger.log('\n'.join([
            f'=== {nickname} {"Cached-solution" if is_solutoin_cached else "Solution"} Found ===',
            f'address   : {address}',
            f'challenge : {challenge.challenge_id}',
            f'solution  : {solution}',
            ]), log_type=LogType.Solution_Found, sufix=nickname)

        if not self.solver.is_running():
            return
        # endif

        # -------------------------
        # Submit the solution
        # -------------------------
        try:
            resp = self.submit_solution(address=address, challenge=challenge, solution=solution)

            msg = [
                f'=== {nickname} Solution Submission Response ===',
                f'{resp}',
                ]

            if 'crypto_receipt' in resp.keys():
                self.tracker.update_solution_submission_result(address=address, challenge=challenge, solution=solution, validated=True)

                msg.append(f'-> Solution Validated !!!')
            else:
                self.tracker.update_solution_submission_result(address=address, challenge=challenge, solution=solution, validated=False)

                code = resp.get('statusCode')
                message = resp.get('message')
                msg.append(f'-> Solution Invalid. code={code}, message={message}')
            # endif

            self.logger.log('\n'.join(msg), log_type=LogType.Solution_Submission, sufix=nickname)
        except Exception as e:
            self.logger.log('\n'.join([
                f'=== {nickname} Solution Submission Error ===',
                f'address   : {address}',
                f'challenge : {challenge.challenge_id}',
                f'solution  : {solution}',
                f'error     : {e}'
                ]), log_type=LogType.Solution_Submission_Error, sufix=nickname)

            time.sleep(1)

            return
        # endtry
    # enddef

    @measure_time
    def pause_solver(self, address: str):
        ev = self.address_active_events.get(address)
        if ev is not None:
            ev.clear()
        # endif
    # enddef

    @measure_time
    def resume_solver(self, address: str):
        ev = self.address_active_events.get(address)
        if ev is not None:
            ev.set()
        # endif
    # enddef

    @measure_time
    def set_active_addresses(self, num_threads: Optional[int]):
        if num_threads is None:
            return
        # endif

        msg = [f'=== Active Addresses (<= {num_threads}) ===']

        counts = {addr: len(self.tracker.get_challenges(address=addr, list__status=[SolutionStatus.Found, SolutionStatus.Invalid])) for addr in self.list__address}
        list_active_address = [addr for addr, _ in sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[:num_threads]]

        changed = False
        for address in self.list__address:
            ev = self.address_active_events[address]
            is_active = address in list_active_address

            if is_active and not ev.is_set():
                ev.set()
                changed = True
            elif not is_active and ev.is_set():
                ev.clear()
                changed = True
            # endif

            nickname = f'[{self.nickname_of_address[address]}]'
            msg.append(f'{nickname}: {"*active*" if is_active else ""}')
        # endfor

        if changed:
            self.logger.log('\n'.join(msg), log_type=LogType.Active_Addresses)
        # endif
    # enddef

    @measure_time
    def mine_loop(self, address: str, num_threads: Optional[int]):
        assert_type(address, str)
        assert_type(num_threads, int, allow_none=True)

        address_active_event = self.address_active_events[address]
        while self.solver.is_running():
            address_active_event.wait()  # run when 'set'; stop when 'clear'

            challenge = self.tracker.get_oldest_unsolved_challenge(address)

            if challenge is None:
                time.sleep(10)
            else:
                self.solve_challenge(address=address, challenge=challenge)
                self.set_active_addresses(num_threads=num_threads)
            # endif

            time.sleep(0.5)
        # endwhile
    # enddef

    # -------------------------
    # interactive commands
    # -------------------------
    @measure_time
    def input_loop(self):
        for line in sys.stdin:
            cmd = line.strip().lower()

            if cmd == 'w':
                async_run_func(self.show_worklist)
            elif cmd == 'h':
                async_run_func(self.show_hashrate)
            elif cmd == 's':
                async_run_func(self.show_statistics)
            elif cmd == 'm':
                async_run_func(self.show_system_metrics)
            elif cmd == 'r':
                async_run_func(self.show_rom_cache_status)
            elif cmd == 'q':
                self.logger.log('=== Stopping miner... ===', log_type=LogType.System)
                self.solver.stop()
                break
            else:
                print(f"Invalid command: '{cmd}'. Available: [W]orklist | [H]ashrate | [S]tatistics | System [M]etrics | [R]OM Cache | [Q]uit")
            # endif
        # endfor
    # enddef

    @measure_time
    def show_worklist(self):
        msg = ['=== [W]orklist ===']

        for idx_address, address in enumerate(self.list__address):
            msg.append(f'[{self.nickname_of_address[address]}] {address}')

            list__challenge = self.tracker.get_challenges(address=address, list__status=[ss for ss in SolutionStatus if ss != SolutionStatus.Validated])
            worker_profile = self.solver.wp_by_address[address]
            job_stats = worker_profile.job_stats
            if job_stats:
                solving_challenge = job_stats.challenge
            else:
                solving_challenge = None
            # endif

            if len(list__challenge) > 0:
                for challenge in list__challenge:
                    msg_info = [f'challenge={challenge.challenge_id}']

                    if solving_challenge and challenge.challenge_id == solving_challenge.challenge_id:
                        mark = '*'
                        msg_info.append(f'hashrate={safefstr(job_stats.hashrate, ",.0f")} H/s')
                        msg_info.append(f'tries={job_stats.tries:,}')
                        msg_info.append(f'batch_size={safefstr(worker_profile.best_batch_size, ",")} (at {timestamp_to_str(job_stats.updated_at)})')
                    else:
                        mark = ' '
                    # endif

                    msg.append(f'- [{mark}] {" | ".join(msg_info)}')
                # endfor
            else:
                msg.append(f'- None')
            # endif
        # endfor

        self.logger.log('\n'.join(msg), log_type=LogType.Worklist)
    # enddef

    @measure_time
    def show_hashrate(self):
        msg = ['=== Hashrate ===']

        list__hashrate = []
        for address in self.list__address:
            nickname = f'[{self.nickname_of_address[address]}]'

            work_profile = self.solver.wp_by_address[address]
            job_stats = work_profile.job_stats
            if job_stats:
                solving_challenge = job_stats.challenge
                hashrate = job_stats.hashrate
                tries = job_stats.tries
                updated_at = timestamp_to_str(job_stats.updated_at)

                if hashrate:
                    list__hashrate.append(hashrate)
                # endif

                msg.append(f'{nickname} challenge={solving_challenge.challenge_id} | {safefstr(hashrate, "7,.0f")} H/s | {tries:10,.0f} tries (at {updated_at})')
            # endif
        # endfor

        if list__hashrate:
            hashrate_sum = sum(list__hashrate)
            hashrate_avg = hashrate_sum / len(list__hashrate)
            hashrate_max = max(list__hashrate)
            hashrate_min = min(list__hashrate)

            msg.append(f'-' * 21)
            msg.append(f'sum: {hashrate_sum:,.0f} H/s')
            msg.append(f'avg: {hashrate_avg:,.0f} H/s | max: {hashrate_max:,.0f} H/s | min: {hashrate_min:,.0f} H/s')
        # endif

        self.logger.log('\n'.join(msg), log_type=LogType.Hashrate)
    # enddef

    @measure_time
    def show_statistics(self):
        msg = [f'=== [S]tatistics ===']

        for address in self.list__address:
            try:
                resp = self.get_statistics(address)
                time.sleep(0.5)

                receipts = resp['local']['crypto_receipts']
                if self.project == Project.Midnight:
                    raise NotImplementedError
                elif self.project == Project.Defensio:
                    allocation = resp['local']['dfo_allocation'] / 1_000_000
                # endif
                msg.append(f'[{self.nickname_of_address[address]}] receipts={receipts:,}, allocation={allocation:,}')
            except:
                msg.append(f'[{self.nickname_of_address[address]}] Error')
            # endtry
        # endfor

        self.logger.log('\n'.join(msg), log_type=LogType.Statistics)
    # enddef

    @measure_time
    def show_system_metrics(self):
        sm = SystemMetrics.init()

        msg = [
            '=== System [M]etrics ===',
            f'memory total     : {sm.memory_total_gb:,.2f} GiB',
            f'memory used      : {sm.memory_used_gb:,.2f} GiB ({sm.memory_used_percent:.1f} %)',
            f'memory available : {sm.memory_availale_gb:,.2f} GiB',
            f'memory free      : {sm.memory_free_gb:,.2f} GiB',
            f'CPU num          : {sm.cpu_num} ({sm.threads_running} threads running)',
            f'CPU usage        : {sm.cpu_usage_percent:.1f} %',
            ]

        # CPUクロック
        if sm.cpu_freq_mhz is not None:
            msg.append(f'CPU freq         : {sm.cpu_freq_mhz:,.0f} MHz')
        # endif

        # CPU温度
        if sm.cpu_temp_c is not None:
            msg.append(f'CPU temp         : {sm.cpu_temp_c:.1f} °C')
        # endif

        # GPU使用率
        if getattr(sm, 'gpu_usage_percent', None) is not None:
            msg.append(f'GPU usage        : {sm.gpu_usage_percent:.1f} %')
        # endif

        # GPUメモリ
        if getattr(sm, 'gpu_mem_used_gb', None) is not None and getattr(sm, 'gpu_mem_total_gb', None) is not None:
            msg.append(f'GPU memory       : {sm.gpu_mem_used_gb:,.2f} / {sm.gpu_mem_total_gb:,.2f} GiB')
        # endif

        # GPU温度
        if getattr(sm, 'gpu_temp_c', None) is not None:
            msg.append(f'GPU temp         : {sm.gpu_temp_c:.1f} °C')
        # endif

        # disk
        if (getattr(sm, 'disk_total', None) is not None) and (getattr(sm, 'disk_used', None) is not None) and (getattr(sm, 'disk_used_percent', None) is not None):
            disk_total_gb = sm.disk_total / (1024 ** 3)
            disk_used_gb = sm.disk_used / (1024 ** 3)
            msg.append(f'disk usage       : {disk_used_gb:,.2f} / {disk_total_gb:,.2f} GiB ({sm.disk_used_percent:.1f} %)')
        # endif

        # network
        if getattr(sm, 'net_bytes_sent', None) is not None and getattr(sm, 'net_bytes_recv', None) is not None:
            sent_mb = sm.net_bytes_sent / (1024 ** 2)
            recv_mb = sm.net_bytes_recv / (1024 ** 2)
            msg.append(f'network tx/rx    : {sent_mb:,.2f} / {recv_mb:,.2f} MiB')
        # endif

        self.logger.log('\n'.join(msg), log_type=LogType.System_Metrics)
    # enddef

    @measure_time
    def show_rom_cache_status(self):
        rom_cache_info = AshMaizeROMManager.status()
        size_gb = sum(rom_cache_info.values()) / (1024 ** 3)

        self.logger.log('\n'.join([
            '=== [R]OM Cache Status ===',
            f'num  : {len(rom_cache_info)}',
            f'used : {size_gb:,.2f} GiB',
            ]
            ), log_type=LogType.ROM_Cache_Status)
    # enddef

    # -------------------------
    # other scheduled commands
    # -------------------------
    @measure_time
    def maintain_rom_cache(self):
        def memory_stats_str(sm: SystemMetrics) -> list[str]:
            return [
                f'memory total     : {sm.memory_total_gb:,.2f} GiB',
                f'memory used      : {sm.memory_used_gb:,.2f} GiB ({sm.memory_used_percent:.1f} %)',
                f'memory available : {sm.memory_availale_gb:,.2f} GiB',
                f'memory free      : {sm.memory_free_gb:,.2f} GiB',
                ]
        # enddef

        # -------------------------
        # check if ROM caches need to be deleted
        # -------------------------
        sm = SystemMetrics.init()
        rom_cache = AshMaizeROMManager.status()
        rom_cache_size_avg = (sum(rom_cache.values()) / len(rom_cache)) if rom_cache else 0

        is_clear_needed = (sm.memory_used_percent > 80) or (sm.memory_available < rom_cache_size_avg)

        # -------------------------
        # take an action
        # -------------------------
        msg = ['=== ROM Cache Maintenance ===']
        msg += memory_stats_str(sm)
        if is_clear_needed:
            AshMaizeROMManager.clear_all()

            msg.append(f'-> All ROM caches have been cleared.')
        else:
            keys_need = {
                ch.no_pre_mine
                for address in self.list__address
                for ch in self.tracker.get_challenges(address=address, list__status=[SolutionStatus.Invalid])
                }
            keys_drop = {key for key in AshMaizeROMManager.keys() if key not in keys_need}
            AshMaizeROMManager.drop(*keys_drop)

            msg.append('-' * 21)
            msg.append(f'-> {len(keys_drop)} ROM caches have been cleared.')
            msg.append('-' * 21)
        # endif
        msg += memory_stats_str(SystemMetrics.init())

        self.logger.log('\n'.join(msg), log_type=LogType.ROM_Cache_Maintenance)

        if is_clear_needed:
            self.show_rom_cache_status()
        # endif
    # enddef
