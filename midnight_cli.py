import argparse
import sys
import threading
import time
from typing import *

import psutil

from ashmaize_miner import AshMaizeMiner
from base_miner import BaseMiner
from challenge import Challenge
from logger import LogType, Logger, measure_time
from project import Project
from solution import Solution
from tracker import Tracker, WorkStatus
from utils import assert_type, print_with_time, safefstr


class MidnightCLI(BaseMiner):
    def __init__(self):
        self.parser = argparse.ArgumentParser(
            description='Scavenger Mine CLI client.',
            )
        self.parser.add_argument(
            '-p', '--project',
            type=str, required=True,
            choices=['midnight', 'defensio'],
            )

        # サブパーサを作成
        subparsers = self.parser.add_subparsers(
            dest="command",
            required=True,  # Python 3.7+ ならOK
            help="サブコマンド"
            )

        # -------------------------
        # wallet サブコマンド
        # -------------------------
        wallet_parser = subparsers.add_parser(
            'wallet',
            help="ウォレットを登録・設定する"
            )
        wallet_parser.add_argument(
            '-a', '--address',
            type=str,
            required=True,
            help="ウォレットアドレス (例: 0x...)"
            )
        wallet_parser.set_defaults(func=self.handle_wallet)

        # -------------------------
        # donate サブコマンド
        # -------------------------
        donate_parser = subparsers.add_parser(
            'donate',
            help='donation',
            )
        donate_parser.add_argument(
            '-d', '--donate_to',
            type=str,
            required=True,
            help='donation address',
            )
        donate_parser.set_defaults(func=self.handle_donate)

        # -------------------------
        # mine サブコマンド
        # -------------------------
        mine_parser = subparsers.add_parser(
            "mine",
            help="マイニングを開始する"
            )
        mine_parser.add_argument(
            '-n', '--num',
            type=int,
            default=0,
            help="使用するスレッド数"
            )
        mine_parser.set_defaults(func=self.handle_mine)
    # enddef

    def run(self, argv=None):
        args = self.parser.parse_args(argv)

        # その他
        if args.project == 'midnight':
            self.project = Project.MidNight
        elif args.project == 'defensio':
            self.project = Project.Defensio
        else:
            raise NotImplementedError(args.project)
        # endif
        self.logger = Logger(project=self.project)

        self.base_url = self.project.base_url
        self.tracker = Tracker(project=self.project, logger=self.logger)
        self.miner = AshMaizeMiner(logger=self.logger)

        self.addrbook = {}
        self.tracker.get_wallets(None)

        args.func(args)
    # enddef

    @measure_time
    def make_addressbook(self, num: Optional[int]) -> list[str]:
        assert_type(num, int, allow_none=True)

        list__address = self.tracker.get_wallets(num)
        self.addrbook.clear()
        for idx_addr, address in enumerate(list__address):
            self.addrbook[address] = f'ADDR-#{idx_addr}'
        # endfor

        return list__address
    # enddef

    @measure_time
    def handle_wallet(self, args: argparse.Namespace):
        self.register(address=args.address)
    # enddef

    @measure_time
    def handle_donate(self, args: argparse.Namespace):
        self.donate_all_with_confirmation(donation_address=args.donate_to)
    # enddef

    @measure_time
    def handle_mine(self, args: argparse.Namespace):
        try:
            list__address = self.make_addressbook(args.num)

            def show_worklist():
                msg = []
                msg.append('=== [W]orklist ===')
                for idx_addr, address in enumerate(list__address):
                    msg.append(f'[{self.addrbook[address]}] {address}')

                    list__challenge = self.tracker.get_challenges(address=address, list__status=[ws for ws in WorkStatus if ws != WorkStatus.Validated])
                    challenge_solving = self.tracker.get_solving_challenge(address=address)
                    if len(list__challenge) > 0:
                        for challenge in list__challenge:
                            if challenge_solving and challenge.challenge_id == challenge_solving.challenge_id:
                                mark = '*'
                            else:
                                mark = ' '
                            # endif

                            msg.append(f'- [{mark}] day/ch#={challenge.day}/{challenge.challenge_number}, id={challenge.challenge_id}')
                        # endfor
                    else:
                        msg.append(f'- None')
                    # endif
                # endfor

                self.logger.log('\n'.join(msg), log_type=LogType.Worklist)
            # enddef

            def async_show_worklist():
                threading.Thread(
                    target=show_worklist,
                    daemon=True,
                    ).start()
            # enddef

            def show_hashrate():
                msg = [f'=== [H]ashrate ===']
                for address in list__address:
                    addr_short = self.addrbook[address]
                    hashrate = safefstr(self.miner.get_hashrate(address), ',.0f')
                    tries = safefstr(self.miner.get_tries(address), ',')
                    challenge = self.miner.get_challenge(address)
                    if challenge:
                        cid = challenge.challenge_id
                    else:
                        cid = None
                    # endif

                    msg.append(f'[{addr_short}] Hashrate={hashrate} H/s, tries={tries}, challenge={cid}')
                # endfor

                self.logger.log('\n'.join(msg), log_type=LogType.Hashrate)
            # enddef

            def async_show_hashrate():
                threading.Thread(
                    target=show_hashrate,
                    daemon=True,
                    ).start()
            # enddef

            def show_statistics():
                msg = [f'=== [S]tatistics ===']
                for address in list__address:
                    try:
                        resp = self.get_statistics(address)
                        time.sleep(0.5)

                        receipts = resp['local']['crypto_receipts']
                        if self.project == Project.MidNight:
                            raise NotImplementedError
                        elif self.project == Project.Defensio:
                            allocation = resp['local']['dfo_allocation'] / 1_000_000
                        # endif
                        msg.append(f'[{self.addrbook[address]}] receipts={receipts:,}, allocation={allocation:,}')
                    except:
                        msg.append(f'[{self.addrbook[address]}] Error')
                    # endtry
                # endfor

                self.logger.log('\n'.join(msg), log_type=LogType.Statistics)
            # enddef

            def async_show_statistics():
                threading.Thread(
                    target=show_statistics,
                    daemon=True,
                    ).start()
            # enddef

            def show_rom_cache_status():
                rom_cache_info = self.miner.rom_cache_info()
                size_in_gib = sum(rom_cache_info.values()) / (1024 ** 3)
                msg = [
                    '=== [R]OM Cache Status ===',
                    f'num: {len(rom_cache_info)}',
                    f'size: {size_in_gib:,.2f} GiB',
                    ]

                self.logger.log('\n'.join(msg), log_type=LogType.Cache_Status)
            # enddef

            def async_show_rom_cache_status():
                threading.Thread(
                    target=show_rom_cache_status,
                    daemon=True,
                    ).start()
            # enddef

            def maintain_rom_cache():
                list__challenge = []
                for address in list__address:
                    list__challenge += self.tracker.get_challenges(address=address, list__status=[WorkStatus.Open, WorkStatus.Invalid])
                # endfor

                self.miner.maintain_rom_cache(list__challenge)
            # enddef

            def async_maintain_rom_cache():
                threading.Thread(
                    target=maintain_rom_cache,
                    daemon=True,
                    ).start()
            # enddef

            def check_memory():
                def current_memory_status(vm: NamedTuple) -> list[str]:
                    B_per_GiB = 1024 ** 3

                    return [
                        '=== Memory ===',
                        f'total:     {vm.total / B_per_GiB:7,.2f} GiB',
                        f'used:      {vm.used / B_per_GiB:7,.2f} GiB ({vm.percent:.1f} %)',
                        f'available: {vm.available / B_per_GiB:7,.2f} GiB',
                        f'free:      {vm.free / B_per_GiB:7,.2f} GiB',
                        ]
                # enddef

                rom_cache = self.miner.rom_cache_info()
                memory_avg = (sum(rom_cache.values()) / len(rom_cache)) if rom_cache else 0

                vm = psutil.virtual_memory()
                release_cache = (vm.percent > 80) or (vm.available < memory_avg)

                msg = current_memory_status(vm)
                msg.append(f'-> release ROM cache?: {release_cache}')
                self.logger.log('\n'.join(msg), log_type=LogType.Memory)

                if release_cache:
                    show_rom_cache_status()

                    self.miner.release_rom_cache()

                    self.logger.log('\n'.join(current_memory_status(psutil.virtual_memory())), log_type=LogType.Memory)
                # endif
            # enddef

            def async_check_memory():
                threading.Thread(
                    target=check_memory,
                    daemon=True,
                    ).start()
            # enddef

            def check_threads():
                num = len(threading.enumerate())

                msg = [
                    '=== Threads ===',
                    f'{num} threads running'
                    ]

                self.logger.log('\n'.join(msg), log_type=LogType.Thread)
            # enddef

            def async_check_threads():
                threading.Thread(
                    target=check_threads,
                    daemon=True,
                    ).start()
            # enddef

            # Thread開始
            threads = []  # type: list[threading.Thread]
            def input_loop():
                for line in sys.stdin:
                    cmd = line.strip().lower()

                    if cmd == 'w':
                        async_show_worklist()
                    elif cmd == 'h':
                        async_show_hashrate()
                    elif cmd == 's':
                        async_show_statistics()
                    elif cmd == 'r':
                        async_show_rom_cache_status()
                    elif cmd == 't':
                        async_check_threads()
                    elif cmd == 'q':
                        self.logger.log('=== Stopping miner... ===', log_type=LogType.System)
                        self.miner.stop()
                        break
                    else:
                        print(f"Invalid command: '{cmd}'. Available: ([W]orklist, [H]ashrate, [S]tatistics, [R]OM-cache, [T]hreads, [Q]uit)")
                    # endif
                # endfor
            # enddef
            threads.append(threading.Thread(target=input_loop, daemon=True))

            for address in list__address:
                t = threading.Thread(
                    target=self.mine_loop,
                    args=(address,),
                    daemon=True,  # プロセス終了時に一緒に落ちてOKなら daemon で
                    )
                threads.append(t)
            # endfor

            self.miner.start()
            for thread in threads:
                thread.start()
            # endfor

            last_fetch_a_new_challenge = 0
            last_show_info = 0
            last_maintain_cache = time.time()
            last_check_memory = 0
            while self.miner.is_running():
                now = time.time()

                if now - last_fetch_a_new_challenge > 60 * 2:
                    self.async_fetch_a_new_challenge()

                    last_fetch_a_new_challenge = now
                # endif

                if now - last_show_info > 60 * 15:
                    async_show_worklist()
                    async_show_hashrate()
                    async_show_statistics()
                    async_show_rom_cache_status()

                    last_show_info = now
                # endif

                if now - last_maintain_cache > 60 * 30:
                    async_maintain_rom_cache()

                    last_maintain_cache = now
                # endif

                if now - last_check_memory > 60 * 10:
                    async_check_memory()

                    last_check_memory = now
                # endif

                time.sleep(0.5)
            # endwhile

            self.logger.log('=== Miner Stopped. ===', log_type=LogType.System)
        finally:
            self.tracker.close()
        # endtry
    # enddef

    # -------------------------
    # wallet サブコマンド
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
        if self.project == Project.MidNight:
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
    def register(self, address: str):
        assert_type(address, str)

        # tandc
        data = self.get_tandc()
        print_with_time('\n'.join([
            f'== Terms and Conditions ===',
            f'Version: {data.get("version")}',
            f'{data.get("content", "")}',
            '',
            f'=== Message to sign (wallet CIP-30)',
            f'{data.get("message", "")}',
            ]))

        # register
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

    # -------------------------
    # donate サブコマンド
    # -------------------------
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
    def donate_each_address(self, address: str, donation_address: str) -> bool:
        assert_type(address, str)
        assert_type(donation_address, str)

        message_to_sign = f'Assign accumulated Scavenger rights to: {donation_address}'
        print_with_time('\n'.join([
            f'address: [{self.addrbook[address]}] {address}',
            f'=== Message to sign (wallet CIP-30) ===',
            ]))
        signature = input(f'{message_to_sign}')

        resp = self.donate_to(destionation_address=donation_address, original_address=address, signature=signature)
        status = resp.get('status')
        if status == 'success':
            print_with_time('\n'.join([
                '=== Donation Response: Success ===',
                f'{resp}'
                ]))

            return True
        else:
            message = resp.get('message')
            error = resp.get('error')
            status_code = resp.get('status_code')
            print_with_time('\n'.join([
                '=== Donetion Response: Error ===',
                f'status: {status_code}'
                f'message: {message}',
                f'error: {error}',
                ]))

            return False
        # endif
    # enddef

    @measure_time
    def donate_all(self, donation_address: str, dry_run: bool = False):
        assert_type(donation_address, str)
        assert_type(dry_run, bool)

        list__address = self.make_addressbook(None)
        assert donation_address in list__address, donation_address

        for address in list__address:
            addr_short = self.addrbook[address]

            resp_statistics = self.get_statistics(address)
            current_donattoin_address = resp_statistics['local_with_donate']['donation_address']
            if current_donattoin_address == donation_address:
                print(f'For [{addr_short}] {address[:7]}...{address[-7:]}, the given donation address={donation_address[:7]}...{donation_address[-7:]} is already set. Skipped.')
            else:
                print(f'For [{addr_short}] {address[:7]}...{address[-7:]}, the given donation address={donation_address[:7]}...{donation_address[-7:]} is not set yet.')
                if dry_run:
                    print(f'-> Dry-run.')
                else:
                    success = self.donate_each_address(address=address, donation_address=donation_address)
                # endif
            # endif

            time.sleep(1)
        # endfor
    # enddef

    @measure_time
    def donate_all_with_confirmation(self, donation_address: str):
        assert_type(donation_address, str)

        pass
        # self.donate_all(donation_address, dry_run=False)
        # self.donate_all(donation_address, dry_run=True)
    # enddef

    # -------------------------
    # mine サブコマンド
    # -------------------------
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

    @measure_time
    def fetch_a_new_challenge(self) -> None:
        try:
            challenge_resp = self.get_challenge()
        except Exception as e:
            self.logger.log('\n'.join([
                f'=== Fetch a new Challenge: Error ===',
                f'error: {e}'
                ]), log_type=LogType.Fetch_New_Challenge_Error)
            time.sleep(5)

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
    def async_fetch_a_new_challenge(self) -> None:
        threading.Thread(
            target=self.fetch_a_new_challenge,
            daemon=True,
            ).start()
    # enddef

    @measure_time
    def mine_challenge(self, address: str, challenge: Challenge) -> None:
        assert_type(address, str)
        assert_type(challenge, Challenge)

        msgheader = f'[{self.addrbook[address]}]'

        # =======
        # Work assign
        # =======
        self.tracker.add_work(address=address, challenge=challenge, status=WorkStatus.Solving)
        self.logger.log('\n'.join([
            f'=== {msgheader} Start this Challenge ===',
            f'address: {address}',
            f'{challenge}',
            ]), log_type=LogType.Start_New_Challenge, sufix=msgheader)

        # =======
        # Find a solution
        # =======
        solution = self.tracker.get_found_solution(address=address, challenge=challenge)
        is_solutoin_cached = (solution is not None)
        if not is_solutoin_cached:
            try:
                solution = self.miner.mine(challenge=challenge, address=address)
                if solution is None:
                    return
                # endif

                self.tracker.add_solution_found(address=address, challenge=challenge, solution=solution)
            finally:
                self.tracker.update_work(address=address, challenge=challenge, status=WorkStatus.Open)
            # endtry
        # endif

        self.logger.log('\n'.join([
            f'=== {msgheader} {"Cached-solution" if is_solutoin_cached else "Solution"} Found ===',
            f'address: {address}',
            f'challenge: {challenge.challenge_id}',
            f'solution: {solution}',
            ]), log_type=LogType.Solution_Found, sufix=msgheader)

        if not self.miner.is_running():
            return
        # endif

        # =======
        # Submit the solution
        # =======
        try:
            resp = self.submit_solution(address=address, challenge=challenge, solution=solution)

            msg = [
                f'=== {msgheader} Solution Submission Response ===',
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

            self.logger.log('\n'.join(msg), log_type=LogType.Solution_Submission, sufix=msgheader)
        except Exception as e:
            self.logger.log('\n'.join([
                f'=== {msgheader} Solution Submission Error ===',
                f'address: {address}',
                f'challenge: {challenge.challenge_id}',
                f'solution: {solution}',
                f'error: {e}'
                ]), log_type=LogType.Solution_Submission_Error, sufix=msgheader)

            time.sleep(1)

            return
        # endtry
    # enddef

    @measure_time
    def mine_loop(self, address: str):
        assert_type(address, str)

        while self.miner.is_running():
            challenge = self.tracker.get_oldest_unsolved_challenge(address)

            if challenge is None:
                time.sleep(60)
            else:
                self.mine_challenge(address=address, challenge=challenge)
            # endif

            time.sleep(0.5)
        # endwhile
    # enddef


if __name__ == "__main__":
    cli = MidnightCLI()
    cli.run()
