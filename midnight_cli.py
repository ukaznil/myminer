import argparse
import sys
import threading
import time

from ashmaize_miner import AshMaizeMiner
from base_miner import BaseMiner
from challenge import Challenge
from project import Project
from solution import Solution
from tracker import SolutionStatus, Tracker, WorkStatus
from utils import print_with_time, safefstr


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
            "-a", "--address",
            required=True,
            help="ウォレットアドレス (例: 0x...)"
            )
        wallet_parser.set_defaults(func=self.handle_wallet)

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
        self.base_url = self.project.base_url
        self.tracker = Tracker(project=self.project)
        self.miner = AshMaizeMiner()
        self.addrbook = {}

        args.func(args)
    # enddef

    def handle_wallet(self, args: argparse.Namespace):
        self.register(address=args.address)
    # enddef

    def handle_mine(self, args: argparse.Namespace):
        list__address = self.tracker.get_wallets(args.num)
        self.addrbook = {}
        for idx_addr, address in enumerate(list__address):
            self.addrbook[address] = f'AD-#{idx_addr}'
        # endfor

        def show_results():
            msg = []
            msg.append('=== [R]esults ===')
            for idx_addr, address in enumerate(list__address):
                msg.append(f'[{self.addrbook[address]}] {address}')
                num_open = self.tracker.get_num_work(address=address, status=WorkStatus.Open)
                num_working = self.tracker.get_num_work(address=address, status=WorkStatus.Working)
                num_solved = self.tracker.get_num_work(address=address, status=WorkStatus.Solved)
                num_invalid = self.tracker.get_num_work(address=address, status=WorkStatus.Invalid)
                msg.append(f'open={num_open}, working={num_working}, solved={num_solved}, invalid={num_invalid}')

                msg.append(f'todo:')
                list__challenge = self.tracker.get_open_challenges(address)
                if len(list__challenge) > 0:
                    for challenge in list__challenge:
                        msg.append(f'- day/ch#={challenge.day}/{challenge.challange_number}, id={challenge.challenge_id}')
                    # endfor
                else:
                    msg.append(f'- None')
                # endif
            # endfor
            print_with_time('\n'.join(msg))
        # enddef

        def show_hashrate():
            msg = [f'=== [H]ashrate ===']
            for address in list__address:
                addr_short = self.addrbook[address]
                hashrate = safefstr(self.miner.get_hashrate(address), ',.0f')
                tries = safefstr(self.miner.get_tries(address), ',')

                msg.append(f'[{addr_short}] Hashrate={hashrate} H/s, tries={tries}')
            # endfor

            print_with_time('\n'.join(msg))
        # enddef

        # Thread開始
        threads = []  # type: list[threading.Thread]
        def input_loop():
            for line in sys.stdin:
                cmd = line.strip()

                if cmd == 'r':
                    show_results()
                elif cmd == 'h':
                    show_hashrate()
                elif cmd == 'q':
                    print_with_time('=== Stopping miner... ===')
                    self.miner.stop()
                    break
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

        time_start = time.time()
        set__sec_fetch_a_new_challenge = set()
        set__sec_addresses_and_works = set()
        while self.miner.is_running():
            sec = int(time.time() - time_start)

            if sec % 30 == 0 and sec not in set__sec_fetch_a_new_challenge:
                self.fetch_a_new_challenge()
                set__sec_fetch_a_new_challenge.add(sec)
            # endif

            if sec % (60 * 10) == 0 and sec not in set__sec_addresses_and_works:
                show_results()
                set__sec_addresses_and_works.add(sec)
            # endif
        # endwhile

        print_with_time('=== Miner Stopped. ===')
    # enddef

    # -------------------------
    # wallet サブコマンド
    # -------------------------
    def _get_tandc(self) -> dict:
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

    def _register_address(self, address: str, signature: str, pubkey: str) -> dict:
        """
        POST /register/{address}/{signature}/{pubkey}

        All params are passed in the URL path, body is {}.
        """
        path = f'register/{address}/{signature}/{pubkey}'

        return self._post(path, {})
    # enddef

    def register(self, address: str):
        # tandc
        data = self._get_tandc()
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
        resp = self._register_address(address=address, signature=signature, pubkey=pubkey)
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
    # mine サブコマンド
    # -------------------------
    def _get_challenge(self) -> dict:
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

    def _submit_solution(self, address: str, challenge: Challenge, solution: Solution) -> dict:
        """
        POST /solution/{address}/{challenge_id}/{nonce}

        Body is {}.
        """
        path = f'/solution/{address}/{challenge.challenge_id}/{solution.nonce_hex}'

        return self._post(path, {})
    # enddef

    def fetch_a_new_challenge(self) -> None:
        try:
            challenge_resp = self._get_challenge()
        except Exception as e:
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
            challenge = Challenge(challenge)

            # save
            if self.tracker.add_challenge(challenge):
                print_with_time('\n'.join([
                    '=== New Challenge ===',
                    f'{challenge}',
                    ]))
            else:
                pass
            # endif
        # endif
    # enddef

    def mine_challenge(self, address: str, challenge: Challenge) -> None:
        if not self.tracker.work_exists(address=address, challenge=challenge):
            self.tracker.add_work(address=address, challenge=challenge)
        # endif
        self.tracker.update_work(address=address, challenge=challenge, status=WorkStatus.Working)

        solution = self.tracker.get_found_solution(address=address, challenge=challenge)
        is_solutoin_cached = (solution is not None)
        if not is_solutoin_cached:
            try:
                solution = self.miner.mine(challenge=challenge, address=address)
                if solution:
                    self.tracker.add_solution_found(address=address, challenge=challenge, solution=solution)
                # endif
            finally:
                self.tracker.update_work(address=address, challenge=challenge, status=WorkStatus.Open)
            # endtry
        # endif

        if not self.miner.is_running():
            return
        # endif

        print_with_time('\n'.join([
            f'=== Solution {"Cached" if is_solutoin_cached else "Found"} ===',
            f'address: [{self.addrbook[address]}] {address}',
            f'challenge: {challenge.challenge_id}',
            f'{solution}',
            ]))

        try:
            resp = self._submit_solution(address=address, challenge=challenge, solution=solution)
        except Exception as e:
            return
        # endtry
        msg = [
            '=== Solution Submission Response ===',
            f'{resp}',
            ]

        if 'crypto_receipt' in resp.keys():
            with self.tracker.db.atomic():
                self.tracker.update_work(address=address, challenge=challenge, status=WorkStatus.Solved)
                self.tracker.update_solution(address=address, challenge=challenge, solution=solution, status=SolutionStatus.Verified)
            # endwith
            msg.append(f'-> Solved !!!')

        else:
            code = resp.get('statusCode')
            message = resp.get('message')
            with self.tracker.db.atomic():
                self.tracker.update_work(address=address, challenge=challenge, status=WorkStatus.Invalid)
                self.tracker.update_solution(address=address, challenge=challenge, solution=solution, status=SolutionStatus.Invalid)
            # endwith
            msg.append(f'-> Solution Invalid. code={code}, message={message}')
        # endif
        print_with_time('\n'.join(msg))
    # enddef

    def mine_loop(self, address: str):
        while self.miner.is_running():
            challenge = self.tracker.get_oldtest_open_challenge(address)

            if challenge is None:
                time.sleep(60)
            else:
                print_with_time('\n'.join([
                    '=== Start this Challenge ===',
                    f'address: [{self.addrbook[address]}] {address}',
                    f'{challenge}',
                    ]))

                self.mine_challenge(address=address, challenge=challenge)
                time.sleep(1)
            # endif
        # endwhile
    # enddef


if __name__ == "__main__":
    cli = MidnightCLI()
    cli.run()
