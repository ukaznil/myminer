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
        self.base_url = self.project.base_url
        self.tracker = Tracker(project=self.project)
        self.miner = AshMaizeMiner()

        self.addrbook = {}
        self.tracker.get_wallets(0)

        args.func(args)
    # enddef

    def make_addressbook(self, num: int) -> list[str]:
        list__address = self.tracker.get_wallets(num)
        self.addrbook.clear()
        for idx_addr, address in enumerate(list__address):
            self.addrbook[address] = f'AD-#{idx_addr}'
        # endfor

        return list__address
    # enddef

    def handle_wallet(self, args: argparse.Namespace):
        self.register(address=args.address)
    # enddef

    def handle_donate(self, args: argparse.Namespace):
        self.donate_all_with_confirmation(donation_address=args.donate_to)
    # enddef

    def handle_mine(self, args: argparse.Namespace):
        list__address = self.make_addressbook(args.num)

        def __show_results():
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

        def show_results():
            threading.Thread(
                target=__show_results,
                daemon=True,
                ).start()
        # enddef

        def __show_hashrate():
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

            print_with_time('\n'.join(msg))
        # enddef

        def show_hashrate():
            threading.Thread(
                target=__show_hashrate,
                daemon=True,
                ).start()
        # enddef

        def __show_statistics():
            msg = [f'=== [S]tatistics ===']
            for address in list__address:
                try:
                    resp = self._get_statistics(address)
                    time.sleep(0.1)
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

            print_with_time('\n'.join(msg))
        # enddef

        def show_statistics():
            threading.Thread(
                target=__show_statistics,
                daemon=True,
                ).start()
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
                elif cmd == 's':
                    show_statistics()
                elif cmd == 'q':
                    print_with_time('=== Stopping miner... ===')
                    self.miner.stop()
                    break
                else:
                    print(f'Invalid command: {cmd}. Available: ([r]esults, [h]ashrate, [s]tatistics, [q]uit)')
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
        while self.miner.is_running():
            now = time.time()

            if now - last_fetch_a_new_challenge > 60 * 1:
                self.fetch_a_new_challenge()

                last_fetch_a_new_challenge = now
            # endif

            if now - last_show_info > 60 * 10:
                show_results()
                show_hashrate()
                show_statistics()

                last_show_info = now
            # endif

            if now - last_maintain_cache > 60 * 60:
                list__challenge = []
                for address in list__address:
                    list__challenge += self.tracker.get_open_challenges(address)
                # endfor
                self.miner.maintain_cache(list__challenge)

                last_maintain_cache = now
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
    # donate サブコマンド
    # -------------------------
    def _get_statistics(self, address: str) -> dict:
        path = f'statistics/{address}'

        return self._get(path)
    # enddef

    def _donate_to(self, destionation_address: str, original_address: str, signature: str) -> dict:
        path = f'donate_to/{destionation_address}/{original_address}/{signature}'

        return self._post(path, {})
    # enddef

    def donate_to(self, address: str, donation_address: str) -> bool:
        message_to_sign = f'Assign accumulated Scavenger rights to: {donation_address}'
        print_with_time('\n'.join([
            f'address: [{self.addrbook[address]}] {address}',
            f'=== Message to sign (wallet CIP-30) ===',
            ]))
        signature = input(f'{message_to_sign}')

        resp = self._donate_to(destionation_address=donation_address, original_address=address, signature=signature)
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

    def donate_all(self, donation_address: str, dry_run: bool = False):
        list__address = self.make_addressbook(0)
        assert donation_address in list__address, donation_address

        for address in list__address:
            addr_short = self.addrbook[address]

            resp_statistics = self._get_statistics(address)
            current_donattoin_address = resp_statistics['local_with_donate']['donation_address']
            if current_donattoin_address == donation_address:
                print(f'For [{addr_short}] {address[:7]}...{address[-7:]}, the given donation address={donation_address[:7]}...{donation_address[-7:]} is already set. Skipped.')
            else:
                print(f'For [{addr_short}] {address[:7]}...{address[-7:]}, the given donation address={donation_address[:7]}...{donation_address[-7:]} is not set yet.')
                if dry_run:
                    print(f'-> Dry-run.')
                else:
                    success = self.donate_to(address=address, donation_address=donation_address)
                # endif
            # endif

            time.sleep(1)
        # endfor
    # enddef

    def donate_all_with_confirmation(self, donation_address: str):
        pass
        # self.donate_all(donation_address, dry_run=False)
        # self.donate_all(donation_address, dry_run=True)
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

    def __fetch_a_new_challenge(self) -> None:
        try:
            challenge_resp = self._get_challenge()
        except Exception as e:
            print_with_time('\n'.join([
                f'=== Fetch a new Challenge: Error ===',
                f'error: {e}'
                ]))

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

    def fetch_a_new_challenge(self) -> None:
        threading.Thread(
            target=self.__fetch_a_new_challenge,
            daemon=True,
            ).start()
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
            f'=== {"Cached-solution" if is_solutoin_cached else "Solution"} Found ===',
            f'address: [{self.addrbook[address]}] {address}',
            f'challenge: {challenge.challenge_id}',
            f'solution: {solution}',
            ]))

        try:
            resp = self._submit_solution(address=address, challenge=challenge, solution=solution)
        except Exception as e:
            print_with_time('\n'.join([
                f'=== Solution Submission Error ===',
                f'address: [{self.addrbook[address]}] {address}',
                f'challenge: {challenge.challenge_id}',
                f'solution: {solution}',
                f'error: {e}'
                ]))

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
