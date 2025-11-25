import argparse

from base_app import BaseApp
from midnight.midnight_app import MidnightApp
from project import Project


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Scavenger Mine CLI client.',
        )
    parser.add_argument(
        '-p', '--project',
        type=str,
        required=True,
        choices=['midnight', 'defensio'],
        help='Target project to use.',
        )
    subparsers = parser.add_subparsers(
        dest='command',
        required=True,
        help='Top level command to run.',
        )

    # -------------------------
    # wallet sub-command
    # -------------------------
    wallet_parser = subparsers.add_parser(
        'wallet',
        description='Wallet related commands.',
        help='Manage wallet addresses.',
        )
    wallet_subparsers = wallet_parser.add_subparsers(
        dest='wallet_command',
        required=True,
        help='Wallet sub commands.',
        )

    # wallet register -a ...
    wallet_register_parser = wallet_subparsers.add_parser(
        'register',
        description='Register a new wallet address to be used for mining rewards.',
        help='Register a wallet address.',
        )
    wallet_register_parser.add_argument(
        '-a', '--address',
        type=str,
        required=True,
        help='Wallet address to register.',
        )
    wallet_register_parser.set_defaults(handler='register_wallet')

    # wallet list
    wallet_list_parser = wallet_subparsers.add_parser(
        'list',
        description='List all wallet addresses registered for this project.',
        help='Show registered wallet addresses.',
        )
    wallet_list_parser.set_defaults(handler='list_wallets')

    # -------------------------
    # mine sub-command
    # -------------------------
    mine_parser = subparsers.add_parser(
        'mine',
        description='Start the miner for the selected project.',
        help='Start mining.',
        )
    mine_parser.add_argument(
        '-t', '--num_threads',
        type=int,
        help='Number of miner threads to spawn.',
        )
    mine_parser.set_defaults(handler='mine')

    return parser


# -------------------------
# handlers
# -------------------------
def handle_register_wallet(app: BaseApp, args: argparse.Namespace) -> None:
    app.handle_register(address=args.address)


def handle_list_wallet(app: BaseApp, args: argparse.Namespace) -> None:
    app.handle_list_wallets()


def handle_mine(app: BaseApp, args: argparse.Namespace) -> None:
    app.handle_mine(num_threads=args.num_threads)


# -------------------------
# main
# -------------------------
def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.project == 'midnight':
        app = MidnightApp(project=Project.Midnight)
    elif args.project == 'defensio':
        app = MidnightApp(project=Project.Defensio)
    else:
        raise NotImplementedError(args.project)
    # endif

    handlers = {
        'register_wallet': handle_register_wallet,
        'list_wallets': handle_list_wallet,
        'mine': handle_mine,
        }

    handler_key = getattr(args, 'handler', None)
    handler = handlers.get(handler_key)
    if handler is None:
        parser.print_help()

        return 1
    else:
        handler(app, args)

        return 0
    # endif


if __name__ == "__main__":
    raise SystemExit(main())
