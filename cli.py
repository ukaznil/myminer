import argparse

from midnight.midnight_app import MidNightApp
from project import Project


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Scavenger Mine CLI client.',
        )
    parser.add_argument(
        '-p', '--project',
        type=str, required=True,
        choices=['midnight', 'defensio'],
        )
    subparsers = parser.add_subparsers(
        dest='command',
        required=True,
        help='Sub commands',
        )

    # -------------------------
    # wallet sub-command
    # -------------------------
    wallet_parser = subparsers.add_parser(
        'register',
        )
    wallet_parser.add_argument(
        '-a', '--address',
        type=str,
        required=True,
        )
    wallet_parser.set_defaults(handler='wallet')

    # -------------------------
    # mine sub-command
    # -------------------------
    mine_parser = subparsers.add_parser(
        'mine',
        )
    mine_parser.add_argument(
        '-t', '--num_threads',
        type=int,
        )
    mine_parser.set_defaults(handler='mine')

    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.project == 'midnight':
        project = Project.MidNight
    elif args.project == 'defensio':
        project = Project.Defensio
    else:
        raise NotImplementedError(args.project)
    # endif

    app = MidNightApp(project=project)
    if args.command == 'register':
        app.handle_register(address=args.address)
    elif args.command == 'mine':
        app.handle_mine(num_threads=args.num_threads)
    # endif

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
