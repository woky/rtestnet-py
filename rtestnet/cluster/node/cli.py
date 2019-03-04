import sys
import argparse

from pathlib import Path

from .. import ClusterContext
from . import NodeError, NodeContext, NodeCtl


def create_arg_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-d',
        '--conf-dir',
        default='.',
        type=lambda x: Path(x),
        help='Configuration directory'
    )
    parser.add_argument(
        '-p',
        '--private-dir',
        default=None,
        type=lambda x: Path(x),
        help='Private directory (defaults to --conf-dir)'
    )
    parser.add_argument('node', help='Name of the node')
    subparsers = parser.add_subparsers(dest='action', required=True)

    def add_clean_options(subp):
        clean_group = subp.add_mutually_exclusive_group()
        clean_group.add_argument(
            '-c',
            '--clean-data',
            action='store_true',
            help='Remove VM instance and its assoicated data'
        )
        clean_group.add_argument(
            '-C',
            '--clean-all',
            action='store_true',
            help=(
                'Like --clean-data, but also remove static external IP and DNS record'
            )
        )

    p = subparsers.add_parser(
        'start', help='Start the node if it is not running'
    )
    p = subparsers.add_parser('stop', help='Stop the node if it is running')
    add_clean_options(p)
    p = subparsers.add_parser('restart', help='stop followed by start')
    add_clean_options(p)
    p = subparsers.add_parser('lead', help='Add "boot" DNS record to this node')

    return parser


def run_with_args(args):
    ctx = NodeContext(
        ClusterContext(args.conf_dir, args.private_dir or args.conf_dir),
        args.node
    )
    ctl = NodeCtl(ctx)
    if args.action == 'stop' or args.action == 'restart':
        ctl.stop(clean=args.clean_data, mrproper=args.clean_all)
        if args.action == 'restart':
            ctl.start()
    elif args.action == 'start':
        ctl.start()
    elif args.action == 'lead':
        ctl.make_leader()
    else:
        raise RuntimeError(f'Invalid action "{args.action}"')


def main():
    args = create_arg_parser().parse_args()
    ec = 0
    try:
        run_with_args(args)
    except NodeError as e:
        print('ERROR:', e, file=sys.stderr)
        if e.__cause__:
            print('ERROR:', e.__cause__, file=sys.stderr)
        ec = 1
    sys.exit(ec)


if __name__ == '__main__':
    main()
