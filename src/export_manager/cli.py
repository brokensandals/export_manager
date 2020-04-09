import argparse
from export_manager.dataset import DatasetDir


def init(args):
    for path in args.path:
        DatasetDir(path).initialize(git=args.git)


def main(args=None):
    parser = argparse.ArgumentParser()
    parser.set_defaults(func=None)

    subs = parser.add_subparsers(title='Commands')

    p_init = subs.add_parser('init', help='initialize new dataset dirs')
    p_init.add_argument('path', nargs='+', help='dataset dir path')
    p_init.add_argument('-g', '--git', action='store_true',
                        help='initialize a git repo')
    p_init.set_defaults(func=init)

    args = parser.parse_args(args)
    if not args.func:
        parser.print_help()
        return
    args.func(args)
