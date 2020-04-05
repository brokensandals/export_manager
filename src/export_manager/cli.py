import argparse
import os
import sys
from git import Repo
from pathlib import Path
from export_manager.exportdir import ExportDirSet
from export_manager.exportdir import ExportDir


def new(args):
    path = Path(args.base, args.name[0])
    exdir = ExportDir(path)
    exdir.initialize()
    if args.git:
        Repo.init(str(path))


def process(args):
    eds = ExportDirSet(args.base)
    exdirs = eds.get_dirs()
    if args.only:
        exdirs = [e for e in exdirs if e.path.stem in args.only]
    failed = False
    for exdir in exdirs:
        for err in exdir.process():
            print(err, file=sys.stderr)
            failed = True
    if failed:
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--base', default=os.getcwd(),
                        help='base directory containing export directories' +
                             ' (default: current working directory)')
    parser.set_defaults(func=None)

    subs = parser.add_subparsers(title="Commands")

    p_new = subs.add_parser('new', help='initialize new export directory')
    p_new.add_argument('name', nargs=1, help='new subdirectory name')
    p_new.add_argument('-g', '--git', action='store_true',
                       help='initialize a git repo')
    p_new.set_defaults(func=new)

    p_process = subs.add_parser('process', help='run due exports & cleanups')
    p_process.add_argument('-o', '--only', nargs='*')
    p_process.set_defaults(func=process)

    args = parser.parse_args()
    if not args.func:
        parser.print_help()
        return
    args.func(args)
