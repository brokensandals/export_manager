import argparse
import sys
from export_manager import dataset
from export_manager.dataset import DatasetDir


def clean(args):
    for path in args.path:
        DatasetDir(path).clean()
    return 0


def export(args):
    for path in args.path:
        parcel_id = args.parcel_id or dataset.new_parcel_id()
        ds = DatasetDir(path)
        ds.run_export(parcel_id)
    return 0


def init(args):
    for path in args.path:
        DatasetDir(path).initialize(git=args.git)
    return 0


def process(args):
    status = 0
    for path in args.path:
        ds = DatasetDir(path)

        if ds.is_due():
            parcel_id = dataset.new_parcel_id()
            try:
                ds.run_export(parcel_id)
            except Exception as e:
                status = 2
                print(f'export failed for {path}', file=sys.stderr)
                print(e, file=sys.stderr)

            try:
                metrics = ds.collect_metrics(parcel_id)
                ds.update_metrics({parcel_id: metrics})
            except Exception as e:
                print(f'metrics failed for {path} parcel_id={parcel_id}',
                      file=sys.stderr)
                print(e, file=sys.stderr)

        try:
            ds.clean()
        except Exception as e:
            print(f'clean failed for {path}', file=sys.stderr)
            print(e, file=sys.stderr)

    return status


def main(args=None):
    parser = argparse.ArgumentParser()
    parser.set_defaults(func=None)

    subs = parser.add_subparsers(title='Commands')

    p_clean = subs.add_parser('clean', help='perform cleaning where needed')
    p_clean.add_argument('path', nargs='+', help='dataset dir path')
    p_clean.set_defaults(func=clean)

    p_export = subs.add_parser('export', help='run export')
    p_export.add_argument('path', nargs='+', help='dataset dir path')
    p_export.add_argument('-p', '--parcel_id', nargs='?',
                          help='parcel_id for new export '
                               '(format: yyyy-mm-ddThhmmssZ) '
                               '(defaults to current timestamp)')
    p_export.set_defaults(func=export)

    p_init = subs.add_parser('init', help='initialize new dataset dirs')
    p_init.add_argument('path', nargs='+', help='dataset dir path')
    p_init.add_argument('-g', '--git', action='store_true',
                        help='initialize a git repo')
    p_init.set_defaults(func=init)

    p_process = subs.add_parser('process',
                                help='run exports, update metrics, '
                                     'and perform cleaning, where needed')
    p_process.add_argument('path', nargs='+', help='dataset dir path')
    p_process.set_defaults(func=process)

    args = parser.parse_args(args)
    if not args.func:
        parser.print_help()
        return 1
    return args.func(args)
