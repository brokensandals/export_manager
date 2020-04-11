"""Command-line interface for export_manager tool."""


import argparse
import sys
import traceback
from export_manager import dataset
from export_manager.dataset import DatasetAccessor
from export_manager.report import Report


def _ingest(args):
    parcel_id = args.parcel_id or dataset.new_parcel_id()
    ds = DatasetAccessor(args.dataset_path[0])
    ds.ingest_path(args.ingest_path[0], parcel_id)
    return 0


def _init(args):
    for path in args.path:
        DatasetAccessor(path).initialize(git=args.git)
    return 0


def _process(args):
    status = 0
    for path in args.path:
        ds = DatasetAccessor(path)

        if ds.is_due():
            parcel_id = dataset.new_parcel_id()
            try:
                ds.run_export(parcel_id)
            except Exception:
                status = 2
                print(f'export failed for {path}', file=sys.stderr)
                traceback.print_exc()

            try:
                ds.reprocess_metrics([parcel_id])
            except Exception:
                status = 2
                print(f'metrics failed for {path} parcel_id={parcel_id}',
                      file=sys.stderr)
                traceback.print_exc()

        try:
            ds.clean()
        except Exception:
            status = 2
            print(f'clean failed for {path}', file=sys.stderr)
            traceback.print_exc()

    return status


def _report(args):
    dsas = [DatasetAccessor(path) for path in args.path]
    r = Report(dsas)
    print(r.plaintext())
    return 0


def _reprocess_metrics(args):
    for path in args.path:
        ds = DatasetAccessor(path)
        if args.parcel_id:
            parcel_ids = [args.parcel_id]
        else:
            parcel_ids = ds.find_parcel_ids()
        ds.reprocess_metrics(parcel_ids)
    return 0


def main(args=None):
    """Runs the tool and returns its exit code.

    args may be an array of string command-line arguments; if absent,
    the process's arguments are used.
    """
    parser = argparse.ArgumentParser()
    parser.set_defaults(func=None)

    subs = parser.add_subparsers(title='Commands')

    p_ingest = subs.add_parser(
        'ingest',
        help='move a file/dir into a dataset')
    p_ingest.add_argument('dataset_path', nargs=1, help='dataset dir path')
    p_ingest.add_argument('ingest_path', nargs=1, help='file/dir to ingest')
    p_ingest.add_argument('-p', '--parcel_id', nargs='?',
                          help='parcel_id for ingested data '
                               '(format: yyyy-mm-ddThhmmssZ) '
                               '(defaults to current timestamp)')
    p_ingest.set_defaults(func=_ingest)

    p_init = subs.add_parser('init', help='initialize new dataset dirs')
    p_init.add_argument('path', nargs='+', help='dataset dir path')
    p_init.add_argument('-g', '--git', action='store_true',
                        help='initialize a git repo')
    p_init.set_defaults(func=_init)

    p_process = subs.add_parser('process',
                                help='run exports, update metrics, '
                                     'and perform cleaning, where needed')
    p_process.add_argument('path', nargs='+', help='dataset dir path')
    p_process.set_defaults(func=_process)

    p_report = subs.add_parser('report', help='summarize export activity')
    p_report.add_argument('path', nargs='+', help='dataset dir path')
    p_report.set_defaults(func=_report)

    p_reprocess_metrics = subs.add_parser('reprocess_metrics',
                                          help='update metrics for parcels')
    p_reprocess_metrics.add_argument(
        'path', nargs='+', help='dataset dir path')
    p_reprocess_metrics.add_argument(
        '-p', '--parcel_id', nargs='?',
        help='only reprocess specific parcel (format: yyyy-mm-ddThhmmssZ)')
    p_reprocess_metrics.set_defaults(func=_reprocess_metrics)

    args = parser.parse_args(args)
    if not args.func:
        parser.print_help()
        return 1
    return args.func(args)
