from export_manager import dataset
from datetime import datetime
from datetime import timedelta
from datetime import timezone


HIGHLIGHT_DELTAS = [
    timedelta(days=7),
    timedelta(days=180),
]


def text_table(header, rows, indent=''):
    widths = [max([len(r[i]) for r in [header] + rows])
              for i in range(len(header))]
    fmt = '  '.join('{:<' + str(w) + '}' for w in widths) + '\n'
    hrow = fmt.format(*header)
    divrow = ('-' * len(hrow)) + '\n'
    result = indent + hrow + indent + divrow
    for row in rows:
        result += indent + fmt.format(*row)
    return result


class Report:
    def __init__(self, dataset_accessors):
        self.dataset_accessors = dataset_accessors
        self.has_no_complete = []
        self.is_overdue = []
        self.last_is_incomplete = []
        self.last_success_gone = []
        self.last_success_id = {}
        self.missing_metrics = []
        self.highlighted_metrics = {}

        for dsa in dataset_accessors:
            pas = dsa.parcel_accessors()
            last_complete = next(
                (p for p in reversed(pas) if p.is_complete()), None)
            if not last_complete:
                self.has_no_complete.append(dsa)

            if dsa.is_due(margin=timedelta()):
                self.is_overdue.append(dsa)

            if pas and not pas[-1].is_complete():
                self.last_is_incomplete.append(dsa)

            metrics = dsa.read_metrics()
            if last_complete:
                if last_complete.parcel_id not in metrics:
                    self.missing_metrics.append(dsa)
                cur_metrics = metrics.get(last_complete.parcel_id, {})
                if any(x == 'ERROR' for x in cur_metrics.values()):
                    self.missing_metrics.append(dsa)

            successes = [r for r in metrics.values()
                         if r.get('success', 'N') == 'Y']
            if len(successes):
                pid = successes[-1]['parcel_id']
                gone = not any(
                    p.parcel_id == pid and p.is_complete() for p in pas)
                if gone:
                    self.last_success_gone.append(dsa)
                self.last_success_id[dsa] = pid

                deltas = list(HIGHLIGHT_DELTAS)
                candidates = list(reversed(successes))[1:]
                highlights = [successes[-1]]
                prev = dataset.parse_parcel_id(pid)
                while deltas and candidates:
                    row = candidates.pop(0)
                    cur = dataset.parse_parcel_id(row['parcel_id'])
                    if prev - deltas[0] > cur:
                        highlights.append(row)
                        prev = cur
                        deltas.pop(0)
                self.highlighted_metrics[dsa] = highlights
            else:
                self.last_success_id[dsa] = None

        self.has_warnings = (self.has_no_complete
                             or self.is_overdue
                             or self.last_is_incomplete
                             or self.last_success_gone
                             or self.missing_metrics)

    def plaintext(self):
        if not self.dataset_accessors:
            return 'No datasets were specified :/'

        namewidth = max(len(d.path.name) for d in self.dataset_accessors)

        result = ''

        if self.has_no_complete:
            result += 'WARNING: no complete parcel for: '
            result += ', '.join(d.path.name for d in self.has_no_complete)
            result += '\n'

        if self.is_overdue:
            result += 'WARNING: overdue: '
            result += ', '.join(d.path.name for d in self.is_overdue)
            result += '\n'

        if self.last_is_incomplete:
            result += 'WARNING: most recent parcel is incomplete for: '
            result += ', '.join(d.path.name for d in self.last_is_incomplete)
            result += '\n'

        if self.last_success_gone:
            result += 'WARNING: most recent successful parcel is missing for: '
            result += ', '.join(d.path.name for d in self.last_success_gone)
            result += '\n'

        if self.missing_metrics:
            result += 'WARNING: missing metrics in last complete parcel for: '
            result += ', '.join(d.path.name for d in self.missing_metrics)
            result += '\n'

        if self.has_warnings:
            result += '\n'
        else:
            result += 'No warnings!\n\n'

        twidth = (namewidth + 2 + 25)
        result += ('Newest successes:\n{:-<' + str(twidth) + '}\n').format('')
        for dsa in self.last_success_id:
            pid = self.last_success_id[dsa] or 'NONE'
            if dsa in self.last_success_gone:
                pid += ' (GONE)'
            result += (('{:<' + str(namewidth) + '}  {}\n')
                       .format(dsa.path.name, pid))

        for dsa in self.highlighted_metrics:
            metrics = self.highlighted_metrics[dsa]
            dates = [dataset.parse_parcel_id(m['parcel_id']) for m in metrics]
            now = datetime.now(timezone.utc)
            reldates = [f'{(now - d).days} days ago' for d in dates]
            headings = ['name'] + reldates
            keys = [k for k in metrics[0].keys()
                    if k not in ['parcel_id', 'success']]
            rows = [[k] + [m[k] for m in metrics] for k in keys]
            result += f'\nMetrics for {dsa.path.name}:\n\n'
            result += text_table(headings, rows, indent='  ')

        return result
