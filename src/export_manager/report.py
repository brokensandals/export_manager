from datetime import timedelta


class Report:
    def __init__(self, dataset_accessors):
        self.dataset_accessors = dataset_accessors
        self.has_no_complete = []
        self.is_overdue = []
        self.last_is_incomplete = []
        self.last_success_gone = []
        self.last_success_id = {}
        self.missing_metrics = []

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

        return result
