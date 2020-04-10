from datetime import timedelta


class Report:
    def __init__(self, dataset_accessors):
        self.has_no_complete = []
        self.is_overdue = []
        self.last_is_incomplete = []
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

    def plaintext(self):
        result = ''

        if self.has_no_complete:
            result += 'WARNING: no complete parcels for: '
            result += ', '.join(d.path.name for d in self.has_no_complete)
            result += '\n'

        if self.is_overdue:
            result += 'WARNING: overdue: '
            result += ', '.join(d.path.name for d in self.is_overdue)
            result += '\n'

        if self.last_is_incomplete:
            result += 'WARNING: most recent parcels are incomplete for: '
            result += ', '.join(d.path.name for d in self.last_is_incomplete)
            result += '\n'

        if self.missing_metrics:
            result += 'WARNING: missing metrics in last complete parcel for: '
            result += ', '.join(d.path.name for d in self.missing_metrics)
            result += '\n'

        return result
