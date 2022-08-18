from datetime import datetime as dt


class StatCollector:
    def __init__(self):
        self.last_status = dt.now()
        self.start = None
        self.end = None
        self.submitted = 0
        self.successes = 0
        self.errors = 0
        self.processed = 0
        self.errortypes = {}

        self.periodic_printer = self.__default_periodic_printer

    def start_clock(self):
        self.start = dt.now()

    def add_submitted(self, n=1):
        self.submitted += n

    def add_success(self, n=1):
        self.successes += n

    def add_error(self, errormsg=None, n=1):
        if errormsg is None:
            self.errors += n
        else:
            if n != 1:
                raise ValueError("N must be 1 if errormsg is provided!")

            if self.errortypes.get(errormsg) is None:
                self.errortypes[errormsg] = 0
            self.errortypes[errormsg] += 1

    def add_processed(self, n=1):
        self.processed += n

    def __default_periodic_printer(self, num_workers, elapsed, now, print_errors):
        success_rate = self.successes / self.processed * 100 if self.processed > 0 else 0
        print(
            "STATUS: workers: %d, processed: %s, successes: %s, errors: %s, lag: %.2f, avg req/s: %.2f/s, success rate: %.2f%%" % (
                num_workers, self.processed, self.successes, self.errors, elapsed,
                self.processed / (now - self.start).total_seconds(), success_rate))
        if print_errors:
            print("ERRORS:", self.errortypes)

    def should_print(self, interval):
        now = dt.now()
        elapsed = (now - self.last_status).total_seconds()
        if elapsed > interval:
            self.last_status = now
            return True, now, elapsed
        else:
            return False, now, elapsed

    def print_periodic(self, num_workers, interval=1, print_errors=False):
        shouldprint, now, elapsed = self.should_print(interval)
        if shouldprint:
            self.periodic_printer(num_workers, elapsed, now, print_errors)

    def print_final(self):
        self.end = dt.now()
        delta = (self.end - self.start).total_seconds()
        print("{} requests took {:.2f} seconds, avg: {:.2f}, errors: {:.2f} %".format(
            self.processed, delta, self.processed / delta, self.errors / self.processed * 100
        ))
