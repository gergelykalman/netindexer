import sys
import requests
from datetime import datetime as dt

from helpers.filereader import FileReader


def test_requests_dumb(urlgen, timeout):
    done, errors = 0, 0
    start = dt.now()
    while True:
        urls = urlgen.get_batch(1)
        if len(urls) == 0:
            break

        try:
            r = requests.get(urls[0], timeout=timeout)
        except Exception as exc:
            # print("Error", exc)
            errors += 1
        else:
            # print("Success")
            resplen = len(r.text)
            pass
        done += 1
    end = dt.now()
    delta = (end-start).total_seconds()
    print("{} requests took {:.2f} seconds, avg: {:.2f}, errors: {}".format(
        done, delta, done/delta, errors
    ))


if __name__ == "__main__":
    fr = FileReader(sys.argv[1])
    timeout = int(sys.argv[2])
    test_requests_dumb(fr, timeout=timeout)
