import os
import sys
import pickle
import random

from helper.helper import find_next_unpicklable_offset

fname = sys.argv[1]
num_tests = int(sys.argv[2])
chunksize = int(sys.argv[3])

with open(fname, "rb") as f:
    size = os.fstat(f.fileno()).st_size
    for i in range(num_tests):
        idx = random.randint(0, chunksize)

        off = find_next_unpicklable_offset(f, idx)
        if off is None:
            print("No unpicklable offset found until EOF: {}/{}".format(i, num_tests))
            continue

        f.seek(off, os.SEEK_SET)

        obj = pickle.load(f)
        print("SUCCESS: {}/{}".format(i, num_tests))
