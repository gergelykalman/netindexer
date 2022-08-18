import sys
from datetime import datetime as dt, timedelta as td

from .helper import FileReader

fname = sys.argv[1]
batchsize = int(sys.argv[2])
batch_read_bytes = int(sys.argv[3])

#start = dt.now()
fr = FileReader(fname, batch_read_bytes)
while True:
    batch = fr.get_batch(batchsize)
    if len(batch) == 0:
        break

    for url in batch:
#        pass
        print(url)
#end = dt.now()
#print("Done in {}".format(end-start))
