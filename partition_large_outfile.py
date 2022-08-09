import sys
import pickle

infilename = sys.argv[1]
outfilename = sys.argv[2]
batchsize = int(sys.argv[3])


def __open_datalog(name, iteration):
    datalog = open("{}_{:016d}".format(name, iteration), "wb")
    return datalog


iteration, counter = 0, 0
infile = open(infilename, "rb")
datalog = __open_datalog(outfilename, iteration)
done = False
while not done:
    objects = []
    while len(objects) < batchsize:
        try:
            obj = pickle.load(infile)
        except EOFError:
            done = True
            break
        objects.append(obj)

    if len(objects) == 0:
        break

    pickle.dump(objects, datalog)

    print("Rotated logs, iteration: ", iteration)
    datalog.close()
    iteration += 1
    datalog = __open_datalog(outfilename, iteration)
