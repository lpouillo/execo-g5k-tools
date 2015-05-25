#!/usr/bin/env python

import optparse, os, re, fileinput


def draw_results(result_dir, plotfile):
    import matplotlib
    if plotfile:
        matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    data = {} # dict: key = cluster, value = dict: key = problem size, value = tuple([n_cores], [times])

    filename_re = re.compile("^cluster-(\w+)-n_core-(\d+)-size-(\w+)\.out$")
    time_re = re.compile(" Time in seconds =\s*(\S+)")

    for fname in os.listdir(result_dir):
        mo = filename_re.match(fname)
        if mo:
            cluster = mo.group(1)
            n_core = int(mo.group(2))
            size = mo.group(3)

            t = None
            for line in fileinput.input(result_dir + "/" + fname):
                mo2 = time_re.match(line)
                if mo2:
                    t = float(mo2.group(1))

            if cluster not in data: data[cluster] = {}
            if size not in data[cluster]: data[cluster][size] = []
            data[cluster][size].append((n_core, t))

    for i, cluster in enumerate(data):
        plt.figure()
        plt.title(cluster)
        plt.xlabel('num cores')
        plt.ylabel('completion time')
        for size in data[cluster]:
            data[cluster][size].sort(cmp = lambda e, f: cmp(e[0], f[0]))
            plt.plot([ d[0] for d in data[cluster][size] ],
                     [ d[1] for d in data[cluster][size] ],
                     label = "size %s" % (size,))
        plt.legend()
        if plotfile:
            plt.savefig(re.sub('(\.\w+$)', '_%i\\1' % i, plotfile))
    if not plotfile:
        plt.show()

if __name__ == "__main__":
    parser = optparse.OptionParser(usage = "%prog [options] <dir>")
    parser.add_option("-f", dest="plotfile", default = None,
                      help="write plot to image file")
    (options, args) = parser.parse_args()
    if len(args) != 1:
        parser.error("incorrect number of arguments")
    result_dir = args[0]
    draw_results(result_dir, options.plotfile)
