from execo import *
from execo_g5k import *
from re import *
from math import *
import os


workingPath = '/home/jrichard/l2c-fft-new-distrib'
logPath = '/home/jrichard/log'
genLadScript = '/home/jrichard/l2c-fft-new-distrib/src/utils/gen-lad/genPencil.py'
params = [
    {
        "site": "nancy", 
        "cluster": "graphene", 
        "coresMin": 64, 
        "coresMax": 64, 
        "dataSizeMin": 256, 
        "dataSizeMax": 256
    },
    {
        "site": "grenoble", 
        "cluster": "edel", 
        "coresMin": 64, 
        "coresMax": 64, 
        "dataSizeMin": 256, 
        "dataSizeMax": 256
    },
]


def isPow2(n):
    if n <= 0:
        return False
    return 2**int(log(n) / log(2) + 0.5) == n


def genLad(site, cluster, cores, px, py, dataSize, transposition):
    Process('cd %s && python %s %d %d %d %d %d %s app.lad' % (workingPath, genLadScript, dataSize, dataSize, dataSize, px, py, transposition)).run()


def removeLad():
    Process('cd %s && rm -f app.lad*' % workingPath).run()


def bench(site, cluster, cores, px, py, dataSize, transposition, results):
    global workingPath
    global logPath

    print "Starting a bench with a %d**3 matrix, %dx%d cores and using a %s transposition on %s:%s..." % (dataSize, px, py, transposition, site, cluster)
    lad = "./n%d-p%dx%d-%s.lad" % (dataSize, px, py, transposition)
    command = "bash -c \"cd %s && l2c_loader -M,-machinefile,/tmp/mfile --mpi -c %d %s\""%(workingPath, cores, lad)
    
    res = Process(command)
    res.stdout_handlers.append(logPath + 
        "/%s:%s-n%d-p%dx%d-%s.log"%(site, cluster, dataSize, px, py, transposition))
    .run()
    if not res.ok:
        print res.stderr
        raise Exception("l2c-assembly process failure")
    if res.stderr != '':
        print "Warning: stderr is not empty:"
        print res.stderr
    outFile = open(logPath+"/%s:%s-n%d-p%dx%d-%s.log"%(site, cluster, dataSize, px, py, transposition), "w")
    outFile.write(res.stdout)
    outFile.close()
    time = float(findall(r'Avg time: ([0-9]*(?:\.[0-9]*)?)', res.stdout)[0])
    print 'Time: ', time
    results.append({
        "site": site, 
        "cluster": cluster, 
        "px": px, 
        "py": py, 
        "dataSize": dataSize, 
        "transposition": transposition,
        "time": time
    })


def main():
    global params
    global logPath

    results = []

    if len(get_current_oar_jobs()) != 0:
        raise Exception("Jobs already started")

    if not os.path.exists(workingPath):
        raise Exception("The working path does not exists")

    if not os.path.exists(logPath):
        raise Exception("The log path does not exists")

    if len(params) == 0:
        print 'Nothing to do'

    for param in params:
        cluster = param["cluster"]
        site = param["site"]
        coresMin = param["coresMin"]
        coresMax = param["coresMax"]
        dataSizeMin = param["dataSizeMin"]
        dataSizeMax = param["dataSizeMax"]
        if not isPow2(coresMin) or not isPow2(coresMax) or not isPow2(dataSizeMin) or not isPow2(dataSizeMax):
            raise Exception("Bad params")
        cores = coresMin
        while cores <= coresMax:
            print 'Reserving for %d cores on %s:%s...' % (cores, site, cluster)
            submission = OarSubmission(resources="core=%s"%cores, sql_properties="cluster='%s'"%cluster)
            job = oarsub([(submission, site)])[0]
            jobId = job[0]
            if not jobId:
                raise Exception("Unable to reserve nodes (bad id)")
            if not wait_oar_job_start(jobId):
                raise Exception("Unable to reserve nodes (wait error)")
            nodes = get_oar_job_nodes(jobId)
            mpiHosts = open('/var/lib/oar/%d'%jobId).read().strip('\n').split('\n')
            # 1 core/node
            #mpiHosts = sorted(set([host+' slots=1' for host in mpiHosts]))
            #print 'Core count: %d'%len(mpiHosts)
            tmpHostFile = open('/tmp/mfile', 'w')
            tmpHostFile.write('\n'.join(mpiHosts))
            tmpHostFile.close()
            dataSize = dataSizeMin
            while dataSize <= dataSizeMax:
                px = 1
                while px <= cores:
                    py = cores / px
                    for transposition in ('XYZ', 'XZY', 'YXZ', 'YZX', 'ZXY', 'ZYX'):
                        genLad(site, cluster, cores, px, py, dataSize, transposition)
                        bench(site, cluster, cores, px, py, dataSize, transposition, results)
                        removeLad()
                    px *= 2
                dataSize *= 2
            cores *= 2
            oardel([job])

    outFile = open(logPath+"/results.log", "w")
    outFile.write(str(results))
    outFile.close()

main()

