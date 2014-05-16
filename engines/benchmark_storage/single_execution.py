#!/usr/bin/env python
from pprint import pprint
from random import randint
from execo import logger, SshProcess, Put, Get
from execo_g5k import *

logger.setLevel('INFO')
cluster = 'petitprince'
# walltime = "2:00:00"
# jobs = oarsub([
#   (OarSubmission(resources="{cluster='" + cluster + "'}/nodes=1",
#                  walltime=walltime, job_type='deploy'),
#                  get_cluster_site(cluster))])
jobs = [(49083, get_cluster_site(cluster))]
if jobs[0][0]:
    wait_oar_job_start(jobs[0][0], jobs[0][1])
    logger.info('Job %s has started on %s, retrieving nodes', jobs[0][0], jobs[0][1])
    nodes = get_oar_job_nodes(jobs[0][0], jobs[0][1])
    logger.info('Deploying on node %s', nodes[0].address)
    deployed, undeployed = deploy(Deployment(nodes, env_name="wheezy-x64-base"))

    logger.info('Installing fio')
    cmd = 'export DEBIAN_MASTER=noninteractive ; apt-get update && apt-get ' + \
          'install -y --force-yes fio'
    install = SshProcess(cmd, nodes[0]).run()

    col_number = {'common': {'output_version': 1, 'fio_version': 2, 'error': 5},
                  'read': {'runtime': 9, 'bw': 7, 'bw_min': 42, 'bw_max': 43,
                           'lat': 40, 'lat_min': 38, 'lat_max': 39,
                           'iops': 8},
                  'write': {'runtime': 50, 'bw': 48, 'bw_min': 83, 'bw_max': 84,
                           'lat': 81, 'lat_min': 79, 'lat_max': 80,
                           'iops': 49}}
    # Reduce col index by one
    for values in col_number.itervalues():
        for key, val in values.iteritems():
            values[key] = val - 1
    numjobs = randint(1, 16)
    io_engine = 'sync'
    io_scheduler = 'noop'
    operation = 'read'
    bs = 32768
    direct_io = 1
    size = 2048
    cmd_bench = 'cd /tmp/ && fio --name=data --numjobs=' + str(numjobs) + ' --ioengine=' + io_engine + \
        ' --ioscheduler=' + io_scheduler + ' --rw=' + operation + ' --bs=' + str(bs) +\
        ' --direct=' +str(direct_io) + ' --size=' + str(size) + 'k --minimal'
    logger.info('Launching bench command \n' + cmd_bench)
    bench = SshProcess(cmd_bench, nodes[0]).run()

    job_outputs = bench.stdout.split('\n')
    f = open('test.csv', 'w')
    for output in job_outputs[0:-1]:
        result = output.split(';')
        f.write(';'.join([nodes[0].address, str(int(bench.start_date)), str(direct_io), io_engine, io_scheduler,\
        result[col_number['common']['error']], operation, str(numjobs), str(bs), str(size),\
        result[col_number[operation]['runtime']], result[col_number[operation]['bw']],\
        result[col_number[operation]['bw_min']], result[col_number[operation]['bw_max']], \
        result[col_number[operation]['lat']], result[col_number[operation]['lat_min']], \
        result[col_number[operation]['lat_max']], result[col_number[operation]['iops']]]))
    f.close()
    logger.info('Cleaning /tmp diretory')
    bench = SshProcess('rm -rf /tmp/data*', nodes[0]).run()
# oardel([(jobs[0][0], jobs[0][1])])


