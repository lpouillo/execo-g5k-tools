#!/usr/bin/env python
from pprint import pformat
from execo import *
from execo_g5k import *
from execo_engine import *


class benchmark_storage(Engine):
    """An execo engine that performs storage benchmark of several
    grid5000 clusters """

    def __init__(self):
        super(benchmark_storage, self).__init__()
#         self.options_parser.add_option("-k", dest="keep_alive",
#                     help="keep reservation alive ..",
#                     action="store_true")
        self.options_parser.add_option("-j", dest="job_id",
                    help="job_id to relaunch an engine",
                    type=int)
        self.options_parser.add_argument("cluster",
                        "The cluster on which to run the experiment")
        self.walltime = "1:00:00"

    def create_paramsweeper(self):
        size_single = [32768, 32768 * 5]
        size_multi = [96, 480, 1024]
        parameters = {
            'operation': ['read', 'write'],
            'io_engine': ['sync'],
            'io_scheduler': ['noop'],
            'direct_io': [0, 1],
            'bs': [32768],
            'numjobs': {i: {'size': size_single} if i == 1
                        else {'size': size_multi}
                        for i in range(1, 16)}

            }
        logger.info('Defining parameters: %s', pformat(parameters))
        combs = sweep(parameters)
        return ParamSweeper(self.result_dir + "/sweeper", combs)

    def run(self):
        logger.info('Defining parameters')
        sweeper = self.create_paramsweeper()
        # The argument is a cluster
        self.cluster = self.args[0]
        # Analyzing options
        self.host = self.setup_hosts(self.cluster)
        while len(sweeper.get_remaining()) > 0:
            comb = sweeper.get_next()
            if comb['size'] <= comb['bs'] and comb['direct_io'] == 1:
                logger.info('Skipping ' + pformat(comb))
                sweeper.skip(comb)
            else:
                logger.info('Treating ' + pformat(comb))
                bench = self.generate_fio(comb).run()
                self.save_results(comb, bench)
                sweeper.done(comb)

    def save_results(self, comb, bench):
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
        job_outputs = bench.stdout.split('\n')
        f = open(slugify(comb) + '.csv', 'w')
        for output in job_outputs[0:-1]:
            result = output.split(';')
            f.write(';'.join([self.host.address, str(int(bench.start_date)), 
                str(comb['direct_io']), comb['io_engine'], comb['io_scheduler'],\
            result[col_number['common']['error']], comb['operation'], \
            str(comb['numjobs']), str(comb['bs']), str(comb['size']),\
            result[col_number[comb['operation']]['runtime']],\
            result[col_number[comb['operation']]['bw']],\
            result[col_number[comb['operation']]['bw_min']],\
            result[col_number[comb['operation']]['bw_max']], \
            result[col_number[comb['operation']]['lat']],\
            result[col_number[comb['operation']]['lat_min']],\
            result[col_number[comb['operation']]['lat_max']],\
            result[col_number[comb['operation']]['iops']]]))
        f.close()

        logger.info('Cleaning /tmp diretory')
        bench = SshProcess('rm -rf /tmp/data*', self.host).run()

    def setup_hosts(self, cluster):
        self.frontend = get_cluster_site(self.cluster)
        if self.options.job_id:
            self.job_id = self.options.job_id
        else:
            logger.info('Submitting a job on ' + self.frontend)
            jobs = oarsub([(OarSubmission(resources="{cluster='" + cluster + "'}/nodes=1",
                  walltime=self.walltime, job_type='deploy'),
                  self.frontend)])
            self.job_id = jobs[0][0]
        wait_oar_job_start(self.job_id, self.frontend)
        logger.info('Job %s has started on %s, retrieving nodes',
                    self.job_id, self.frontend)
        nodes = get_oar_job_nodes(self.job_id, self.frontend)
        logger.info('Deploying on node %s', nodes[0].address)
        deployed, undeployed = deploy(Deployment(nodes,
                        env_name="wheezy-x64-base"))
        logger.info('Installing fio')
        cmd = 'export DEBIAN_MASTER=noninteractive ; apt-get update && apt-get ' + \
              'install -y --force-yes fio'

        install = SshProcess(cmd, nodes[0]).run()

        return nodes[0]

    def generate_fio(self, comb):
        """ """
        cmd_bench = 'cd /tmp/ && fio --name=data --numjobs=' + str(comb['numjobs']) \
            + ' --ioengine=' + comb['io_engine'] + \
        ' --ioscheduler=' + comb['io_scheduler'] + ' --rw=' + comb['operation'] + \
        ' --bs=' + str(comb['bs']) +\
        ' --direct=' +str(comb['direct_io']) + ' --size=' + str(comb['size']) + 'k --minimal'
        return SshProcess(cmd_bench, self.host)

if __name__ == "__main__":
    e = benchmark_storage()
    e.start()
