#!/usr/bin/env python

import os
import copy
from pprint import pformat
from execo import *
from execo.log import style
from execo_g5k import *
from execo_engine import *
from scipy import stats
from numpy import median, array
from getpass import getuser


class benchmark_storage5k(Engine):
    """An execo engine that perform storage5k benchmark between sites"""

    def run(self):
        """The executor for the experimental workflow
        The basic workflow is the following :
             - Initialise parameters and reserve resources
             - Initialise parameters and reserve resources
             - Initialise parameters and reserve resources
        """
        self.user = getuser() # can be converted to get userID on cmdline - ex. g5kadmin
        sweeper = self.create_param_sweeper()
        logger.info('Starting exploration of %s combinations',
                    len(sweeper.get_remaining()))
        storage = self.get_storage_resources()

        while True:
            comb = sweeper.get_next()
            if not comb:
                break
            job_id, hosts = self.get_compute_hosts(comb['distant_site'])
            if len(hosts) < 10:
                oardel([(job_id, comb['distant_site'])])
                sweeper.cancel(comb)
                break
            while True:
                try:
                    logger.info('Performing comb \n%s', pformat(comb))
                    comb_dir = self.result_dir + '/' + slugify(comb)

                    if not os.path.isdir(comb_dir):
                        os.mkdir(comb_dir)
                    mount_storage = Remote('mount storage5k.%s.grid5000.fr:/data/%s_%s /mnt/'
                                           % (comb['storage_site'], self.user, storage[comb['storage_site']]),
                                           hosts,
                                           connection_params={'user': 'root'},
                                           process_args={'nolog_exit_code': True,
                                                         'ignore_exit_code': True}).run()
                    logger.info('Launching benchmark')
                    stats = self.benchmark(hosts, bs=comb['bs'])

                    # Write the median data to separate files
                    f = open(comb_dir + '/read', 'w')
                    f.write(str(stats['read_median']))
                    f.close()
                    f = open(comb_dir + '/write', 'w')
                    f.write(str(stats['write_median']))
                    f.close()
                    umount_storage = Remote('umount /mnt/', hosts,
                                            connection_params={'user': 'root'}).run()
                    sweeper.done(comb)
                    logger.info('comb %s has been done', pformat(comb))
                except:
                    sweeper.cancel(comb)
                    logger.error('Comb %s has been cancelled', pformat(comb))
                finally:
                    distant_site = comb['distant_site']
                    comb = sweeper.get_next(filtr=lambda r: filter(lambda s: s['distant_site'] == comb['distant_site'], r))
                    if not comb:
                        logger.info('Destroying job')
                        oardel([(job_id, distant_site)])
                        break

            # delete storage jobs
            oardel([(v, s) for s, v in storage.iteritems()])

    def get_storage_resources(self, data_size=50):
        """ Reserve storage on the storage site using storage5k """
        sites = sorted(self.parameters['storage_site'])
        # Get the size of a 'chunk' - this is fixed (generally 10GB) over all sites.
        get_chunk_size = Remote("storage5k -a chunk_size|cut -f 4 -d ' '",
                                sites).run()
        chunks = {}
        for p in get_chunk_size.processes:
            chunk_size = int(p.stdout[1:])
            chunks[p.host.address] = data_size / chunk_size

        logger.info('Reserving chunks on %s', sites)
        storage = {}
        get_storage = Remote('storage5k -a add -l chunks={{chunks.values()}},walltime=168',
                             sites).run()
        for p in get_storage.processes:
            if not p.ok:
                storage[p.host.address] = None
                break
            for s in p.stdout.split('\n'):
                if 'OAR_JOB_ID' in s:
                    storage[p.host.address] = int(s.split('=')[1])
                    break
        return storage
    # End of function get_storage_resources(self, site, data_size)

    def get_compute_hosts(self, site, n_nodes=10):
        """ Reserves 10 nodes for distant_site and deploys on them wheezy-x64-nfs"""
        logger.info('Reserving %s nodes on %s', n_nodes, site)
        walltime = 3600 * len(self.parameters['storage_site'])
        # oarsub reservation of nodes (improve to reserve on same type of nodes (ex. 10G)
        jobs = oarsub([(OarSubmission(resources="nodes=" + str(n_nodes),
                                      job_type="deploy",
                                      walltime=walltime,
                                      name="Bench_storage5k"), site)])

        hosts = get_oar_job_nodes(jobs[0][0], jobs[0][1])
        env_name = "wheezy-x64-nfs"
        logger.info('Deploying %s on %s', env_name, hosts_list(hosts))

        # Deploy the environment 'wheezy-x64-nfs' on reserved nodes
        deployed, _ = deploy(Deployment(hosts, env_name=env_name))
        return jobs[0][0], list(deployed)
    # End of function get_compute_hosts(self, site, n_nodes)

    def create_param_sweeper(self):
        """ Define and initialise parameters """
        self.parameters = {'distant_site': get_g5k_sites(), # site from where file is read
                           'storage_site': [s for s in get_g5k_sites() # site where file store
                                            if get_site_attributes(s)['storage5k']],
                           'bs': [1024]}   # block size for reading file.

        self.parameters['distant_site'].remove('luxembourg')
        self.parameters['distant_site'].remove('toulouse')
        self.parameters['storage_site'].remove('sophia')
        self.parameters['storage_site'].remove('lyon')
        self.parameters['storage_site'].remove('luxembourg')
        logger.info(pformat(self.parameters))

        # Instantiate the 'comb' for sweeping across parameter values
        combs = sweep(self.parameters)
        return ParamSweeper(self.result_dir + "/sweeper", combs)
    # End of function create_param_sweeper(self)

    def benchmark(self, hosts, bs, count=10, n_measure=10):
        """The core function that does the statistical benchmarks and sends results
        2 benchmarks are performed - Read and Write. 
        A large file is read/written 'count' number of times, each time with 
        block size 'bs' . The experiment is repeated 'n_measure' times to calculate 
        a statistical medians separately for read and write.
        Note : The 'dd' function already calculates the mean over 'count' operations.
        Hence, we are calculating the 'median of means' in this function.
        Function parameters:
        hosts :
        bs : block size (in kB, MB or GB) for each read or write phase
        count : no. of times a file is read/written
        n_measure : no. of times read/write experiment is repeated to calculate median
        """
        read_data = []   # array for collecting read rate in each experiment run
        write_data = []   # array for collecting write rate in each experiment run

        for i in range(n_measure):    # repeat the experiment 'n_measure' times
            logger.info('Measure %s', i)
            # Perform the write benchmark
            bench_write = Remote('dd if=/dev/zero of=/mnt/test.out bs=%sk count=%s conv=fdatasync oflag=direct' % (bs, count), hosts).run()
            write_rate = 0
            for p in bench_write.processes:
                write_rate += float(p.stdout.strip().split('\n')[-1].split()[7])
            write_data.append(write_rate)
            logger.info('WRITE %s', write_data)

            # Perform the read benchmark
            bench_read = Remote('dd if=/mnt/test.out of=/dev/null bs=%sk count=%s' % (bs, count), hosts).run()
            read_rate = 0
            for p in bench_read.processes:
                read_rate += float(p.stdout.strip().split('\n')[-1].split()[7])
            read_data.append(read_rate)
            logger.info('READ %s', read_data)

        # Finally return the median values for read and write
        return {'read_median': median(array(read_data)),
                'write_median': median(array(write_data))}


def hosts_list(hosts, separator=' '):
    """Return a formatted string from a list of hosts"""
    tmp_hosts = copy.deepcopy(hosts)
    for i, host in enumerate(tmp_hosts):
        if isinstance(host, Host):
            tmp_hosts[i] = host.address

    return separator.join([style.host(host.split('.')[0])
                           for host in sorted(tmp_hosts)])
# End of function hosts_list(hosts, separator)

if __name__ == "__main__":
    e = benchmark_storage5k()
    e.start()
