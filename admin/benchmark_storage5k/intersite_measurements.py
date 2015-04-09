#!/usr/bin/env python

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
        """ """
        self.user = getuser()
        sweeper = self.create_param_sweeper()
        logger.info('Starting exploration of %s combinations',
                    len(sweeper.get_remaining()))
        while True:
            comb = sweeper.get_next()
            if not comb:
                break
            logger.info('Performing comb \n%s', pformat(comb))
            try:
                comb_dir = self.result_dir + '/' + slugify(comb)
                storage_job_id = self.get_storage_resources(comb['storage_site'])
                if not storage_job_id:
                    exit()
                job_id, hosts = self.get_compute_hosts(comb['distant_site'])
                if len(hosts) < 10:
                    exit()
                mount_storage = Remote('mount storage5k.%s.grid5000.fr:data/%s_%s /mnt/'
                                        % (comb['storage_site'], self.user,
                                           storage_job_id),
                                        hosts,
                                        connection_params={'user': 'root'}).run()
                if not mount_storage.ok:
                    exit()
                stats = self.benchmark(hosts, bs=comb['bs'])
                f = open(comb_dir + '/read', 'w')
                f.write(stats['read_median'])
                f.close()
                f = open(comb_dir + '/write', 'w')
                f.write(stats['write_median'])
                f.close()
                sweeper.done(comb)
                logger.info('comb %s has been done', pformat(comb))
            except:
                sweeper.cancel(comb)
                logger.error('Comb %s has been canceled', pformat(comb))
            finally:
                logger.info('Destroying jobs')
                oardel([(storage_job_id, comb['storage_site']),
                        (job_id, comb['distant_site'])])

    def get_storage_resources(self, site, data_size=50):
        """ """
        get_chunk_size = SshProcess("storage5k -a chunk_size|cut -f 4 -d ' '",
                                    site).run()
        chunk_size = int(get_chunk_size.stdout[1:])
        number = data_size / chunk_size
        logger.info('Reserving %s chunks on %s', number, site)
        get_storage = SshProcess('storage5k -a add -l chunks=' + str(number) +
                                 ',walltime=1', site).run()
        for s in get_storage.stdout.split('\n'):
            if 'OAR_JOB_ID' in s:
                return int(s.split('=')[1])
        return None

    def get_compute_hosts(self, site, n_nodes=10):
        """ """
        logger.info('Reserving %s nodes on %s', n_nodes, site)
        jobs = oarsub([(OarSubmission(resources="nodes=" + str(n_nodes),
                                      job_type="deploy",
                                      walltime="1:00:00",
                                      name="Bench_storage5k"), site)])
        hosts = get_oar_job_nodes(jobs[0][0], jobs[0][1])
        env_name = "wheezy-x64-nfs"
        logger.info('Deploying %s on %s', env_name, hosts_list(hosts))
        deployed, _ = deploy(Deployment(hosts, env_name=env_name))
        return jobs[0][0], list(deployed)

    def create_param_sweeper(self):
        """ """
        parameters = {'distant_site': get_g5k_sites(),
                      'storage_site': [s for s in get_g5k_sites()
                                       if get_site_attributes(s)['storage5k']],
                      'bs': [1024]}
        logger.info(pformat(parameters))
        combs = sweep(parameters)
        return ParamSweeper(self.result_dir + "/sweeper", combs)

    def benchmark(self, hosts, bs, count=10, n_measure=100):
        read_data = []
        write_data = []
        for i in range(n_measure):
            logger.info('Measure %s', i)
            bench_write = Remote('dd if=/dev/zero of=/mnt/test.out bs=%sk count=% conv=fdatasync iflag=direct'
                                 % (bs, count), hosts).run()
            write_rate = 0
            for p in bench_write.processes:
                write_rate += int(p.stdout.strip().split('\n')[-1].split()[7])
            write_data.append(write_rate)
            logger.info('WRITE %s', write_data)
            bench_read = Remote('dd if=/mnt/test.out of=/dev/null  bs=%sk count=%s conv=fdatasync oflag=direct'
                                % (bs, count), hosts).run()
            read_rate = 0
            for p in bench_read.processes:
                read_rate += int(p.stdout.strip().split('\n')[-1].split()[7])
            read_data.append(read_rate)
            logger.info('WRITE %s', read_data)

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

if __name__ == "__main__":
    e = benchmark_storage5k()
    e.start()
