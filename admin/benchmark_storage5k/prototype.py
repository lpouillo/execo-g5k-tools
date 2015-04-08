#!/usr/bin/env python
from execo import *
from execo_g5k import *

# define some constants
user = default_frontend_connection_params['user']
storage_site = 'rennes'
distant_site = 'nancy'
logger.info('Benchmarking %s storage from % site', storage_site, distant_site)

# perform a storage reservation
get_chunk_size = SshProcess("storage5k -a chunk_size | cut -f 4 -d ' '", storage_site).run()
chunk_size = int(get_chunk_size.stdout[1:])
number = 50 / chunk_size
get_storage = SshProcess('storage5k -a add -l chunks=' + str(number) +',walltime=2' , storage_site).run()
for s in get_storage.stdout.split('\n'):
    if 'OAR_JOB_ID' in s:
        storage_job_id = int(s.split('=')[1])
        break
logger.info('Storage available on %s: /data/%s_%s', storage_site, user, storage_job_id)
# reserve a node on the distant site and deploy wheezy-x64-nfs
logger.info('Reserving a node on %s', distant_site)
jobs = oarsub([(OarSubmission(resources = "nodes=1", job_type="deploy", walltime="2:00:00", name="Bench_storage5k"), distant_site)])
hosts = get_oar_job_nodes(jobs[0][0], distant_site)
logger.info('Deploying %s', hosts[0].address)
deployed, undeployed = deploy(Deployment(hosts, env_name = "wheezy-x64-nfs"))
hosts = list(deployed)

# mount storage on deployed nodes
logger.info('Mount storage on node')
mount_storage = SshProcess('mount storage5k.' + storage_site + '.grid5000.fr:data/' + user + '_' + str(storage_job_id) +' /mnt/', hosts[0], connection_params= {'user': 'root'}).run()

# perform benchs
logger.info('Perform bench write')
bench_write = SshProcess('dd if=/dev/zero of=/mnt/test.out bs=64M count=200 conv=fdatasync oflag=direct', hosts[0]).run()
print bench_write.stdout.strip().split('\n')[-1].split()[7]
print bench_write.start_date
print bench_write.end_date

logger.info('Perform bench read')
bench_read = SshProcess('dd if=/mnt/test.out of=/tmp/test.out bs=64M count=200 conv=fdatasync oflag=direct', hosts[0]).run()
print bench_read.stdout.strip().split('\n')[-1].split()[7]
print bench_read.start_date
print bench_read.end_date

