#!/usr/bin/env python

import os
import time
import numpy as np
import __main__
from math import ceil
from getpass import getuser
from string import Template
from execo import logger, TaktukRemote, default_connection_params, sleep, \
    Remote, SequentialActions, SshProcess
from execo.log import style
from execo_g5k import get_planning, compute_slots, get_resource_attributes, \
    find_max_slot, OarSubmission, oarsub, wait_oar_job_start, deploy, \
    get_cluster_site, Deployment, get_host_site, get_host_attributes, \
    get_host_shortname, oardel, get_g5k_clusters, find_first_slot, g5k_graph
from execo_g5k.planning import get_job_by_name
from execo_g5k.utils import g5k_args_parser, hosts_list
from execo_engine import copy_outputs

default_connection_params['user'] = 'root'
_sys_grep = '| grep "execution time" | awk \'{print $4}\' | cut -d / -f 1'


def main():
    """Execute a performance check on a given cluster"""

    args = init_options()

    hosts = setup_hosts(get_hosts(args.job, args.cluster, args.walltime,
                                  args.now, args.hosts_file),
                        args.forcedeploy, args.nodeploy, args.env_name,
                        args.packages)

    for test in args.tests.split(',') * args.n_measures:
        clear_cache(hosts)
        logger.info(style.user3('Starting ' + test.upper() + ' test'))
        bench_results = getattr(__main__, test)(hosts)
        print_bench_result(test, bench_results, args.full)
        save_bench_results(test, bench_results, args.outdir)
        logger.info(style.user3(test.upper() + ' test done') + '\n')

    if args.kill_job:
        logger.info('Destroying job')
        job_id, site = get_job_by_name(args.job)
        oardel([(job_id, site)])


def cpu_mono(hosts, max_prime=10000):
    """Execute a stress intensive bench using one core"""
    cmd = 'sysbench --test=cpu --cpu-max-prime=%s run %s' % \
        (max_prime, _sys_grep)
    logger.info('Launching CPU_MONO benchmark with \n%s', style.command(cmd))
    monocore = TaktukRemote(cmd, hosts).run()

    results = parse_hosts_perf(monocore)

    return results


def cpu_multi(hosts, max_prime=100000):
    """Execute a stress intensive using all the core of the machine"""
    n_core = get_host_attributes(hosts[0])['architecture']['smt_size']
    cmd = 'sysbench --num-threads=%s --test=cpu --cpu-max-prime=%s run %s' % \
        (n_core, max_prime, _sys_grep)
    logger.info('Launching CPU_MULTI benchmark with \n%s', style.command(cmd))
    multicore = TaktukRemote(cmd, hosts).run()

    results = parse_hosts_perf(multicore)

    return results


def memory(hosts):
    """Execute a memory intensive test that use the whole memory of the node"""
    n_core = get_host_attributes(hosts[0])['architecture']['smt_size']
    mem_size = get_host_attributes(hosts[0])['main_memory']['ram_size']
    cmd = 'sysbench --test=memory --num-threads=%s --memory-block-size=%s ' \
        'run %s' % (n_core, mem_size, _sys_grep)
    logger.info('Launching MEM benchmark with \n%s', style.command(cmd))
    mem_test = TaktukRemote(cmd, hosts).run()

    results = parse_hosts_perf(mem_test)

    return results


def fio(hosts):
    """Execute sequential read write """
    attr = get_host_attributes(hosts[0])
    n_core = attr['architecture']['smt_size']
    perf = float(attr['performance']['node_flops'])
    filesize = int(ceil(float(perf) / 2. / 10 ** 9))
    if filesize == 0:
        logger.warning('No performance information in Reference API for %s',
                       get_host_shortname(hosts[0]).address)
        filesize = 10
    print filesize
    cmd = Template("cd /tmp && sysbench --num-threads=%s --test=fileio "
                   "--file-total-size=%sG --file-test-mode=seqwr "
                   "$action $grep" % (n_core, filesize))
    logger.info('Preparing FIO benchmark')
    prepare = TaktukRemote(cmd.substitute(action='prepare', grep=""),
                           hosts).run()
    if not prepare.ok:
        logger.error('Unable to prepare the data for FIO benchmark\n%s',
                     '\n'.join([p.host.address + ': ' + p.stout.strip()
                                for p in prepare.processes]))
        exit()
    logger.info('Launching FIO benchmark with \n%s',
                style.command(cmd.substitute(action='run', grep=_sys_grep)))
    run = TaktukRemote(cmd.substitute(action='run', grep=_sys_grep),
                       hosts).run()
    logger.info('Cleaning FIO benchmark')
    clean = TaktukRemote(cmd.substitute(action='cleanup', grep=""),
                         hosts).run()
    if not clean.ok:
        logger.error('Unable to clean the data for FIO benchmark\n%s',
                     '\n'.join([p.host.address + ': ' + p.stout.strip()
                                for p in clean.processes]))
        exit()

    results = parse_hosts_perf(run)

    return results


def lat_gw(hosts, n_ping=10):
    """Measure the latency between hosts and site router"""
    cmd = 'ping -c %s gw-%s |tail -1| awk \'{print $4}\' |cut -d \'/\' -f 2' \
        % (n_ping, get_host_site(hosts[0]))
    logger.info('Executing ping from hosts to site router \n%s',
                style.command(cmd))
    ping_gw = TaktukRemote(cmd, hosts).run()

    results = {}
    for p in ping_gw.processes:
        link = get_host_shortname(p.host.address).split('-')[1] + '->' + \
            p.remote_cmd.split('|')[0].split()[3].strip()
        results[link] = float(p.stdout.strip())

    return results


def lat_hosts(hosts, n_ping=10):
    """Measure latency between hosts using fping"""
    cmd = 'fping -c %s -e -q %s 2>&1 | awk \'{print $1" "$8}\'' % \
        (n_ping, ' '.join([get_host_shortname(h) for h in hosts]))
    logger.info('Executing fping from hosts to all other hosts \n%s',
                style.command(cmd))
    fping = TaktukRemote(cmd, hosts).run()

    results = {}
    for p in fping.processes:
        src = get_host_shortname(p.host.address).split('-')[1]
        for h_res in p.stdout.strip().split('\n'):
            h, tmpres = h_res.split()
            dst = get_host_shortname(h).split('-')[1]
            res = tmpres.split('/')[1]
            if src != dst:
                results[src + '->' + dst] = float(res)

    return results


def bw_frontend(hosts):
    """Sequential measurement of bandwidth between hosts and frontend"""
    frontend = get_host_site(hosts[0])
    f_user = getuser()
    port = '4567'
    with SshProcess('iperf -s -p ' + port, frontend,
                    connection_params={"user": f_user}).start() as iperf_serv:
        iperf_serv.expect("^Server listening", timeout=10)
        logger.info('IPERF server running on %s, launching measurement',
                    style.host(frontend))
        actions = [Remote('iperf -f m -t 5 -c ' + frontend + ' -p ' + port +
                         ' | tail -1| awk \'{print $8}\'', [h]) for h in hosts]
        iperf_clients = SequentialActions(actions).run()
    iperf_serv.wait()

    results = {}
    for p in iperf_clients.processes:
        link = get_host_shortname(p.host.address).split('-')[1] + '->' + \
            frontend
        results[link] = float(p.stdout.strip())

    return results


def bw_oneone(hosts):
    """Parallel measurements of bandwitdh from one host to another"""
    servers = hosts
    clients = [hosts[-1]] + hosts[0:-1]
    logger.info('Launching iperf measurements')
    with TaktukRemote('iperf -s', servers).start() as iperf_serv:
        iperf_serv.expect("^Server listening")
        logger.info('IPERF servers are running, launching measurement')
        iperf_clients = TaktukRemote('iperf -f m -t 30 -c {{servers}}'
                                     '| tail -1 | awk \'{print $7}\'',
                                     clients).run()
    iperf_serv.wait()
    results = {}
    for p in iperf_clients.processes:
        src = get_host_shortname(p.host.address).split('-')[1]
        dst = get_host_shortname(p.remote_cmd.split('|')[0].split()[6].strip()).split('-')[1] 
        link = src + '->' + dst
        results[link] = float(p.stdout.strip())

    return results


def bw_hosts(hosts):
    """Sequential measurements of bandwidth from all hosts to all others"""
    results = {}
    g = g5k_graph(hosts)
    with TaktukRemote('iperf -s', hosts).start() as iperf_serv:
        iperf_serv.expect("^Server listening", timeout=10)
        for src in hosts:
            logger.info('%s to others',
                        style.host(get_host_shortname(src)))
            dests = g.get_host_neighbours(get_host_shortname(src))
            actions = [Remote('iperf -f m -t 5 -c ' + dst +
                              ' | tail -1 | awk \'{print $8}\'', src)
                       for dst in dests]
            iperf_clients = SequentialActions(actions).run()
            for p in iperf_clients.processes:
                link = src.split('-')[1] + '->' + \
                    p.remote_cmd.split('|')[0].split()[6].strip()
                results[link] = float(p.stdout.strip())

    iperf_serv.wait()

    return results


def init_options(args=None):
    """Define the options, set log level and create some default values if
    some options are not set"""

    parser = g5k_args_parser(description="Reserve all the available nodes on "
                             "a cluster and check that nodes exhibits same "
                             "performance for cpu, disk, network",
                             cluster=True,
                             walltime='2:00:00',
                             loglevel=True,
                             job=True,
                             deploy=True,
                             outdir=True)
    default_tests = 'cpu_mono,cpu_multi,memory,fio,' + \
        'lat_gw,bw_frontend,lat_hosts,bw_oneone'
    parser.add_argument('-t', '--tests',
                        default=default_tests,
                        help='comma separated list of tests')
    parser.add_argument('-n', '--n-measures',
                        default=1,
                        type=int,
                        help='number of measures to be run for each test')
    parser.add_argument('--kill-job',
                        action="store_true",
                        help='Kill the job at the end')
    parser.add_argument('--now',
                        action="store_true",
                        help='Use the nodes that are available on the cluster')
    parser.add_argument('--full',
                        action="store_true",
                        help='print all the measures')
    parser.add_argument('--env-name',
                        default="wheezy-x64-prod",
                        help='Select environment name, such as '
                            'jessie-x64-base or user:env_name')
    parser.add_argument('--packages',
                        default="sysbench,fio",
                        help='List of packages to install')
    parser.add_argument('--hosts-file',
                        help='The path to a file containing the list of hosts')

    if not args:
        args = parser.parse_args()

    if args.verbose:
        logger.setLevel('DEBUG')
    elif args.quiet:
        logger.setLevel('WARN')
    else:
        logger.setLevel('INFO')
    if args.cluster not in get_g5k_clusters() and not args.hosts_file:
        logger.error('cluster %s is not a valid g5k cluster, specify it'
                     'with -c ', style.emph(args.cluster))
        exit()
    if args.hosts_file:
        args.cluster = 'custom'

    if isinstance(args.job, bool):
        args.job = 'check_perf_' + args.cluster
    if isinstance(args.outdir, bool):
        args.outdir = args.cluster + '_' + time.strftime("%Y%m%d_%H%M%S_%z")
    if not os.path.exists(args.outdir):
        os.mkdir(args.outdir)
    copy_outputs(args.outdir + '/run_' + args.cluster + '.log',
                 args.outdir + '/run_' + args.cluster + '.log')

    logger.info(style.user3('LAUNCHING PERFORMANCE HOMOGENEITY CHECK'))
    logger.info('%s will be benchmarked using tests %s',
               style.host(args.cluster),
               style.emph(args.tests))

    return args


def setup_hosts(hosts, force_deploy, no_deploy, env_name, packages=None):
    """Deploy a wheezy-x64-prod environment, configure SSH,
    install some packages and """
    logger.info('Deploying hosts')
    check = not force_deploy
    num_tries = int(not no_deploy)

    deployed, undeployed = deploy(Deployment(hosts=hosts,
                                             env_name=env_name),
                                  num_tries=num_tries,
                                  check_deployed_command=check)
    if len(undeployed) > 0:
        logger.warning('%s have not been deployed',
                       hosts_list(list(undeployed)))
    hosts = list(deployed)
    hosts.sort(key=lambda h: (h.split('.', 1)[0].split('-')[0],
                              int(h.split('.', 1)[0].split('-')[1])))

    taktuk_conf = ('-s', '-S',
                   '$HOME/.ssh/id_rsa:$HOME/.ssh/id_rsa,' +
                   '$HOME/.ssh/id_rsa.pub:$HOME/.ssh')
    conf_ssh = TaktukRemote('echo "Host *" >> /root/.ssh/config ;' +
                            'echo " StrictHostKeyChecking no" >> ' +
                            '/root/.ssh/config; ',
                            hosts, connection_params={'taktuk_options':
                                                      taktuk_conf}).run()
    if not conf_ssh.ok:
        logger.error('Unable to configure SSH')
        exit()
    if packages:
        logger.info('Installing ' + style.emph(packages))
        cmd = 'apt-get update && apt-get install -y ' + \
            packages.replace(',', ' ')
        install_pkg = TaktukRemote(cmd, hosts).run()
        if not install_pkg.ok:
            logger.error('Unable to install sysbench')
            exit()

    return hosts


def get_hosts(job_name, cluster, walltime, now=False, hosts_file=None):
    """Retrieve the job from the job_name, perform a new job if none found
    and return the list of hosts"""
    if not hosts_file:
        site = get_cluster_site(cluster)
        if job_name.isdigit():
            job_id = int(job_name)
        else:
            job_id, _ = get_job_by_name(job_name, sites=[site])
        if not job_id:
            job_id, site = _default_job(job_name, cluster, walltime, now)
            logger.info('Reservation done %s:%s', style.host(site),
                        style.emph(job_id))
        logger.info('Waiting for job start')
        wait_oar_job_start(job_id, site)
        job_info = get_resource_attributes('/sites/' + site +
                                           '/jobs/' + str(job_id))
        hosts = job_info['assigned_nodes']

        hosts.sort(key=lambda h: (h.split('.', 1)[0].split('-')[0],
                                  int(h.split('.', 1)[0].split('-')[1])))
        logger.info('Hosts: %s', hosts_list(hosts))
    else:
        hosts = []
        with open(hosts_file) as f:
            hosts.append(f.readline())

    return hosts


def print_bench_result(name, results, full=False):
    """ """
    name = name.upper()
    mean, median, stdev = compute_stats(results)

    logger.info('Stats: '
                + '\n' + style.emph('mean'.ljust(10)) + str(mean)
                + '\n' + style.emph('median'.ljust(10)) + str(median)
                + '\n' + style.emph('stdev'.ljust(10)) + str(stdev))
    error = []
    warning = []
    if '->' not in results.keys()[0]:
        sort_func = lambda h: int(h.split('-')[1])
    else:
        sort_func = lambda h: int(h.split('->')[0])

    for h in sorted(results.keys(),
                    key=sort_func):
        res = results[h]

        if res > (median + 2 * stdev) \
            or res < (median - 2 * stdev):
            if abs((res - median) / median) < 0.10:
                warning.append(h)
                logger.warning('%s %s %s', name, style.host(h).ljust(15),
                               style.report_warn(res))
            else:
                error.append(h)
                logger.error('%s %s %s', name, style.host(h).ljust(15),
                             style.report_error(res))
        elif full:
            logger.info('%s %s %s', name, style.host(h).ljust(15),
                               res)

    if len(error) > 0:
        logger.info('Need to open a bug ?')
    elif len(warning) > 0:
        logger.warning('%s performance is slightly not homogeneous ?',
                       name.upper())
    else:
        logger.info('%s performance is homogeneous', name.upper())


def save_bench_results(test, results, outdir):
    """ """
    base_fname = outdir + '/' + test
    i = 0
    fname = base_fname + '.' + str(i)
    while os.path.exists(fname):
        fname = base_fname + '.' + str(i)
        i += 1
    f = open(fname, 'w')
    f.write('\n'.join([e + '\t' + str(val) for e, val in results.iteritems()]))
    f.close()


def compute_stats(results):
    """ """
    mean = np.mean(np.array(results.values()))
    median = np.median(np.array(results.values()))
    stdev = np.std(np.array(results.values()))

    return mean, median, stdev


def parse_hosts_perf(act):
    """ """
    results = {get_host_shortname(p.host.address): float(p.stdout.strip())
                         for p in act.processes}

    return results


def clear_cache(hosts):
    """ """
    clear = TaktukRemote('killall sysbench; sync; echo 3 > /proc/sys/vm/drop_caches',
                         hosts).run()
    sleep(2)
    return clear.ok


def _default_job(job_name, cluster, walltime, now=False):
    """ """
    logger.info('No job running, making a reservation')
    wanted = {cluster: 0}
    planning = get_planning(wanted.keys())
    slots = compute_slots(planning, walltime)
    if now:
        start_date, _, resources = find_first_slot(slots, wanted)
    else:
        start_date, _, resources = find_max_slot(slots, wanted)
    jobs_specs = [(OarSubmission(resources='{cluster=\'%s\'}/nodes=%s' % 
                                 (cluster, resources[cluster]),
                                 job_type="deploy",
                                 walltime=walltime,
                                 reservation_date=start_date,
                                 name=job_name),
                   get_cluster_site(cluster))]
    return oarsub(jobs_specs)[0]


if __name__ == "__main__":
    main()
