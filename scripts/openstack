#!/usr/bin/env python

from pprint import pformat
from execo import ProcessLifecycleHandler, Remote, Host
from execo import logger
from execo_g5k import *
import argparse

logger.setLevel('INFO')


class NotifyAtEnd(ProcessLifecycleHandler):

    def __init__(self, message):
        self.message = message

    def end(self, process):
        logger.info(self.message)


####################### Parameters #################
##
## 1 - Configuring the parser
## 2 - Handling undefined arguments


## Configuring the parser

parser = argparse.ArgumentParser(description="Install openstack on G5K",
                                 fromfile_prefix_chars='@')
parser.add_argument("site",
                    help="site on which experiments are carried out")
parser.add_argument("cluster",
                    help="cluster on which experiments are carried out")
parser.add_argument("switch", default="*", nargs='?',
                    help="switch on which nodes are connected")

parser.add_argument("-r", "--reservation", default=False,
                    help="make a reservation", action="store_true")
parser.add_argument("-kad", "--kadeploy", default=False,
                    help="make a kadeployment", action="store_true")
parser.add_argument("-w", "--walltime", default="02:00:00",
                    help="wallTime of the reservation, eg : 8:30:00")
parser.add_argument("-jid", "--job-id", type=int,
                    help="use the reservation id of OpenStack")

parser.add_argument("-omin", "--openstack-min-nodes", type=int,
                    help="minimal number of openstack nodes to deploy")
parser.add_argument("-onn", "--openstack-nodes-number", type=int, default=2,
                    help="number of nodes in the reservation")

parser.add_argument("-c", "--check-only", default=False, action="store_true",
                    help="Check openstack installation (instead of install)")
parser.add_argument("-f", "--openstack-campaign-folder",
                    default="openstack-campaign",
                    help=("Location of the openstack-campaign folder"
                          " on the frontend"))
## Handling arguments

args = parser.parse_args()

logger.debug(args)

site = args.site
cluster = args.cluster
switch = args.switch
walltime = args.walltime

if not args.job_id is None:
    os_job_id = args.job_id
    frontend = site
    os_hosts = get_oar_job_nodes(os_job_id, frontend)
    vlan = get_oar_job_kavlan(os_job_id, frontend)

if args.openstack_min_nodes is None:
    openstack_min_nodes = args.openstack_nodes_number
else:
    openstack_min_nodes = args.openstack_min_nodes

## Warnings: conflicting parameters

if args.check_only and args.kadeploy:
    logger.warning("Option kadeploy and check are both true.")

total_nb_nodes = args.openstack_nodes_number

################ Reservations ###################
##
## 1 - Reserving Openstack nodes
## 2 - Enabling KaVLAN DHCP

user_connexion_params = {
    'user': 'root',
    'default_frontend': site,
    'ssh_options': ('-tt',
                    '-o', 'BatchMode=yes',
                    '-o', 'PasswordAuthentication=no',
                    '-o', 'StrictHostKeyChecking=no',
                    '-o', 'UserKnownHostsFile=/dev/null',
                    '-o', 'ConnectTimeout=45')}

## Reserving Openstack Nodes
if args.reservation:
    logger.info("Reservation for %i nodes, on site/cluster/switch %s/%s/%s"
                " with a walltime of %s" %
                (total_nb_nodes, site, cluster, switch, walltime))

    # Soumission
    logger.info('Performing submission')

    submission = OarSubmission(
        walltime=walltime,
        job_type=["deploy", "destructive"],
        project="openstack_execo",
        name="openstack_%s" % cluster)
    if switch == '*':
        submission.resources = ("{'type=\"kavlan\"'}/vlan=1+"
                                "{'cluster=\"%s\"'}/nodes=%i" %
                                (cluster, total_nb_nodes))
    else:
        submission.resources = ("{'type=\"kavlan\"'}/vlan=1+"
                                "{'cluster=\"%s\" and switch=\"%s\"'}"
                                "/nodes=%i" %
                                (cluster, switch, total_nb_nodes))

    jobs = oarsub([(submission, site)])
    (os_job_id, frontend) = (jobs[0][0], jobs[0][1])
    logger.info("OAR job id is %i", os_job_id)

    wait_oar_job_start(os_job_id, frontend)

## Enabling KaVLAN DHCP
if args.kadeploy:
    logger.info('Enabling DHCP server for the KaVLAN')
    cmd = 'kavlan -e -j %i' % os_job_id
    Remote(cmd, [frontend+'.grid5000.fr']).run()

################ Deployment ###################

if args.kadeploy:
    num_deployment_tries = 2
    checkDeployedCommand = None
    deploy_log = 'Performing'
else:
    num_deployment_tries = 0
    checkDeployedCommand = True
    deploy_log = 'Checking'

logger.info(deploy_log + ' OpenStack Deployment')

os_hosts = get_oar_job_nodes(os_job_id, frontend)
logger.info('hosts: %s', pformat(os_hosts))
vlan = get_oar_job_kavlan(os_job_id, frontend)
logger.info('vlan: %s', vlan)

deployment = Deployment(hosts=os_hosts, env_name="ubuntu-x64-1204-parted",
                        vlan=vlan,
                        other_options=("--set-custom-operations"
                                       " customparted.yml"))

deployed_hosts = deploy(deployment, num_tries=num_deployment_tries,
                        check_deployed_command=checkDeployedCommand)

if len(deployed_hosts[0]) < openstack_min_nodes:
    logger.error('Only %d openstack hosts were correctly deployed (min = %d)',
                 len(deployed_hosts[0]), openstack_min_nodes)
    logger.error('%d Hosts %s were not correctly deployed',
                 len(deployed_hosts[1]), pformat(deployed_hosts[1]))
    exit()
elif len(deployed_hosts[1]) > 0:
    logger.warning('Some Hosts %s were not correctly deployed',
                   pformat(deployed_hosts[1]))
os_hosts = map(lambda h: Host(h.address.replace("."+site+".grid5000.fr",
               "-kavlan-"+str(vlan)+"."+site+".grid5000.fr")),
               deployed_hosts[0])


################ Installing Openstack ###################

if not args.check_only:
    logger.info('Install openstack')

    # Install openstack
    logger.info('Installing openstack')
    os_hosts_str = ''
    Remote("rm -f nodes.txt", [frontend+'.grid5000.fr'],
           connexion_params=default_frontend_connexion_params).run()
    for h in os_hosts:
        Remote("echo %s >> nodes.txt" % h.address, [frontend+'.grid5000.fr'],
               connexion_params=default_frontend_connexion_params).run()

    cmd = ("ruby %s/bin/openstackg5k -m educ -i ~/nodes.txt"
           " -k ~/.ssh/id_rsa.pub" % args.openstack_campaign_folder)
    logger.info(cmd)
    install_openstack_p = Remote(
        cmd, [frontend+'.grid5000.fr'],
        connexion_params=default_frontend_connexion_params)
    install_openstack_p.lifecycle_handlers.append(
        NotifyAtEnd("Openstack installation done."))
    install_openstack_p.run()

################ Checking Openstack ###################

logger.info("Checking Openstack installation")
get_hostname = Remote('if [ -f /etc/nova/nova.conf ];'
                      'then grep "rabbit_host=" /etc/nova/nova.conf'
                      ' | cut -d"=" -f2; else false;fi',
                      os_hosts,
                      connexion_params=user_connexion_params).run()
openstack_controller_fqdn = "UNKNOWN"
openstack_well_installed = True
for p in get_hostname.processes():
    if not p.finished_ok():
        logger.error("Nova doesn't seem well installed on %s!",
                     pformat(p._host))
        openstack_well_installed = False
    else:
        openstack_controller_fqdn = p.stdout().rstrip()

if openstack_well_installed:
    logger.info("Openstack is properly installed. Controller node is %s",
                openstack_controller_fqdn)
