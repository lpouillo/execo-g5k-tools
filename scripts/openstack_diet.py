#!/usr/bin/env python

import re
from pprint import pformat
from execo import ProcessLifecycleHandler, Remote, Local, Host
from execo import logger, wait_all_actions
from execo_g5k import *
import argparse
import time

logger.setLevel('INFO')


class NotifyAtEnd(ProcessLifecycleHandler):

    def __init__(self, message):
        self.message = message

    def end(self, process):
        logger.info(self.message)


def installDIET(site, os_hosts, dietSrcDir, logServiceSrcDir):
    #seed4Cgit = "/home/gverger/git/Seed4C"
    seed4Cgit = "../../../../"
    ####################### Puppet configuration #################
    ##
    ## 1 - Configure network (adding proxy to work with git)
    ## 2 - Copy modules and manifests from Seed4C git directory into node
    ## 3 - Copy diet and logservice sources into node
    ## 4 - Compress (tar) sources into puppet module
    ## 5 - Get libvirt
    logger.info("Installing puppet agent and git on " + pformat(os_hosts))
    mc = Remote("apt-get update", os_hosts,
                connexion_params=user_connexion_params)
    mc.run()

    mc = Remote("apt-get install puppet-common git", os_hosts,
                connexion_params=user_connexion_params)
    mc.run()

    ## Configure Network
    mc = Remote("git config --global http.proxy http://proxy:3128 && \
                git config --global https.proxy https://proxy:3128", os_hosts,
                connexion_params=user_connexion_params)
    mc.run()

    os_hosts = sorted(os_hosts, key=lambda h: h.address)

    ## Modules and Manifests
    logger.info("Configuring modules and copying diet on " + pformat(os_hosts))
    for host in os_hosts:
        hostG5K = host.address.replace(".grid5000.fr", ".g5k")

        Local("rsync -az " + seed4Cgit + "/Tools/diet-puppet/modules root@" +
              hostG5K + ":/etc/puppet/").run()
        Local("rsync -az " + seed4Cgit +
              "/Tools/diet-puppet/master/manifests/site.pp root@" + hostG5K +
              ":/etc/puppet/manifests").run()

        ## Diet and logService sources
        cmd = "rsync -r -t -v -z --delete --exclude '*.git' \
                --filter=':- .gitignore' \
                %s/ root@%s:/tmp/diet/" % (dietSrcDir, hostG5K)
        Local(cmd).run()

        cmd = "rsync -r -t -v -z --delete --exclude '*.git' \
                --filter=':- .gitignore' %s/ root@%s:/tmp/LogService/" %\
              (logServiceSrcDir, hostG5K)
        Local(cmd).run()

    ## Get libvirt

    mc = Remote("rm -rf /etc/puppet/modules/libvirt;\
            git clone https://github.com/puppetlabs/puppetlabs-libvirt.git \
            /etc/puppet/modules/libvirt",
                os_hosts,  connexion_params=user_connexion_params)
    mc.lifecycle_handlers.append(NotifyAtEnd("Libvirt installed"))
    mc.run()
    ####################### Puppet Agents configuration #################
    ##
    ## 1 - Divide puppet agents between Diet nodes and ONE nodes
    ## 2 - Set gems proxy to be able to install ruby modules
    ## 3 - Configure DIET nodes
    ## 4 - Configure ONE nodes

    ## Set gems http proxy
    Remote('echo "gem: --http-proxy http://proxy:3128 --no-ri --no-rdoc" > \
            $HOME/.gemrc', os_hosts,
           connexion_params=user_connexion_params).run()

    ## Configure DIET NODES
    # Puppet agent returns 0 when no changes happened, 2 when changes happened,
    # and 4 or 6 when it failed, and 1 when connection problem
    runs = []
    for host in os_hosts:
        cmd = ("cd /etc/puppet; puppet apply manifests/site.pp"
               " --tags diet,deltacloud")
        logger.info(pformat(host)+" : " + cmd)
        puppetRun = Remote("%s; ok=$?; if [ $ok -ne 2 ]; then exit $ok; fi" %
                           cmd, [host], connexion_params=user_connexion_params)
        puppetRun.start()
        puppetRun.lifecycle_handlers.append(
            NotifyAtEnd("Puppet done on node " + pformat(host)))
        runs.append(puppetRun)
        time.sleep(2)

    wait_all_actions(runs)
    Remote("/etc/init.d/omniorb4-nameserver stop", os_hosts,
           connexion_params=user_connexion_params).run()


def replaceInFile(infilename, outfilename, replacements):
    infile = open(infilename)
    outfile = open(outfilename, 'w')

    for line in infile:
        for src, target in replacements.iteritems():
            line = line.replace(src, target)
        outfile.write(line)
    infile.close()
    outfile.close()


def createConfigFiles(diet_hosts):

    logger.info('Creating GoDIET infrastructure')
    ####################### GoDIET Infrastructure creation #################
    ##
    Local('mkdir -p ' + args.out_dir + '/godiet').run()

    infile = 'expes/godiet/templates/g5kinfra.xml'
    outfile = args.out_dir + '/godiet/infra.xml'
    replacements = {}
    idx = 0
    for i in xrange(1, 6):
        replacements['#NODE' + str(i)+'#'] = diet_hosts[idx].address
        idx = idx + 1
        if idx >= len(diet_hosts):
            idx = 0

    replaceInFile(infile, outfile, replacements)

    Local('cp expes/godiet/templates/g5kdiet.xml %s/godiet/diet.xml' %
          args.out_dir).run()

    ######################## Copying Wrappers ###################
    logger.info('Copying ramses wrappers')
    wrappers_dir = '/root/ramses/wrappers'
    Remote('mkdir -p %s' % wrappers_dir, diet_hosts,
           connexion_params=user_connexion_params).run()
    for h in diet_hosts:
        Local('rsync -az expes/seds/wrappers/ root@%s:%s' %
              (h.address, wrappers_dir)).run()

    replacements = {}
    replacements['#WRAPPERS_DIR#'] = wrappers_dir
    replaceInFile('expes/seds/ramses_config.xml',
                  '%s/ramses_config.xml' % args.out_dir, replacements)

    replaceInFile('expes/seds/b_8.xml', args.out_dir + '/b_8.xml',
                  {'#CONFIG_XML#': args.out_dir + '/ramses_config.xml'})
    Local('cp expes/seds/8.xml ' + args.out_dir + '/8.xml').run()

    # TODO
    # Chercher le openstack enpoint (dans openrc) et creer un script pour
    # lancer deltacloud + godiet


####################### Parametres #################
##
## 1 - Setting arguments in the parser
## 2 - Handling undefined arguments


## Setting args in the parser

parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
parser.add_argument("site",
                    help="site on which experiments are carried out")
parser.add_argument("cluster",
                    help="cluster on which experiments are carried out")

parser.add_argument("-r", "--reservation",
                    help="make a reservation", action="store_true")
parser.add_argument("-kad", "--kadeploy",
                    help="make a kadeployment", action="store_true")
parser.add_argument("-w", "--walltime",
                    help="wallTime of the reservation, eg : 8:30:00")
parser.add_argument("-s", "--switch",
                    help="use a specified switch")
parser.add_argument("-jid", "--reservation_id", type=int,
                    help="use the reservation id of OpenStack")

parser.add_argument("-omin", "--openstack_min_nodes", type=int,
                    help="minimal number of openstack nodes to deploy")
parser.add_argument("-onn", "--openstack_nodes_number",
                    help="number of nodes in the reservation for opennenebula")
parser.add_argument("--openstack_install", type=int,
                    help="install openstack from step X,\
                            use 0 to process from the begining,\
                            step 1 is ramses config,\
                            step 2 is diet config,\
                            step 1000 just openstack deploy",
                    choices=[0, 1, 2, 1000])
parser.add_argument("--openstack_env",
                    help="openstack environment to deploy by default :\
                        /home/latoch/ubuntu-1204-server-with-puppet-agent.env")
parser.add_argument("-sbadia",
                    help="make a kadeployment with S. Badia's ubuntu-x64-1204",
                    action="store_true")
parser.add_argument("-essex",
                    help="use openstack essex", action="store_true")

parser.add_argument("-dnn", "--diet_nodes_number",
                    help="number of nodes in the reservation for diet")
parser.add_argument("-dmin", "--diet_min_nodes", type=int,
                    help="minimal number of diet nodes to deploy")
parser.add_argument("--diet_env",
                    help="diet environment to deploy by default :\
                            /home/latoch/rich-ubuntu-1204.env")
parser.add_argument("--diet_source_dir",
                    help="path of the local diet source directory")
parser.add_argument("--logservice_source_dir",
                    help="path of the local logservice source directory")

parser.add_argument("--out_dir",
                    help="where to put needed files for expe run")


## Handling arguments

args = parser.parse_args()

site = args.site
cluster = args.cluster

if args.reservation:
    make_reservation_01 = True
else:
    make_reservation_01 = False

if not args.walltime is None:
    walltime = args.walltime
    make_reservation_01 = True
else:
    walltime = '10:00:00'

if not args.openstack_nodes_number is None:
    openstack_nodes_number = args.openstack_nodes_number
    make_reservation_01 = True
else:
    openstack_nodes_number = '2'

if not args.switch is None:
    switch = args.switch
else:
    switch = '*'

if not args.diet_nodes_number is None:
    make_reservation_01 = True
    diet_nodes_number = args.diet_nodes_number
else:
    diet_nodes_number = "1"

if not args.reservation_id is None:
    make_reservation_01 = False
    (os_job_id, frontend) = (args.reservation_id, site)
    os_hosts = get_oar_job_nodes(os_job_id, frontend)
    vlan = get_oar_job_kavlan(os_job_id, frontend)

"""if not args.openstack_reservation_id is None:
    make_reservation_01 = False
    oid = args.openstack_reservation_id
    (os_job_id, frontend) = (oid, site)
    vlan = get_oar_job_kavlan(os_job_id, frontend)
    """
if args.openstack_min_nodes is None:
    openstack_min_nodes = int(openstack_nodes_number)
else:
    openstack_min_nodes = args.openstack_min_nodes
"""
if not args.diet_reservation_id is None:
    make_reservation_01 = False
    did = args.diet_reservation_id
    (diet_job_id, frontend) = (did, site)
    diet_hosts = get_oar_job_nodes(diet_job_id, frontend)
    diet_nodes_number = len(diet_hosts)
else :
    diet_job_id = -1
"""
if args.diet_min_nodes is None:
    diet_min_nodes = int(diet_nodes_number)
else:
    diet_min_nodes = args.diet_min_nodes

if args.kadeploy:
    make_kadeploy = True
else:
    make_kadeploy = False


if not args.openstack_install is None:
    openstack_install = args.openstack_install
else:
    openstack_install = -1


if not args.openstack_env is None:
    openstack_env = args.openstack_env
else:
    openstack_env = "~/ubuntu-1204-server-with-puppet-agent.env"

if not args.diet_env is None:
    diet_env = args.diet_env
else:
    diet_env = "~/rich-ubuntu-1204.env"


if args.out_dir is None:
    args.out_dir = "/tmp/ramses/" + site + "/" + cluster
Local("mkdir -p " + args.out_dir).run()

user_connexion_params = {
    'user': 'root',
    'default_frontend': site,
    'ssh_options': ('-tt',
                    '-o', 'BatchMode=yes',
                    '-o', 'PasswordAuthentication=no',
                    '-o', 'StrictHostKeyChecking=no',
                    '-o', 'UserKnownHostsFile=/dev/null',
                    '-o', 'ConnectTimeout=45')}

total_nb_nodes = int(diet_nodes_number) + int(openstack_nodes_number)


################ Reservations ###################
##
## 1 - Reserving Openstack nodes
## 2 - Reserving Diet nodes
## 3 - Enabling KaVLAN DHCP

## Reserving Openstack Nodes
if make_reservation_01:
    logger.info("Making a reservation on site " + site + " during " +
                walltime + " with " + openstack_nodes_number + " nodes " +
                "and switch=" + switch + " and cluster=" + cluster +
                " for openstack " + diet_nodes_number + " nodes for DIET")

    # Soumission
    logger.info('Performing submission')

    if switch == '*':
        submission = OarSubmission(
            resources=("{'type=\"kavlan\"'}/vlan=1+"
                       "{'cluster=\"%s\"'}/nodes=%i" %
                       (cluster, total_nb_nodes)),
            walltime=walltime,
            job_type=["deploy", "destructive"],
            project="openstack_Seed4C",
            name="openstack_%s" % cluster)
    else:
        submission = OarSubmission(
            resources=("{'type=\"kavlan\"'}/vlan=1+"
                       "{'cluster=\"%s\" and switch=\"%s\"'}/nodes=%i" %
                       (cluster, switch, total_nb_nodes)),
            walltime=walltime,
            job_type=["deploy", "destructive"],
            project="Seed4C",
            name="seed4c_%s" % cluster)

    jobs = oarsub([(submission, site)])
    (os_job_id, frontend) = (jobs[0][0], jobs[0][1])

    wait_oar_job_start(os_job_id, frontend)

## 3 - Enabling KaVLAN DHCP
if make_reservation_01 or make_kadeploy:
    logger.info('Enabling DHCP server for the KaVLAN')
    cmd = 'kavlan -e -j '+str(os_job_id)
    Remote(cmd, [frontend+'.grid5000.fr']).run()

################ Deployment ###################
##
## 1 - Deploying Openstack nodes
## 2 - Deploying Diet nodes


if make_kadeploy:
    num_deployment_tries = 2
    checkDeployedCommand = None
    # Deploiement
    deploy_log = 'Performing'
else:
    num_deployment_tries = 0
    checkDeployedCommand = True
    deploy_log = 'Checking'

## Deploying Openstack nodes

logger.info(deploy_log + ' OpenStack Deployment')

os_hosts = get_oar_job_nodes(os_job_id, frontend)
diet_hosts = []
for i in xrange(0, int(diet_nodes_number)):
    diet_hosts.append(os_hosts.pop())

#os_hosts = get_oar_job_nodes(os_job_id, frontend)
logger.info('hosts: %s', pformat(os_hosts))
vlan = get_oar_job_kavlan(os_job_id, frontend)
logger.info('vlan: %s', vlan)

if args.sbadia:
    deployment = Deployment(hosts=os_hosts, env_name="ubuntu-x64-1204-parted",
                            vlan=vlan,
                            other_options=("--set-custom-operations"
                                           " customparted.yml"))
else:
    deployment = Deployment(
        hosts=os_hosts, env_file=openstack_env, vlan=vlan,
        other_options=("-p 5 --force-steps 'SetDeploymentEnv|"
                       "SetDeploymentEnvUntrusted:1:1000&BroadcastEnv|"
                       "BroadcastEnvKastafior:1:1000&BootNewEnv|"
                       "BootNewEnvClassical:1:1000'"))

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

if openstack_install >= 0:
    logger.info('Install openstack and Configure Ramses experiments')

    if args.essex:
        folder = 'openstack-essex'
    else:
        folder = 'openstack-campaign'

    # Install openstack
    logger.info('[common step] Installing openstack')
    os_hosts_str = ''
    Remote("rm -f nodes.txt", [frontend+'.grid5000.fr'],
           connexion_params=default_frontend_connexion_params).run()
    for h in os_hosts:
        Remote("echo %s >> nodes.txt" % h.address, [frontend+'.grid5000.fr'],
               connexion_params=default_frontend_connexion_params).run()

    cmd = ("ruby %s/bin/openstackg5k -m educ -i ~/nodes.txt"
           " -k ~/.ssh/id_rsa.pub" % folder)
    logger.info(cmd)
    install_openstack_p = Remote(
        cmd, [frontend+'.grid5000.fr'],
        connexion_params=default_frontend_connexion_params)
    install_openstack_p.lifecycle_handlers.append(
        NotifyAtEnd("Openstack installed "))
    install_openstack_p.start()

## As it is very long process, install DIET in the mean time
## Deploying Diet nodes
logger.info(deploy_log + ' Diet Deployment')
#   diet_hosts = get_oar_job_nodes(diet_job_id, frontend)
logger.info('diet_hosts: %s', pformat(diet_hosts))
diet_deployment = Deployment(hosts=diet_hosts, env_file=diet_env, vlan=vlan)
diet_deployed_hosts = deploy(diet_deployment, num_tries=num_deployment_tries,
                             check_deployed_command=checkDeployedCommand)

if len(diet_deployed_hosts[0]) < diet_min_nodes:
    logger.error('Some diet_Hosts %s were not correctly deployed',
                 pformat(diet_deployed_hosts[1]))
    exit()
elif len(diet_deployed_hosts[1]) > 0:
    logger.warning('Some diet_Hosts %s were not correctly deployed',
                   pformat(diet_deployed_hosts[1]))

diet_hosts = diet_deployed_hosts[0]

kavlanDietHosts = map(lambda h: Host(h.address.replace("."+site+".grid5000.fr",
                      "-kavlan-"+str(vlan)+"."+site+".grid5000.fr")),
                      diet_hosts)
installDIET(site, kavlanDietHosts, args.diet_source_dir,
            args.logservice_source_dir)
createConfigFiles(kavlanDietHosts)
logger.info('Diet is installed')

if openstack_install >= 0:
    install_openstack_p.wait()
    for p in install_openstack_p.processes():
        for line in p.stdout().split('\n'):
            if "It's ok ! " in line:
                print line
                regexp = re.compile(".*@(.*\."+site+"\.grid5000\.fr)")
                matches = regexp.match(line)
                openstack_controller_fqdn = matches.groups()[0]

    # Launching cirros
    #cmd =  'bash /tmp/nova.sh cirros'
    #launch = Remote(cmd, [Host(controller)])
    #openstack_controller_fqdn = controller;
    openstack_controller_without_suffix = openstack_controller_fqdn.replace(
        "."+site+".grid5000.fr", "")
    if openstack_install <= 0:
        logger.info('[step 0] copying ramses VM into openstack controller')
        cmd0 = "./setup-0.sh " + site + " " + openstack_controller_fqdn
        logger.info(cmd0)
        setup0 = Local(cmd0)
        setup0.lifecycle_handlers.append(
            NotifyAtEnd("Ramses VM copied on %s." %
                        openstack_controller_fqdn))
        setup0.run()
    if openstack_install <= 1:
        logger.info('[step 1] configuring openstack for Ramses')
        cmd1 = ("./setup-1.sh %s %s" %
                (site, openstack_controller_without_suffix))
        setup1 = Local(cmd1)
        setup1.lifecycle_handlers.append(
            NotifyAtEnd("Ramses Openstack configured"))
        setup1.run()


#################################
### DIET

# One of the openstack nodes
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

if not openstack_well_installed:
    exit(1)

openstack_controller_without_suffix = openstack_controller_fqdn.replace(
    "."+site+".grid5000.fr", "")

logger.info('[step 2] preparing prerequisite for DIET')
for i in diet_hosts:
    diet_host_fqdn_without_suffix = i.address.replace("."+site+".grid5000.fr", "-kavlan-"+str(vlan))
    cmd4 = "./setup-diet.sh " + site + " " + openstack_controller_without_suffix + " " + diet_host_fqdn_without_suffix
    cmd = Local(cmd4)
    cmd.lifecycle_handlers.append(NotifyAtEnd("Diet node " + pformat(i) + " linked to openstack"))
    cmd.run()


get_endpoint=Remote('cat openrc | grep SERVICE_ENDPOINT | cut -d"=" -f2', [Host(openstack_controller_fqdn)], connexion_params=user_connexion_params)
get_endpoint.run()
for p in get_endpoint.processes():
    #logger.setLevel('DEBUG')
    logger.info('Openstack endpoint is ' + p.stdout())
    endpoint=p.stdout().rstrip()
    Remote('killall -q deltacloudd; true', kavlanDietHosts, connexion_params=user_connexion_params).run()
    deltacloudCmd = Remote('nohup deltacloudd -i openstack -r 0.0.0.0 -P '+ endpoint+' 1> deltacloud.log.stdout 2> deltacloud.log.stderr',
                        kavlanDietHosts, connexion_params=user_connexion_params)
    deltacloudCmd.start()

get_ip=Remote("host "+openstack_controller_fqdn + " | awk '{ print $NF }'", [Host(openstack_controller_fqdn)], connexion_params=user_connexion_params).run()
for p in get_ip.processes():
    os_ip=p.stdout().rstrip()
    Remote("ip route list 10.0.0.0/24 | grep '10.0.0.0/24 via'; if [ $? == 1 ]; then ip route add 10.0.0.0/24 via "+os_ip+"; fi",
        kavlanDietHosts, connexion_params=user_connexion_params).run()

#user_connexion_params={'user': 'root', 'default_frontend': site,
#                               'ssh_options': ('-tt', '-o', 'BatchMode=yes', '-o', 'PasswordAuthentication=no', '-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null', '-o', 'ConnectTimeout=45')}

#Put([Host(openstack_controller_fqdn)], "getip.sh", connexion_params = user_connexion_params).run()

#cmd=Remote("sh getip.sh",[Host(openstack_controller_fqdn)],connexion_params = user_connexion_params)
#cmd.run();
#for p in cmd.processes():
#   controllerIP = p.stdout()

#logger.info("Controller ip is " + controllerIP)
#Remote("ip route add 10.0.0.0/24 via " + controllerIP, diet_hosts, connexion_params=user_connexion_params).run()


#for h in diet_hosts:
#   diet_host_fqdn_without_suffix = i.address.replace("."+site+".grid5000.fr", "-kavlan-"+str(vlan))
#   Remote("scp tmp-openstack-controller-pubkey root@"+diet_host_fqdn_without_suffix+":")
