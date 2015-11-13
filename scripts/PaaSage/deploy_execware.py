#!/usr/bin/env python
from socket import getfqdn
from execo_g5k.planning import get_planning, compute_slots, find_first_slot,\
    get_jobs_specs, get_job_by_name
from execo_g5k.api_utils import get_g5k_clusters
from execo_g5k.oar import oarsub, get_oar_job_nodes
from execo_g5k.kadeploy import Deployment, deploy
from execo.log import logger, style
from execo.process import SshProcess
from execo.config import default_connection_params
from execo.action import Put

if "lyon" not in getfqdn():
    logger.error('Must be executed from Lyon')
    exit()

job_name = "PaasageExecWare"
packages = "jsvc maven openjdk-8-jdk"
walltime = "3:00:00"
sites = ['lyon', 'rennes', 'nancy', 'grenoble', 'nantes']
source_code = "/home/lpouilloux/src/executionware_backend/"

default_connection_params['user'] = 'root'

logger.info("Looking for a running job")
job = get_job_by_name(job_name, sites)
if not job[0]:
    planning = get_planning(sites)
    blacklisted = ['talc', 'mbi']
    slots = compute_slots(planning, walltime, excluded_elements=blacklisted)
    wanted = {'grid5000': 1}
    start_date, end_date, resources = find_first_slot(slots, wanted)

    for c in filter(lambda x: x in get_g5k_clusters(), resources.keys()):
        if resources[c] > 1:
            wanted = {c: 1}
            break
    jobs_specs = get_jobs_specs(wanted, name=job_name)
    for sub, frontend in jobs_specs:
        sub.walltime = walltime
        sub.job_type = "deploy"
    job = oarsub(jobs_specs)[0]

nodes = get_oar_job_nodes(job[0], job[1])
logger.info('Deploying host %s', nodes[0].address)
deployed, undeployed = deploy(Deployment(nodes,
                                         env_name="jessie-x64-base"))

execware_host = list(deployed)[0]
logger.info('Installing required packages %s', style.emph(packages))
install_packages = SshProcess('apt-get update && apt-get install -y '
                              + packages, execware_host).run()
logger.info('Copying files to host')
put_files = Put(execware_host, [source_code], remote_location="/tmp").run()

xml_file = """
<settings>
     <proxies>
      <proxy>
         <id>g5k-proxy</id>
         <active>true</active>
         <protocol>http</protocol>
         <host>proxy</host>
         <port>3128</port>
       </proxy>
      <proxy>
         <id>g5k-proxy-https</id>
         <active>true</active>
         <protocol>https</protocol>
         <host>proxy</host>
         <port>3128</port>
       </proxy>
     </proxies>
   </settings>
"""

conf_java8 = SshProcess('update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java; '+
                        'update-alternatives --set javac /usr/lib/jvm/java-8-openjdk-amd64/bin/javac;',
                        execware_host).run()

conf_maven_proxy = SshProcess('mkdir /root/.m2 ; echo "' + xml_file +
                              '" > /root/.m2/settings.xml',
                              execware_host).run()
java_home = 'export JAVA_HOME=$(readlink -f /usr/bin/java|sed "s:bin/java::");'
compile_jar = SshProcess(java_home +
                     'cd /tmp/executionware_backend && mvn clean install',
                     execware_host).run()
launch_jar = SshProcess(java_home + ' /tmp/executionware_backend/run.sh',
                    execware_host).start()


