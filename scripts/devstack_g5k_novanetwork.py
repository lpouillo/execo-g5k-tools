#!/usr/bin/env python

import execo as EX
import execo_g5k as EX5
import socket
import string
import re

def host_rewrite_func(host):
    return re.sub("\.grid5000\.fr$", ".g5k", host)

# sites = EX5.get_g5k_sites()
# sites.remove('bordeaux')


EX.logger.setLevel('INFO')
jobs = EX5.get_current_oar_jobs(['reims'])
 
if len(jobs) == 0:
    jobs = EX5.oarsub([( EX5.OarSubmission(resources = "{type=\\'kavlan\\'}/vlan=1+/nodes=2", walltime="3:00:00", job_type ='deploy'), "reims")])
    EX5.wait_oar_job_start( oar_job_id=jobs[0][0], frontend=jobs[0][1])  

print jobs
hosts = EX5.get_oar_job_nodes(jobs[0][0], jobs[0][1])
print hosts
kavlan_id = EX5.get_oar_job_kavlan(jobs[0][0], jobs[0][1])
print kavlan_id
deployment = EX5.Deployment( hosts = hosts, env_file= "ubuntu-x64-1204", vlan = kavlan_id) 

deployed_hosts, undeployed_hosts = EX5.deploy(deployment)
#deployed_hosts, undeployed_hosts = EX5.deploy(deployment, num_tries=0,check_deployed_command=True)

if kavlan_id is not None:
        hosts = [ EX5.get_kavlan_host_name(host, kavlan_id) for host in deployed_hosts ]
print hosts[0]


def get_kavlan_network(kavlan, site):
    """Retrieve the network parameters for a given kavlan from the API"""
    network, mask_size = None, None
    equips = EX5.get_resource_attributes('/sites/' + site + '/network_equipments/')
    for equip in equips['items']:
        if 'vlans' in equip and len(equip['vlans']) > 2:
            all_vlans = equip['vlans']
    for info in all_vlans.itervalues():
        if type(info) == type({}) and 'name' in info \
            and info['name'] == 'kavlan-' + str(kavlan):
            network, _, mask_size = info['addresses'][0].partition('/',)
    EX.logger.debug('network=%s, mask_size=%s', network, mask_size)
    print network, mask_size
    return network, mask_size


def get_kavlan_ip_mac(kavlan, site):
    """Retrieve the network parameters for a given kavlan from the API"""
    network, mask_size = get_kavlan_network(kavlan, site)
    min_2 = (kavlan - 4) * 64 + 2 if kavlan < 8 \
            else (kavlan - 8) * 64 + 2 if kavlan < 10 \
            else 216
    ips = [".".join([str(part) for part in ip]) for ip in
           [ip for ip in get_ipv4_range(tuple([int(part)
                for part in network.split('.')]), int(mask_size))
           if ip[3] not in [0, 254, 255] and ip[2] >= min_2]]
    print ips

def get_ipv4_range(network, mask_size):
    """Get the ipv4 range from a network and a mask_size"""
    net = (network[0] << 24
            | network[1] << 16
            | network[2] << 8
            | network[3])
    mask = ~(2 ** (32 - mask_size) - 1)
    ip_start = net & mask
    ip_end = net | ~mask
    return [((ip & 0xff000000) >> 24,
              (ip & 0xff0000) >> 16,
              (ip & 0xff00) >> 8,
              ip & 0xff)
             for ip in xrange(ip_start, ip_end + 1)]

get_kavlan_ip_mac(5,'reims')
    
def configure_devstack():
    update = EX.Remote('apt-get update;apt-get install python-software-properties -y;add-apt-repository cloud-archive:havana -y; apt-get update;', hosts, connexion_params = {'user': 'root'}).run()

    add_stack_user = EX.Remote('apt-get install -y git sudo;groupadd stack;useradd -g stack -s /bin/bash -d /opt/stack -m stack;'+
                             'echo "stack ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers;'+
                             ' mkdir /opt/stack/.ssh/; cp /root/.ssh/authorized_keys /opt/stack/.ssh/;'+
                             ' chmod 700 ~/.ssh;'+
                             'echo "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCyYjfgyPazTvGpd8OaAvtU2utL8W6gWC4JdRS1J95GhNNfQd657yO6s1AH5KYQWktcE6FO/xNUC2reEXSGC7ezy+sGO1kj9Limv5vrvNHvF1+wts0Cmyx61D2nQw35/Qz8BvpdJANL7VwP/cFI/p3yhvx2lsnjFE3hN8xRB2LtLUopUSVdBwACOVUmH2G+2BWMJDjVINd2DPqRIA4Zhy09KJ3O1Joabr0XpQL0yt/I9x8BVHdAx6l9U0tMg9dj5+tAjZvMAFfye3PJcYwwsfJoFxC8w/SLtqlFX7Ehw++8RtvomvuipLdmWCy+T9hIkl+gHYE4cS3OIqXH7f49jdJf jesse@spacey.local" >> ~/.ssh/authorized_keys', hosts, connexion_params = {'user': 'root'}).run()


    for host in hosts:
        proxy_config = EX.SshProcess("export ip=`/sbin/ifconfig br100 | sed '/inet\ /!d;s/.*r://g;s/\ .*//g'`;"+'echo -e "http_proxy=http://proxy.reims.grid5000.fr:3128/\nhttps_proxy=http://proxy.reims.grid5000.fr:3128/\nip=$ip">> /etc/environment;'
                            ,host, connexion_params = {'user': 'root'},pty = True).run()
        #reset = EX.SshProcess('echo -e "PATH=\\\"/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games\\\"" > /etc/environment;'
                            #,host, connexion_params = {'user': 'root'},pty = True).run()
    
    
    ip_process = EX.Remote('host {{{host}}}', hosts).run()
    
    i=0
    hs=[]
    for p in ip_process.processes():
        
        ip = p.stdout().split(' ')[3]
        ip=str(ip)
        ip=ip.replace('\r\n', '')
        
        hs=hs+[{'host':hosts[i],'ip':ip}]
        i=i+1
    
    ip_split=hs[0]['ip'].split('.') 
    
    
    
    for host in hs:
        no_proxy_config= EX.SshProcess('echo -e "PATH=\\\"/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games\\\"\nno_proxy=\\\"localhost,127.0.0.0,"'+host['ip']+'"\\\"" >> /etc/environment',host['host'], connexion_params = {'user': 'root'},pty = True).run()
        print no_proxy_config.stderr()
    
    download_devstack = EX.Remote('cd ~;git clone https://github.com/openstack-dev/devstack;', hosts, connexion_params = {'user': 'stack'}).run()
    
   
    config_controller=EX.SshProcess('echo -e "HOST_IP=$ip\n"'+
                                    '"FLAT_INTERFACE=eth0\n"'+
                                    '"FIXED_RANGE=10.4.128.0/20\n"'+
                                    '"FIXED_NETWORK_SIZE=4096\n"'+
                                    #'"FLOATING_RANGE="'+ip_split[0]+'"."'+ip_split[1]+'"."'+ip_split[2]+'".128/25\n"'+
                                    '"FLOATING_RANGE=10.36.66.0/26\n"'+
                                    '"MULTI_HOST=1\n"'+
                                    '"LOGFILE=/opt/stack/logs/stack.sh.log\n"'+
                                    '"GIT_BASE=https://github.com\n"'+
                                    '"ADMIN_PASSWORD=pass\n"'+
                                    '"MYSQL_PASSWORD=pass\n"'+
                                    '"RABBIT_PASSWORD=pass\n"'+
                                    '"SERVICE_PASSWORD=pass\n"'+
                                    '"SERVICE_TOKEN=s4c\n"'+
                                    '"DISABLE_SERVICE=n-cpu" > /opt/stack/devstack/localrc', 
                                    hosts[0],connexion_params = {'user': 'stack'}).run()
                                    
                              
    config_compute=EX.Remote('echo -e "HOST_IP=$ip\n"'+
                                    '"FLAT_INTERFACE=eth0\n"'+
                                    '"FIXED_RANGE=10.4.128.0/20\n"'+
                                    '"FIXED_NETWORK_SIZE=4096\n"'+
                                    #'"FLOATING_RANGE="'+ip_split[0]+'"."'+ip_split[1]+'"."'+ip_split[2]+'".128/25\n"'+
                                    '"FLOATING_RANGE=10.36.66.0/26\n"'+
                                    '"MULTI_HOST=1\n"'+
                                    '"LOGFILE=/opt/stack/logs/stack.sh.log\n"'+
                                    '"GIT_BASE=https://github.com\n"'+
                                    '"ADMIN_PASSWORD=pass\n"'+
                                    '"MYSQL_PASSWORD=pass\n"'+
                                    '"RABBIT_PASSWORD=pass\n"'+
                                    '"SERVICE_PASSWORD=pass\n"'+
                                    '"SERVICE_TOKEN=s4c\n"'+
                                    '"DATABASE_TYPE=mysql\n"'+
                                    '"SERVICE_HOST="'+hs[0]['ip']+'"\n"'+
                                    '"MYSQL_HOST="'+hs[0]['ip']+'"\n"'+
                                    '"RABBIT_HOST="'+hs[0]['ip']+'"\n"'+
                                    '"GLANCE_HOSTPORT="'+hs[0]['ip']+'":9292\n"'+
                                    '"ENABLED_SERVICES=n-cpu,n-net,n-api,rabbit,c-sch,c-api,c-vol\n" > /opt/stack/devstack/localrc',
                                    hosts[1:],connexion_params = {'user': 'stack'}).run()
    
    

  
def patch_sps():
    git_download=EX.SshProcess('cd ~/;git clone https://github.com/openstack/nova; cd ~/;git clone https://github.com/openstack/python-novaclient;', 
                                    hosts[0],connexion_params = {'user': 'stack'},pty = True).run()
                                        
    #copy_patch_file = EX.SshProcess('scp devstack/patch/sps-nova-patch.diff stack@'+hosts[0]+':nova/;scp devstack/patch/sps-novaclient-patch.diff stack@'+hosts[0]+':python-novaclient/;',
    #                                jobs[0][1],connexion_params = EX5.config.default_frontend_connexion_params, pty = True).run()
    EX.Put([hosts[0]],'~/workspacenova/devstack/patch/sps-nova-patch.diff', '~/nova', connexion_params = {'user': 'stack'}).run()    
    EX.Put([hosts[0]],'~/workspacenova/devstack/patch/sps-novaclient-patch.diff', '~/python-novaclient', connexion_params = {'user': 'stack'}).run()  
                                    
    patch_git = EX.SshProcess('git config --global user.email "yulinz88@gmail.com";git config --global user.name "Yulin Zhang"; cd ~/nova;git branch sps; git checkout sps; git am < sps-nova-patch.diff;cd ~/python-novaclient;git branch sps; git checkout sps; git am < sps-novaclient-patch.diff;', 
                                     hosts[0],connexion_params = {'user': 'stack'}).run()
    print patch_git._process
    
def modify_novaconf():
       
    config_controller=EX.SshProcess('echo -e "\n[default]\nscheduler_driver = nova.scheduler.security_scheduler.FilterScheduler\n"'+
                                    '"scheduler_available_filters=nova.scheduler.filters.all_filters\n"'+
                                    '"scheduler_available_filters=nova.scheduler.filters.isolation_hosts_filter.IsolationHostsFilter\n"'+
                                    '"scheduler_default_filters=IsolationHostsFilter" >> /etc/nova/nova.conf', 
                                    hosts[0],connexion_params = {'user': 'stack'}).run()
    
def uninstall_devstack():
    
    uninstall_stack=EX.Remote('cd ~/devstack;./unstack.sh', 
                                hosts,connexion_params = {'user': 'stack'},pty = True).run()
 
def install_devstack():                               
    install_stack_controller=EX.SshProcess('cd ~/devstack;./stack.sh', 
                                    hosts[0],connexion_params = {'user': 'stack'},pty = True).run()
                                     
    install_stack_compute=EX.Remote('cd ~/devstack;./stack.sh', 
                                    hosts[1:],connexion_params = {'user': 'stack'},pty = True).run()
 
    for p in install_stack_compute.processes():
        print p.stdout()
 
    print hosts

def configure_devstack_git():
    '''install_stack_compute=EX.Remote('rm ~/devstack/stackrc',
                                    hosts,connexion_params = {'user': 'stack'},pty = True).run()
    '''
    EX.Put(hosts,'~/workspacenova/devstack/configure/stackrc', '~/devstack', connexion_params = {'user': 'stack'}).run() 
    
def backup_patch():
    create_patch_file = EX.SshProcess('cd ~/nova;git format-patch master --stdout > sps-nova-patch.diff; cd ~/python-novaclient;git format-patch master --stdout > sps-novaclient-patch.diff; ', 
                                    hosts[0],connexion_params = {'user': 'stack'},pty = True).run()
    #copy_patch_file = EX.SshProcess('scp stack@'+hosts[0]+':nova/sps-nova-patch.diff devstack/patch/;scp stack@'+hosts[0]+':python-novaclient/sps-novaclient-patch.diff devstack/patch/;',
    #                                jobs[0][1],connexion_params = EX5.config.default_frontend_connexion_params, pty = True).run()
    EX.Get([hosts[0]],'~/nova/sps-nova-patch.diff', '~/workspacenova/devstack/patch', connexion_params = {'user': 'stack'}).run()    
    EX.Get([hosts[0]],'~/python-novaclient/sps-novaclient-patch.diff', '~/workspacenova/devstack/patch', connexion_params = {'user': 'stack'}).run()    


var = raw_input("configure_devstack: taper 1 \npatch_sps: taper 2\nuninstall_devstack: taper 3\n"+
                "install_devstack: taper 4\nbackup_patch: taper 5\nyour choice:")


if var=='1':
    configure_devstack()
elif var=='2':
    patch_sps()
elif var=='3':
    uninstall_devstack()
elif var=='4':
    install_devstack()
elif var=='5':
    backup_patch()
elif var=='0':
    configure_devstack()
    patch_sps()
    install_devstack()
elif var=='6':
    modify_novaconf() 
elif var=='8':  
    configure_devstack_git()                        













