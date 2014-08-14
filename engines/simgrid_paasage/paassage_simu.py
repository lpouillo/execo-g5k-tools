#!/usr/bin/env python

import os, time, datetime

from threading import Thread
from execo import Put, Remote, Get, sleep
from execo.log import style
from execo.time_utils import timedelta_to_seconds
from execo_g5k import get_host_attributes, get_planning, compute_slots, \
    find_first_slot, distribute_hosts, get_jobs_specs, \
    wait_oargrid_job_start, oargridsub, oargriddel, get_oargrid_job_nodes, \
    Deployment, deploy
    
from execo_engine import Engine, logger, ParamSweeper, sweep, slugify

class paassage_simu(Engine):

    def __init__(self):
        """Overloading class initialization with parent and adding options"""
        super(paassage_simu, self).__init__()
        self.options_parser.set_usage("usage: %prog ")
        self.options_parser.set_description("Execo Engine that can be used to" + \
                "perform automatic virtual machines experiments")
        self.options_parser.add_option("-n", dest="n_nodes",
                    help="maximum number of nodes used",
                    type="int",
                    default=200)
        self.options_parser.add_option("-w", dest="walltime",
                    help="walltime for the reservation",
                    type="string",
                    default="3:00:00")
        self.options_parser.add_option("-j", dest="oargrid_job_id",
                    help="oargrid_job_id to relaunch an engine",
                    type=int)
        self.options_parser.add_option("-k", dest="keep_alive",
                    help="keep reservation alive ..",
                    action="store_true")
        
    def run(self):
        """ """
        if self.options.oargrid_job_id:
            self.oargrid_job_id = self.options.oargrid_job_id
        else:
            self.oargrid_job_id = None
        
        try:
            # Creation of the main iterator which is used for the first control loop.
            self.define_parameters()
        
            job_is_dead = False
            # While they are combinations to treat
            while len(self.sweeper.get_remaining()) > 0:
                # If no job, we make a reservation and prepare the hosts for the experiments
                if self.oargrid_job_id is None:
                    self.make_reservation()
                # Wait that the job starts
                wait_oargrid_job_start(self.oargrid_job_id)
                # Retrieving the hosts and subnets parameters
                self.hosts = get_oargrid_job_nodes(self.oargrid_job_id)
                # Hosts deployment and configuration
                deployment = Deployment(hosts = self.hosts, 
                                        env_file='/home/sirimie/envs/mywheezy-x64-base.env')
                self.hosts, _ = deploy(deployment)
                
                if len(self.hosts) == 0:
                    break

                # Initializing the resources and threads
                available_hosts = [host for host in self.hosts 
                    for i in range(get_host_attributes(host)['architecture']['smt_size'])]
                        
                threads = {}
                # Checking that the job is running and not in Error
                while self.is_job_alive(self.oargrid_job_id) or len(threads.keys()) > 0:
                    job_is_dead = False
                    while self.options.n_nodes > len(available_hosts):
                        tmp_threads = dict(threads)
                        for t in tmp_threads:
                            if not t.is_alive():
                                available_hosts.append(tmp_threads[t]['host'])
                                del threads[t]
                        sleep(5)
                        if not self.is_job_alive(self.oargrid_job_id):
                            job_is_dead = True
                            break
                    if job_is_dead:
                        break

                    # Getting the next combination
                    comb = self.sweeper.get_next()
                    if not comb:
                        while len(threads.keys()) > 0:
                            tmp_threads = dict(threads)
                            for t in tmp_threads:
                                if not t.is_alive():
                                    del threads[t]
                            logger.info('Waiting for threads to complete')
                            sleep(20)
                        break
                    
                    host = available_hosts[0]
                    available_hosts = available_hosts[1:]

                    t = Thread(target=self.workflow,
                               args=(comb, host))
                    threads[t] = {'host': host}
                    t.daemon = True
                    t.start()

                if not self.is_job_alive(self.oargrid_job_id):
                    job_is_dead = True

                if job_is_dead:
                    self.oargrid_job_id = None
            
                
        finally:
            if self.oargrid_job_id is not None:
                if not self.options.keep_alive:
                    logger.info('Deleting job')
                    oargriddel(self.oargrid_job_id)
                else:
                    logger.info('Keeping job alive for debugging')
        
 
    def define_parameters(self):
        """ """
        parameters = {
            'http_c1.xlarge': range(1),
            'http_m1.large': range(1, 4),
            'app_c1.medium': range(1, 6),
            'db_m2.xlarge': range(0, 3),
            'db_m1.medium': range(0, 3)}
        
        sweeps = sweep(parameters)
        self.sweeper = ParamSweeper(os.path.join(self.result_dir, "sweeps"), sweeps)        
        logger.info('Number of parameters combinations %s', len(self.sweeper.get_remaining()))
        

    def make_reservation(self):
        """ """
        logger.info('Performing reservation')
        starttime = int(time.time() + timedelta_to_seconds(datetime.timedelta(minutes=1)))
        planning = get_planning(elements=['grid5000'],
                                starttime=starttime)
        slots = compute_slots(planning, self.options.walltime)
        wanted = { "grid5000": 0 }
        start_date, end_date, resources = find_first_slot(slots, wanted)
        wanted['grid5000'] = min(resources['grid5000'], self.options.n_nodes)
        
        actual_resources = distribute_hosts(resources, wanted, ratio = 0.8)
        job_specs = get_jobs_specs(actual_resources, name='Paasage_Simu') 
        logger.info("try to reserve " + str(actual_resources))
        self.oargrid_job_id , _= oargridsub(job_specs, start_date,
                          walltime = end_date - start_date,
                          job_type = "deploy")


    def workflow(self, comb, host):
        """ """
        comb_ok = False
        thread_name = style.Thread(host.split('.')[0]) + ': '
        logger.info(thread_name + 'Starting combination ' + slugify(comb) )
    
        try:
            # Create the XML file
            
            # Run the code
            
            # Get the results
            comb_dir = self.result_dir + '/' + slugify(comb) + '/'
            try:
                os.mkdir(comb_dir)
            except:
                logger.warning(thread_name +
                    '%s already exists, removing existing files', comb_dir)
                for f in os.listdir(comb_dir):
                    os.remove(comb_dir + f)
                    
            get_results = Get([host], ['file1', 'file2']).run() 
            for p in get_results.processes:
                if not p.ok:
                    logger.error(host +
                        ': Unable to retrieve the files for combination %s',
                        slugify(comb))
                    exit()
            
            comb_ok = True
        finally:
            if comb_ok:
                self.sweeper.done(comb)
                logger.info(thread_name + ': ' + slugify(comb) + \
                             ' has been done')
            else:
                self.sweeper.cancel(comb)
                logger.warning(thread_name + ': ' + slugify(comb) + \
                            ' has been canceled')
        logger.info(style.step('%s Remaining'),
                        len(self.sweeper.get_remaining()))
    

if __name__ == "__main__":
    engine = paassage_simu()
    engine.start()
