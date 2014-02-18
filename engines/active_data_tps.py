#!/usr/bin/env python

import logging, time, datetime, signal
import pprint as PP, os, sys, math
import execo as EX
from execo.process import ProcessOutputHandler
import execo_g5k as EX5
from execo_g5k.api_utils import get_cluster_site
from execo_engine import Engine, ParamSweeper, logger, sweep, slugify
EX.logger.setLevel(logging.ERROR)
logger.setLevel(logging.ERROR)

NORMAL			= "\x1B[0m"
GREEN			= "\x1B[32m"
BOLD_MAGENTA	= "\x1B[35;1m"
RED				= "\x1B[31m"

OUT_FILE_FORMAT = 'events_per_sec_{0}W_{1}T'

# Setup signal handler
def sighandler(signal, frame):
	if active_data_tps._job is not None:
		print("\nInterrupted: killing oar job " + str(active_data_tps._job))
		EX5.oar.oardel(active_data_tps._job)
	else:
		print("\nInterrupted: no job to kill")
	sys.exit(0)

signal.signal(signal.SIGINT, sighandler)
signal.signal(signal.SIGQUIT, sighandler)
signal.signal(signal.SIGTERM, sighandler)

class active_data_tps(Engine):
	_stat = ""
	_job = None
	
	def __init__(self):
		super(active_data_tps, self).__init__()
	
	def _updateStat(self, stat):
		self.__class__._stat = ""
		
		for nc in self.parameters['n_clients']:
			done = 0
			if stat['done'] and stat['done']['n_clients'] and nc in stat['done']['n_clients']:
				done = stat['done']['n_clients'][nc]
			total = stat['total']['n_clients'][nc]
			
			if done == total:
				color = GREEN
			else: color = RED
			self.__class__._stat += "{0}[{1}: {2}/{3}]{4} ".format(color, nc, done, total, NORMAL)
	
	@classmethod
	def _log(cls, message, new_line = True):
		if new_line:
			sys.stdout.write('\n')
		else:
			sys.stdout.write('\r')
		
		sys.stdout.write(cls._stat)
		sys.stdout.write(message)
		sys.stdout.flush()
		
	
	def run(self):
		# Defining experiment parameters
		self.parameters = {
			'n_clients': [400, 450, 500, 550, 600],
			'n_transitions': [10000]
		}
		cluster = 'griffon'
		sweeps = sweep(self.parameters)
		sweeper = ParamSweeper(os.path.join(self.result_dir, "sweeps"), sweeps)
		server_out_path = os.path.join(self.result_dir, "server.out")
		
		self._updateStat(sweeper.stats())
		
		# Loop on the number of nodes
		while True:
			# Taking the next parameter combinations
			comb = sweeper.get_next()
			if not comb: break

			# Performing the submission on G5K
			site = get_cluster_site(cluster)
			self._log("Output will go to " + self.result_dir)
			
			n_nodes = int(math.ceil(float(comb['n_clients']) / EX5.get_host_attributes(cluster + '-1')['architecture']['smt_size'])) + 1
			self._log("Reserving {0} nodes on {1}".format(n_nodes, site))
			
			resources = "{cluster=\\'" + cluster + "\\'}/nodes=" + str(n_nodes)
			submission = EX5.OarSubmission(resources = resources, job_type = 'allow_classic_ssh', walltime ='00:10:00')
			
			job = EX5.oarsub([(submission, site)])
			self.__class__._job = job
			
			# Sometimes oarsub fails silently
			if job[0][0] is None:
				print("\nError: no job was created")
				sys.exit(1)
				
			# Wait for the job to start
			self._log("Waiting for job {0} to start...\n".format(BOLD_MAGENTA + str(job[0][0]) + NORMAL))
			EX5.wait_oar_job_start(job[0][0], job[0][1], prediction_callback = prediction)
			nodes = EX5.get_oar_job_nodes(job[0][0], job[0][1])
			
			# Deploying nodes
			#deployment = EX5.Deployment(hosts = nodes, env_file='path_to_env_file')
			#run_deploy = EX5.deploy(deployment)
			#nodes_deployed = run_deploy.hosts[0]
			
			# Copying active_data program on all deployed hosts
			EX.Put([nodes[0]], '../dist/active-data-lib-0.1.2.jar', connexion_params = {'user': 'ansimonet'}).run()
			EX.Put([nodes[0]], '../server.policy', connexion_params = {'user': 'ansimonet'}).run()
			
			# Loop on the number of requests per client process
			while True:
				# Split the nodes
				clients = nodes[1:]
				server = nodes[0] 
				
				self._log("Running experiment with {0} nodes and {1} transitions per client".format(len(clients), comb['n_transitions']))
				
				# Launching Server on one node
				out_handler = FileOutputHandler(server_out_path)
				launch_server = EX.Remote('java -jar active-data-lib-0.1.2.jar', [server], stdout_handler = out_handler, stderr_handler = out_handler).start()
				self._log("Server started on " + server.address)
				time.sleep(2)
				
				# Launching clients
				rank=0
				n_cores = EX5.get_host_attributes(clients[0])['architecture']['smt_size'];
				cores = nodes * n_cores
				cores = cores[0:comb['n_clients']] # Cut out the additional cores
				
				client_connection_params = {
						'taktuk_gateway': 'lyon.grid5000.fr',
						'host_rewrite_func': None
				}
				
				self._log("Launching {0} clients...".format(len(cores)))
				
				client_cmd = "/usr/bin/env java -cp /home/ansimonet/active-data-lib-0.1.2.jar org.inria.activedata.examples.perf.TransitionsPerSecond " + \
								"{0} {1} {2} {3} {4}".format(server.address, 1200, "{{range(len(cores))}}", len(cores), comb['n_transitions'])
				client_out_handler = FileOutputHandler(os.path.join(self.result_dir, "clients.out"))
				client_request = EX.TaktukRemote(client_cmd, cores, connexion_params = client_connection_params, \
									stdout_handler = client_out_handler, stderr_handler = client_out_handler)
				
				client_request.run()
				
				if not client_request.ok():
					# Some client failed, please panic
					self._log("One or more client process failed. Enjoy reading their outputs.")
					self._log("OUTPUT STARTS -------------------------------------------------\n")
					for process in client_request.processes():
						print("----- {0} returned {1}".format(process.host().address, process.exit_code()))
						if not process.stdout() == "": print(GREEN + process.stdout() + NORMAL)
						if not process.stderr() == "": print(RED + process.stderr() + NORMAL)
						print("")
					self._log("OUTPUT ENDS ---------------------------------------------------\n")
					sweeper.skip(comb)
					launch_server.kill()
					launch_server.wait()
				else:
					# Waiting for server to end
					launch_server.wait()
				
					# Getting log files
					distant_path = OUT_FILE_FORMAT.format(len(cores), comb['n_transitions'])
					local_path = distant_path
					
					EX.Get([server], distant_path).run()
					
					EX.Local('mv ' + distant_path + ' ' + os.path.join(self.result_dir, local_path)).run()
					
					EX.Get([server], 'client_*.out', local_location = self.result_dir)
					EX.Remote('rm -f client_*.out', [server])
					
					self._log("Finishing experiment with {0} clients and {1} transitions per client".format(comb['n_clients'], comb['n_transitions']))
					
					sweeper.done(comb)
					
				sub_comb = sweeper.get_next (filtr = lambda r: filter(lambda s: s["n_clients"] == comb['n_clients'], r))
				self._updateStat(sweeper.stats())
				
				if not sub_comb: 
					# Killing job
					EX5.oar.oardel(job)
					self.__class__._job = None
					break
				else: 
					comb = sub_comb
		
		print ""

def prediction(timestamp):
	start = datetime.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
	active_data_tps._log("Waiting for job to start (prediction: {0})".format(start), False)

class FileOutputHandler(ProcessOutputHandler):
	__file = None
	
	def __init__(self, path):
		super(ProcessOutputHandler, self).__init__()
		self.__file = open(path, 'a')
	
	def __del__(self):
		self.__file.flush()
		self.__file.close()
	
	def read(self, process, string, eof=False, error=False):
		self.__file.write(string)
		self.__file.flush()
	
	def read_line(self, process, string, eof=False, error=False):
		self.__file.write(time.localtime().strftime("[%d-%m-%y %H:%M:%S"))
		self.__file.write(' ')
		self.__file.write(string)
		self.__file.flush()
