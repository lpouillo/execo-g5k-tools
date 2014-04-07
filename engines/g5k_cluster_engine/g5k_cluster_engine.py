from execo import Timer, sleep, format_date
from execo_g5k import oarsub, oardel, get_cluster_site, wait_oar_job_start, get_oar_job_nodes
from execo_engine import Engine, logger
from threading import Thread, Lock, current_thread
import functools

class _WorkerLogger(object):
    def __getattribute__(self, name):
        th = current_thread()
        prefix = "worker #%i %s@%s - job %s: %%s" % (
            th.worker_index,
            th.cluster,
            th.site,
            th.jobid)
        return functools.partial(logger.__getattribute__(name), prefix)

worker_log = _WorkerLogger()
"""execo_engine.logger proxy for logging from a worker thread.

To be called as a logger, for example worker_log.debug(...). Prefixes
the log message with information about the worker (worker number,
cluster, site, oar job id). Limitation: takes a single string as
argument, contrary to logger instances, whose log message can contain
a string and an arbitrary number of args / kwargs to be formatted in
the string.
"""

class g5k_cluster_engine(Engine):

    """execo engine automatizing the workflow of submitting jobs in parallel to Grid5000 clusters.

    Well suited for bag-of-task kind of jobs, where the cluster is one
    of the experiment parameter, e.g. benching flops, benching
    storage, network, etc.

    This engine's workflow is to continuously poll the involved
    clusters and check how much oar jobs are running, how much oar
    jobs are waiting. As soon as number of jobs waiting is below a
    threshold (and if the number of running jobs is not over a
    threshold), it submits a job, then runs a worker thread which
    waits for the job to start, gets the nodes and passes control to
    client code. So, from the client point of view, it is only needed
    to implement:
    `g5k_cluster_engine.g5k_cluster_engine.get_clusters`,
    `g5k_cluster_engine.g5k_cluster_engine.get_job`, which are called
    for the scheduling, and implement the client main "business" code
    inside `g5k_cluster_engine.g5k_cluster_engine.worker`
    """

    def __init__(self):
        super(g5k_cluster_engine, self).__init__()
        self.options_parser.add_option(
            "-r", dest = "max_workers", type = "int", default = 20,
            help = "maximum number of concurrent worker jobs per cluster")
        self.options_parser.add_option(
            "-t", dest = "max_waiting", type = "int", default = 2,
            help = "maximum number of concurrent waiting jobs per cluster")
        self.options_parser.add_option(
            "-s", dest = "schedule_delay", type = "int", default = 10,
            help = "delay between rescheduling worker jobs")

    def run(self):
        num_total_workers = 0
        sites_clusters_threads = {} # dict: keys = sites, values =
                                    # dict: keys = clusters, values =
                                    # list: threads
        try:
            while True:
                t = Timer()
                clusters_to_submit = set()
                for clusterspec in self.get_clusters():
                    cluster, _, site = clusterspec.partition(".")
                    if site == "":
                        site = get_cluster_site(cluster)
                    clusters_to_submit.add((cluster, site))
                for site in sites_clusters_threads.keys():
                    for cluster in sites_clusters_threads[site].keys():
                        sites_clusters_threads[site][cluster] = [
                            th
                            for th in sites_clusters_threads[site][cluster]
                            if th.is_alive() ]
                        if len(sites_clusters_threads[site][cluster]) == 0:
                            del sites_clusters_threads[site][cluster]
                    if len(sites_clusters_threads[site]) == 0:
                        del sites_clusters_threads[site]
                all_involved_sites = set(sites_clusters_threads.keys())
                all_involved_sites.update([ s for (c, s) in clusters_to_submit ])
                no_submissions = True
                for site in all_involved_sites:
                    all_involved_clusters = set()
                    if sites_clusters_threads.has_key(site):
                        all_involved_clusters.update(sites_clusters_threads[site].keys())
                    all_involved_clusters.update([ c for (c, s) in clusters_to_submit if s == site ])
                    for cluster in all_involved_clusters:
                        num_workers = 0
                        num_waiting = 0
                        if sites_clusters_threads.has_key(site) and sites_clusters_threads[site].has_key(cluster):
                            num_workers = len(sites_clusters_threads[site][cluster])
                            num_waiting = len([
                                    th
                                    for th in sites_clusters_threads[site][cluster]
                                    if th.waiting ])
                        num_max_new_workers = min(self.options.max_workers - num_workers,
                                                  self.options.max_waiting - num_waiting)
                        logger.trace(
                            "rescheduling on cluster %s@%s: num_workers = %s / num_waiting = %s / num_max_new_workers = %s" %
                            (cluster, site, num_workers, num_waiting, num_max_new_workers))
                        if num_max_new_workers > 0:
                            for worker_index in range(0, num_max_new_workers):
                                jobdata = self.get_job(cluster)
                                if not jobdata:
                                    break
                                no_submissions = False
                                logger.detail(
                                    "spawning worker %i on %s@%s" % (
                                        num_total_workers,
                                        cluster, site))
                                (oarsubmission, data) = jobdata
                                th = Thread(target = self.worker_start,
                                            args = (cluster, site,
                                                    oarsubmission, data,
                                                    num_total_workers,))
                                th.waiting = True
                                th.daemon = True
                                th.oarsublock = Lock()
                                th.willterminate = False
                                th.start()
                                num_total_workers += 1
                                if not sites_clusters_threads.has_key(site):
                                    sites_clusters_threads[site] = {}
                                if not sites_clusters_threads[site].has_key(cluster):
                                    sites_clusters_threads[site][cluster] = []
                                sites_clusters_threads[site][cluster].append(th)
                if no_submissions and len(sites_clusters_threads) == 0:
                    break
                sleep(self.options.schedule_delay)
            logger.detail("no more combinations to explore. exit schedule loop")
        finally:
            for site in sites_clusters_threads.keys():
                for cluster in sites_clusters_threads[site].keys():
                    for th in sites_clusters_threads[site][cluster]:
                        with th.oarsublock:
                            th.willterminate = True
                            if th.jobid:
                                logger.detail("cleaning: delete job %i of worker #%i on %s" % (
                                        th.jobid, th.worker_index, site))
                                oardel([(th.jobid, site)])
                                th.jobid = None

    def worker_start(self, cluster, site, oarsubmission, data, worker_index):
        th = current_thread()
        th.cluster = cluster
        th.site = site
        th.worker_index = worker_index
        th.jobid = None
        try:
            with th.oarsublock:
                if th.willterminate:
                    return
                worker_log.detail("submit oar job")
                ((th.jobid, _),) = oarsub([(oarsubmission, site)])
            if not th.jobid:
                worker_log.detail("job submission failed")
                self.worker(cluster, site, data, None, worker_index, oarsubmission, None)
            worker_log.detail("job submitted - wait job start")
            wait_oar_job_start(th.jobid, site,
                               prediction_callback = lambda ts:
                                   worker_log.detail("job start prediction: %s" % (format_date(ts),)))
            th.waiting = False
            worker_log.detail("job started - get job nodes")
            nodes = get_oar_job_nodes(th.jobid, site)
            worker_log.detail("got %i nodes" % (len(nodes),))
            self.worker(cluster, site, data, nodes, worker_index, oarsubmission, th.jobid)
        finally:
            with th.oarsublock:
                if th.jobid:
                    worker_log.detail("delete oar job")
                    oardel([(th.jobid, site)])
                    th.jobid = None
            worker_log.detail("exit")

    def get_clusters(self):
        """Returns an iterable of cluster names where it is planned to schedule jobs.

        This may be only an estimation, as getting actual jobs is done
        in get_jobs: thus it is possible to return more clusters than
        needed. But not less.

        This func must support being called continuously (it is called
        each scheduling iteration, ie. each schedule_delay).

        Cluster names can be returned in the form "cluster.site". In
        this cases, g5k_cluster_engine does not use the g5k api to
        resolve the cluster's site, it uses the provided site. This
        can be usefull to work with clusters not handled by the
        api. In these situations, `g5k_cluster_engine.get_job` and
        `g5k_cluster_engine.worker` will still be passed the naked
        cluster name, without ".site".

        to be overriden in client code inheriting from this class"""
        return []

    def get_job(self, cluster):
        """Returns a tuple (``execo_g5k.oar.OarSubmission``, data) for a job which will be immediately submitted.

        :param cluster: name of cluster for which a job is asked.

        The returned oarsubmission *must* embed the cluster selection,
        this is *not* done automatically by this engine.

        The data is handled as opaque data, which is passed to the
        worker code.

        Return None if no more submissions for this cluster.

        This func must support being called continuously (it may be
        called several times at each scheduling iteration, ie. each
        schedule_delay).

        to be overriden in client code inheriting from this class"""
        return None

    def worker(self, cluster, site, data, nodes, worker_index, oarsubmission, jobid):
        """Worker code which will be called for each job running.

        Also called if job submission / wait / nodes list retrieval
        failed for some reason, in order for client code to be
        notified of this special condition.

        :param cluster: name of cluster on which the job runs.

        :param site: name of site on which the job runs.

        :param data: opaque client data passed to
          ``g5k_cluster_engine.get_job``.

        :param nodes: list of nodes for this job. None if job
        submission / wait / nodes list retrieval failed.

        :param worker_index: an index incremented for each worker
          instanciated. This index is unique during one run of the
          engine.

        :param oarsubmission: the ``execo_g5k.oar.OarSubmission``
          which was used to submit this worker's job.

        :param jobid: this worker's oar job id. None if job submission
        failed
        """
        pass
