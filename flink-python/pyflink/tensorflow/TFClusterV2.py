################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
#################################################################################

from __future__ import absolute_import
from __future__ import division
from __future__ import nested_scopes
from __future__ import print_function

import json
import logging
import multiprocessing
import os
import pkg_resources
import platform
import socket
import subprocess
import sys
import uuid
import time
import traceback

from . import TFManager
from . import TFNode
from . import gpu_info
from . import marker
from . import reservation
from . import util
import random
import logging
from pyflink.table import DataTypes
from pyflink.table.udf import UserDefinedScalarFunctionWrapper


from pyflink.tensorflow.TFFlinkNode import TFFlinkNode, TFNodeContext

logger = logging.getLogger(__name__)


def get_server_fn(num_executors):
    def server_fn(iter):

        f = open("/tmp/hequn", "a")
        import os
        pid = os.getpid()
        f.write(str("\nserver_fn start with pid: ") + str(pid))
        f.close()
        # start a server to listen for reservations and broadcast cluster_spec
        server = reservation.Server(num_executors)
        ip, port = server.start()
        return ip + ':' + str(port)
    return server_fn


def wrap_map_fn(
        fn,
        num_executors,
        num_ps,
        tf_args,
        master_node=None,
        eval_node=False,
        queues=[
            'input',
            'output',
            'error'],
        background=False):

    # compute size of TF cluster and validate against number of Spark executors
    num_master = 1 if master_node else 0
    num_eval = 1 if eval_node else 0
    num_workers = max(num_executors - num_ps - num_eval - num_master, 0)
    total_nodes = num_ps + num_master + num_eval + num_workers

    cluster_template = {}
    executors = list(range(num_executors))

    if num_ps > 0:
        cluster_template['ps'] = executors[:num_ps]
        del executors[:num_ps]
    if master_node:
        cluster_template[master_node] = executors[:1]
        del executors[:1]
    if eval_node:
        cluster_template['evaluator'] = executors[:1]
        del executors[:1]
    if num_workers > 0:
        cluster_template['worker'] = executors[:num_workers]

    logger.info("cluster_template: {}".format(cluster_template))

    defaultFS = "file://"
    # strip trailing "root" slash from "file:///" to be consistent w/ "hdfs://..."
    if defaultFS.startswith("file://") and len(defaultFS) > 7 and defaultFS.endswith("/"):
        defaultFS = defaultFS[:-1]

    # get current working dir of spark launch
    working_dir = os.getcwd()

    cluster_meta = {
        'id': random.getrandbits(64),
        'cluster_template': cluster_template,
        'num_executors': num_executors,
        'default_fs': defaultFS,
        'working_dir': working_dir
    }

    def mapfn(executor_id, server_addr):

        f = open("/tmp/hequn", "a")
        import os
        pid = os.getpid()
        f.write(str("\n_mapfn start with pid: ") + str(pid))
        f.close()

        # Note: consuming the input iterator helps Pyspark re-use this worker,
        # executor_id = iter

        # if executor_id == 1:
        #     print(1)

        # check that there are enough available GPUs (if using tensorflow-gpu) before committing reservation on this node
        # def _get_gpus(cluster_spec=None):
        #     gpus = []
        #     is_k8s = 'SPARK_EXECUTOR_POD_IP' in os.environ
        #
        #     # handle explicitly configured tf_args.num_gpus
        #     if 'num_gpus' in tf_args:
        #         requested_gpus = tf_args.num_gpus
        #         user_requested = True
        #     else:
        #         requested_gpus = 0
        #         user_requested = False
        #
        #     # first, try Spark 3 resources API, returning all visible GPUs
        #     # note: num_gpus arg is only used (if supplied) to limit/truncate visible devices
        #     if version.parse(pyspark.__version__).base_version >= version.parse("3.0.0").base_version:
        #         from pyspark import TaskContext
        #         context = TaskContext()
        #         if 'gpu' in context.resources():
        #             # get all GPUs assigned by resource manager
        #             gpus = context.resources()['gpu'].addresses
        #             logger.info("Spark gpu resources: {}".format(gpus))
        #             if user_requested:
        #                 if requested_gpus < len(gpus):
        #                     # override/truncate list, if explicitly configured
        #                     logger.warn("Requested {} GPU(s), but {} available".format(requested_gpus, len(gpus)))
        #                     gpus = gpus[:requested_gpus]
        #             else:
        #                 # implicitly requested by Spark 3
        #                 requested_gpus = len(gpus)
        #
        #     # if not in K8s pod and GPUs available, just use original allocation code (defaulting to 1 GPU if available)
        #     # Note: for K8s, there is a bug with the Nvidia device_plugin which can show GPUs for non-GPU pods that are hosted on GPU nodes
        #     if not is_k8s and gpu_info.is_gpu_available() and not gpus:
        #         # default to one GPU if not specified explicitly
        #         requested_gpus = max(1, requested_gpus) if not user_requested else requested_gpus
        #         if requested_gpus > 0:
        #             if cluster_spec:
        #                 # compute my index relative to other nodes on the same host (for GPU allocation)
        #                 my_addr = cluster_spec[job_name][task_index]
        #                 my_host = my_addr.split(':')[0]
        #                 flattened = [v for sublist in cluster_spec.values() for v in sublist]
        #                 local_peers = [p for p in flattened if p.startswith(my_host)]
        #                 my_index = local_peers.index(my_addr)
        #             else:
        #                 my_index = 0
        #
        #             # try to allocate a GPU
        #             gpus = gpu_info.get_gpus(requested_gpus, my_index, format=gpu_info.AS_LIST)
        #
        #     if user_requested and len(gpus) < requested_gpus:
        #         raise Exception("Unable to allocate {} GPU(s) from available GPUs: {}".format(requested_gpus, gpus))
        #
        #     gpus_to_use = ','.join(gpus)
        #     if gpus:
        #         logger.info("Requested {} GPU(s), setting CUDA_VISIBLE_DEVICES={}".format(requested_gpus if user_requested else len(gpus), gpus_to_use))
        #     os.environ['CUDA_VISIBLE_DEVICES'] = gpus_to_use

        # try GPU allocation at executor startup so we can try to fail out if unsuccessful
        # _get_gpus()

        # assign TF job/task based on provided cluster_spec template (or use default/null values)
        job_name = 'default'
        task_index = -1
        cluster_id = cluster_meta['id']
        cluster_template = cluster_meta['cluster_template']

        server_ip, server_port = server_addr.split(":")
        cluster_meta['server_addr'] = (server_ip, int(server_port))

        for jobtype in cluster_template:
            nodes = cluster_template[jobtype]
            if executor_id in nodes:
                job_name = jobtype
                task_index = nodes.index(executor_id)
                break

        # get unique key (hostname, executor_id) for this executor
        host = util.get_ip_address()
        util.write_executor_id(executor_id)
        port = 0

        # check for existing TFManagers
        if TFFlinkNode.mgr is not None and str(TFFlinkNode.mgr.get('state')) != "'stopped'":
            if TFFlinkNode.cluster_id == cluster_id:
                # raise an exception to force Spark to retry this "reservation" task on
                # another executor
                raise Exception(
                    "TFManager already started on {0}, executor={1}, state={2}".format(
                        host, executor_id, str(
                            TFFlinkNode.mgr.get("state"))))
            else:
                # old state, just continue with creating new manager
                logger.warn(
                    "Ignoring old TFManager with cluster_id {0}, requested cluster_id {1}".format(
                        TFFlinkNode.cluster_id, cluster_id))

        # start a TFManager and get a free port
        # use a random uuid as the authkey
        authkey = uuid.uuid4().bytes
        addr = None
        if job_name in ('ps', 'evaluator'):
            # PS nodes must be remotely accessible in order to shutdown from Spark driver.
            TFFlinkNode.mgr = TFManager.start(authkey, ['control', 'error'], 'remote')
            addr = (host, TFFlinkNode.mgr.address[1])
        else:
            # worker nodes only need to be locally accessible within the executor for data feeding
            TFFlinkNode.mgr = TFManager.start(authkey, queues)
            addr = TFFlinkNode.mgr.address

        # initialize mgr state
        TFFlinkNode.mgr.set('state', 'running')
        TFFlinkNode.cluster_id = cluster_id

        # check server to see if this task is being retried (i.e. already reserved)
        f = open("/tmp/hequn", "a")
        import os
        f.write(str("\nserver_adder: ") + server_ip + ":" + str(server_port))
        f.close()

        client = reservation.Client(cluster_meta['server_addr'])
        cluster_info = client.get_reservations()
        tmp_sock = None
        node_meta = None
        for node in cluster_info:
            (nhost, nexec) = (node['host'], node['executor_id'])
            if nhost == host and nexec == executor_id:
                node_meta = node
                port = node['port']

        # if not already done, register everything we need to set up the cluster
        if node_meta is None:
            if 'TENSORFLOW_PORT' in os.environ:
                # use port defined in env var
                port = int(os.environ['TENSORFLOW_PORT'])
            else:
                # otherwise, find a free port
                tmp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                tmp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                tmp_sock.bind(('', port))
                port = tmp_sock.getsockname()[1]

            node_meta = {
                'executor_id': executor_id,
                'host': host,
                'job_name': job_name,
                'task_index': task_index,
                'port': port,
                'tb_pid': 0,
                'tb_port': 0,
                'addr': addr,
                'authkey': authkey
            }
            # register node metadata with server
            logger.info("TFSparkNode.reserve: {0}".format(node_meta))
            client.register(node_meta)
            # wait for other nodes to finish reservations
            cluster_info = client.await_reservations()
            client.close()

        # construct a TensorFlow clusterspec from cluster_info
        sorted_cluster_info = sorted(cluster_info, key=lambda k: k['executor_id'])
        cluster_spec = {}
        last_executor_id = -1
        for node in sorted_cluster_info:
            if (node['executor_id'] == last_executor_id):
                raise Exception("Duplicate worker/task in cluster_info")
            last_executor_id = node['executor_id']
            logger.info("node: {0}".format(node))
            (njob, nhost, nport) = (node['job_name'], node['host'], node['port'])
            hosts = [] if njob not in cluster_spec else cluster_spec[njob]
            hosts.append("{0}:{1}".format(nhost, nport))
            cluster_spec[njob] = hosts

        # update TF_CONFIG if cluster spec has a 'master' node (i.e. tf.estimator)
        if 'master' in cluster_spec or 'chief' in cluster_spec:
            tf_config = json.dumps({
                'cluster': cluster_spec,
                'task': {'type': job_name, 'index': task_index},
                'environment': 'cloud'
            })
            logger.info("export TF_CONFIG: {}".format(tf_config))
            os.environ['TF_CONFIG'] = tf_config

        # reserve GPU(s) again, just before launching TF process (in case situation has changed)
        # and setup CUDA_VISIBLE_DEVICES accordingly
        # _get_gpus(cluster_spec=cluster_spec)

        # create a context object to hold metadata for TF
        ctx = TFNodeContext(
            executor_id,
            job_name,
            task_index,
            cluster_spec,
            cluster_meta['default_fs'],
            cluster_meta['working_dir'],
            TFFlinkNode.mgr)

        # release port reserved for TF as late as possible
        if tmp_sock is not None:
            tmp_sock.close()

        def wrapper_fn(args, context):
            """Wrapper function that sets the sys.argv of the executor."""
            if isinstance(args, list):
                sys.argv = args
            fn(args, context)

        def wrapper_fn_background(args, context):
            """Wrapper function that signals exceptions to foreground process."""
            errq = TFFlinkNode.mgr.get_queue('error')
            try:
                wrapper_fn(args, context)
            except Exception:
                errq.put(traceback.format_exc())

        if job_name in ('ps', 'evaluator') or background:
            # invoke the TensorFlow main function in a background thread
            logger.info(
                "Starting TensorFlow {0}:{1} as {2} on cluster node {3} on background process".format(
                    job_name, task_index, job_name, executor_id))

            p = multiprocessing.Process(target=wrapper_fn_background, args=(tf_args, ctx))
            if job_name in ('ps', 'evaluator'):
                p.daemon = True
            p.start()
            p.join()

            # for ps and evaluator nodes, wait indefinitely in foreground thread for a "control" event (None == "stop")
            # if job_name in ('ps', 'evaluator'):
            #     queue = TFFlinkNode.mgr.get_queue('control')
            #     equeue = TFFlinkNode.mgr.get_queue('error')
            #     done = False
            #     while not done:
            #         while (queue.empty() and equeue.empty()):
            #             time.sleep(1)
            #         if (not equeue.empty()):
            #             e_str = equeue.get()
            #             raise Exception("Exception in " + job_name + ":\n" + e_str)
            #         msg = queue.get(block=True)
            #         logger.info("Got msg: {0}".format(msg))
            #         if msg is None:
            #             logger.info("Terminating {}".format(job_name))
            #             TFFlinkNode.mgr.set('state', 'stopped')
            #             done = True
            #         queue.task_done()
        else:
            # otherwise, just run TF function in the main executor/worker thread
            logger.info(
                "Starting TensorFlow {0}:{1} on cluster node {2} on foreground thread".format(
                    job_name, task_index, executor_id))
            wrapper_fn(tf_args, ctx)
            logger.info(
                "Finished TensorFlow {0}:{1} on cluster node {2}".format(
                    job_name, task_index, executor_id))

        return executor_id

    return mapfn


def run(map_fun, num_executors, num_ps, tf_args, master_node=None, eval_node=False):
    from pyflink.java_gateway import get_gateway

    _map_fn = wrap_map_fn(
            map_fun,
            num_executors,
            num_ps,
            tf_args,
            master_node=master_node,
            eval_node=eval_node)
    w_map_fn = UserDefinedScalarFunctionWrapper(_map_fn, [DataTypes.INT(), DataTypes.STRING()], DataTypes.INT(), "general", None, None)

    _server_fn = get_server_fn(num_executors)
    w_server_fn = UserDefinedScalarFunctionWrapper(_server_fn, [DataTypes.INT()], DataTypes.STRING(), "general", None, None)

    get_gateway().jvm.TFCluster.run(
        w_map_fn.java_user_defined_function(),
        w_server_fn.java_user_defined_function())
