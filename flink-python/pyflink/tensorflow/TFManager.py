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

# Copyright 2017 Yahoo Inc.
# Licensed under the terms of the Apache 2.0 license.
# Please see LICENSE file in the project root for terms.

from __future__ import absolute_import
from __future__ import division
from __future__ import nested_scopes
from __future__ import print_function

from multiprocessing.managers import BaseManager
from multiprocessing import JoinableQueue


class TFManager(BaseManager):
    """Python multiprocessing.Manager for distributed, multi-process communication."""
    pass


# global to each Spark executor's python worker
mgr = None  # TFManager
qdict = {}  # dictionary of queues
kdict = {}  # dictionary of key-values


def _get(key):
    return kdict[key]


def _set(key, value):
    kdict[key] = value


def _get_queue(qname):
    try:
        return qdict[qname]
    except KeyError:
        return None


def start(authkey, queues, mode='local'):
    """Create a new multiprocess.Manager (or return existing one).

    Args:
      :authkey: string authorization key
      :queues: *INTERNAL_USE*
      :mode: 'local' indicates that the manager will only be accessible from the same host, otherwise remotely accessible.

    Returns:
      A TFManager instance, which is also cached in local memory of the Python worker process.
    """
    global mgr, qdict, kdict
    qdict.clear()
    kdict.clear()
    for q in queues:
        qdict[q] = JoinableQueue()

    TFManager.register('get_queue', callable=lambda qname: _get_queue(qname))
    TFManager.register('get', callable=lambda key: _get(key))
    TFManager.register('set', callable=lambda key, value: _set(key, value))
    if mode == 'remote':
        mgr = TFManager(address=('', 0), authkey=authkey)
    else:
        mgr = TFManager(authkey=authkey)
    mgr.start()
    return mgr


def connect(address, authkey):
    """Connect to a multiprocess.Manager.

    Args:
      :address: unique address to the TFManager, either a unique connection string for 'local', or a (host, port) tuple for remote.
      :authkey: string authorization key

    Returns:
      A TFManager instance referencing the remote TFManager at the supplied address.
    """
    TFManager.register('get_queue')
    TFManager.register('get')
    TFManager.register('set')
    m = TFManager(address, authkey=authkey)
    m.connect()
    return m
