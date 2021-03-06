#!/usr/bin/env python

"""
    Copyright 2009 Oregon State University

    This file is part of Pydra.

    Pydra is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    Pydra is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with Pydra.  If not, see <http://www.gnu.org/licenses/>.
"""

# ==========================================================
# Setup django environment 
# ==========================================================

import sys
import os

#python magic to add the current directory to the pythonpath
sys.path.append(os.getcwd())

#
if not os.environ.has_key('DJANGO_SETTINGS_MODULE'):
    os.environ['DJANGO_SETTINGS_MODULE'] = 'settings'

# ==========================================================
# Done setting up django environment
# ==========================================================
import time

# should be executed before any other reactor stuff to prevent from using non
# glib2 event loop which we need for dbus
from twisted.internet import glib2reactor
glib2reactor.install()


from twisted.application import service

from pydra_server.cluster.amf.interface import AMFInterface
from pydra_server.cluster.module import ModuleManager
from pydra_server.cluster.master import *
from pydra_server.cluster.tasks.task_manager import TaskManager
from pydra_server.cluster.master.task_sync import TaskSyncServer
from pydra_server.models import pydraSettings

# init logging
import settings
from pydra_server.logging.logger import init_logging
logger = init_logging(settings.LOG_FILENAME_MASTER)


class Master(ModuleManager):
    """
    Master is the server that controls the cluster.  There must be one and only one master
    per cluster (for now).  It will direct and delegate work taking place on the Nodes and Workers
    """

    def __init__(self):
        logger.info('====== starting master ======')

        """
        List of modules to load.  They will be loaded sequentially
        """
        self.modules = [
            AutoDiscoveryModule,
            NodeConnectionManager,
            WorkerConnectionManager,
            TaskManager,
            TaskSyncServer,
            TaskScheduler,
            AMFInterface,
            NodeManager
        ]

        ModuleManager.__init__(self)

        self.emit_signal('MANAGER_INIT')


#setup application used by twistd
master = Master()

application = service.Application("Pydra Master")

for service in master.get_services():
    logger.info('Starting service: %s' % service)
    service.setServiceParent(application)

