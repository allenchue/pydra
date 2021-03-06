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
from __future__ import with_statement
from threading import Lock

from twisted.cred import credentials
from twisted.internet import reactor
from twisted.spread import pb

from pydra_server.cluster.auth.rsa_auth import RSAClient, load_crypto
from pydra_server.cluster.module import Module, ModuleReferenceable

# init logging
import logging
logger = logging.getLogger('root')


class MasterClientFactory(pb.PBClientFactory):
    """
    Subclassing of PBClientFactory to add automatic reconnection
    """
    def __init__(self, reconnect_func, *args, **kwargs):
        pb.PBClientFactory.__init__(self)
        self.reconnect_func = reconnect_func
        self.args = args
        self.kwargs = kwargs

    def clientConnectionLost(self, connector, reason):
        logger.warning('Lost connection to master.  Reason: %s' % reason)
        pb.PBClientFactory.clientConnectionLost(self, connector, reason)
        self.reconnect_func(*(self.args), **(self.kwargs))

    def clientConnectionFailed(self, connector, reason):
        logger.warning('Connection to master failed. Reason: %s' % reason)
        pb.PBClientFactory.clientConnectionFailed(self, connector, reason)


class WorkerConnectionManager(Module):
    """
    Module that manages connection with the Master for a Worker
    """

    _signals = [
        'MASTER_CONNECTED',
        'MASTER_DISCONNECTED'
    ]

    _shared = [
        'worker_key',
        'master',
        'master_host',
        'master_port',
        '_lock_connection'
    ]

    def __init__(self, manager):

        self._listeners = {
            'MANAGER_INIT':self.connect
        }

        Module.__init__(self, manager)

        self._lock_connection = Lock()
        self.reconnect_count = 0

        # load crypto for authentication
        # workers use the same keys as their parent Node
        self.pub_key, self.priv_key = load_crypto('./node.key')
        self.master_pub_key = load_crypto('./node.master.key', False, both=False)
        self.rsa_client = RSAClient(self.priv_key)


    def connect(self):
        """
        Make initial connections to all Nodes
        """
        import fileinput

        logger.info('worker:%s - connecting to master @ %s:%s' % (self.worker_key, self.master_host, self.master_port))
        factory = MasterClientFactory(self.reconnect)
        reactor.connectTCP(self.master_host, self.master_port, factory)

        # construct referenceable with remotes for MASTER
        client =  ModuleReferenceable(self.manager._remotes['MASTER'])

        deferred = factory.login(credentials.UsernamePassword(self.worker_key, '1234'), client=client)
        deferred.addCallbacks(self.connected, self.reconnect, errbackArgs=("Failed to Connect"))


    def reconnect(self, *arg, **kw):
        with self._lock_connection:
            self.master = None
        reconnect_delay = 5*pow(2, self.reconnect_count)
        #let increment grow exponentially to 5 minutes
        if self.reconnect_count < 6:
            self.reconnect_count += 1 
        logger.debug('worker:%s - reconnecting in %i seconds' % (self.worker_key, reconnect_delay))
        self.reconnect_call_ID = reactor.callLater(reconnect_delay, self.connect)


    def connected(self, result):
        """
        Callback called when connection to master is made
        """
        with self._lock_connection:
            self.master = result
        self.reconnect_count = 0

        logger.info('worker:%s - connected to master @ %s:%s' % (self.worker_key, self.master_host, self.master_port))

        # Authenticate with the master
        self.rsa_client.auth(result, None, self.master_pub_key)


    def connect_failed(self, result):
        """
        Callback called when conenction to master fails
        """
        self.reconnect()

