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

from twisted.application import internet
from twisted.cred import portal, checkers
from twisted.spread import pb
from zope.interface import implements

from pydra_server.cluster.auth.rsa_auth import load_crypto
from pydra_server.cluster.auth.master_avatar import MasterAvatar
from pydra_server.cluster.module import Module

import logging
logger = logging.getLogger('root')


class ClusterRealm:
    implements(portal.IRealm)
    def requestAvatar(self, avatarID, mind, *interfaces):
        assert pb.IPerspective in interfaces
        avatar = MasterAvatar(avatarID, self.server)
        avatar.attached(mind)
        return pb.IPerspective, avatar, lambda a=avatar:a.detached(mind)


class MasterConnectionManager(Module):

    
    _shared = [
        'port',
        'host'
    ]

    def __init__(self, manager):

        self._services = [
            self.get_service
        ]

        Module.__init__(self, manager)

        self.port = 11890
        self.host='localhost'
        self.node_key = None

        #load crypto keys for authentication
        self.pub_key, self.priv_key = load_crypto('./node.key')
        self.master_pub_key = load_crypto('./node.master.key', create=False, both=False)



    def get_service(self, manager):
        """
        Creates a service object that can be used by twistd init code to start the server
        """
        logger.info('Node - starting server on port %s' % self.port)

        realm = ClusterRealm()
        realm.server = self

        # create security - Twisted does not support ssh in the pb so were doing our
        # own authentication until it is implmented, leaving in the memory
        # checker just so we dont have to rip out the authentication code
        from twisted.cred.checkers import InMemoryUsernamePasswordDatabaseDontUse
        checker =   InMemoryUsernamePasswordDatabaseDontUse()
        checker.addUser('master','1234')
        p = portal.Portal(realm, [checker])

        factory = pb.PBServerFactory(p)

        return internet.TCPServer(self.port, factory)
