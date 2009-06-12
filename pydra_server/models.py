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

from django.db import models

import dbsettings
from dbsettings.loading import set_setting_value


""" ================================
Settings
================================ """
from _mysql_exceptions import ProgrammingError
try:
    class PydraSettings(dbsettings.Group):
        host        = dbsettings.StringValue('host', 'IP Address or hostname for this server.  This value will be used by all nodes in the cluster to connect', default='localhost')
        port        = dbsettings.IntegerValue('port','Port for this server', default=18800)
    pydraSettings = PydraSettings('Pydra')

except ProgrammingError:
    pass #table hasnt been created yet 




""" ================================
Models
================================ """

"""
 Represents a node in the cluster
"""
class Node(models.Model):
    host            = models.CharField(max_length=255)
    port            = models.IntegerField(default=11880)
    cores_available = models.IntegerField(null=True)
    cores           = models.IntegerField(null=True)

    # key given to node for use by its workers
    key             = models.CharField(max_length=50, null=True)

    # keys used by master to connect to the node
    # this keypair is generated by the Master, the private key
    # is passed to the Node the first time it sees it.
    pub_key         = models.TextField(null=True)

    cpu_speed       = models.IntegerField(null=True)
    memory          = models.IntegerField(null=True)
    seen            = models.IntegerField(default=False)

    # non-model fields
    ref             = None
    _info           = None
    pub_key_obj     = None

    def __str__(self):
        return '%s:%s' % (self.host, self.port)

    def status(self):
        ret = 1 if self.ref else 0
        return ret

    class Meta:
        permissions = (
            ("can_edit_nodes", "Can create and edit nodes"),
        )

    def load_pub_key(self):
        """
        Load public key object from raw data stored in the model
        """
        if self.pub_key_obj:
            return self.pub_key_obj

        elif not self.pub_key:
            return None

        else:
            from django.utils import simplejson
            from Crypto.PublicKey import RSA

            pub_raw = simplejson.loads(self.pub_key)
            pub = [long(x) for x in pub_raw]
            pub_key_obj = RSA.construct(pub)
            self.pub_key_obj = pub_key_obj

            return  pub_key_obj

"""
Custom manager overridden to supply pre-made queryset for queued and running tasks
"""
class TaskInstanceManager(models.Manager):
    def queued(self):
        return self.filter(completion_type=None, started=None)

    def running(self):
        return self.filter(completion_type=None).exclude(started=None)


"""
Represents and instance of a Task.  This is used to track when a Task was run
and whether it completed.
"""
class TaskInstance(models.Model):
    task_key        = models.CharField(max_length=255)
    subtask_key     = models.CharField(max_length=255, null=True)
    args            = models.TextField(null=True)
    queued_time     = models.DateTimeField(auto_now_add=True)
    started_time    = models.DateTimeField(null=True)
    completed       = models.DateTimeField(null=True)
    worker          = models.CharField(max_length=255, null=True)
    status          = models.IntegerField(null=True)

    ######################
    # non-model attributes
    ######################

    # scheduling-related
    priority        = 5
    workers         = [] # workers allocated to this task (only keys)
    last_succ_time  = None # when this task last time gets a worker
    worker_requests = [] # (args, subtask_key, args, workunit_key)

    # others
    primary_worker  = None

    objects = TaskInstanceManager()

    class Meta:
        permissions = (
            ("can_run", "Can run tasks on the cluster"),
            ("can_stop_all", "Can stop anyone's tasks")
        )

    def compute_score(self):
        """
        Computes a priority score for this task, which will be used by the
        scheduler.

        Empirical analysis may reveal a good calculation formula. But in
        general, the following guideline is useful:
        1) Stopped tasks should have higher scores. At least for the current
           design, a task can well proceed even with only one worker. So letting
           a stopped task run ASAP makes sense.
        2) A task with higher priority should obviously have a higher score.
        3) A task that has been out of worker supply for a long time should
           have a relatively higher score.
        """
        return self.priority 

