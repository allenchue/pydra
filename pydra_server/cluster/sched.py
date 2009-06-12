#!/usr/bin/env python

# Copyright 2009 Oregon State University
#
# This file is part of Pydra.
#
# Pydra is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Pydra is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Pydra.  If not, see <http://www.gnu.org/licenses/>.

from heapq import heappush, heappop, heapify
from datetime import datetime
import logging

from twisted.internet import reactor

from pydra_server.cluster.tasks.tasks import STATUS_STOPPED, \
         STATUS_RUNNING, STATUS_COMPLETE, STATUS_CANCELLED, STATUS_FAILED

logger = logging.getLogger('pydra_server.cluster.sched')

# TODO make this module work well with Twisted


class Scheduler:
    """
    The core scheduler class.

    All workers requests must be initiated on behalf of a root task, which is
    started from the master.
    """

    def __init__(self, listener=None):
        self._long_term_queue = []
        self._short_term_queue = []
        self._task_instances = {} # caching task instances
        self._idle_workers = [] # all workers are seen equal

        # the key is the worker key, and the value is a tuple of
        # (root_task_id, root_task_key, args, subtask_key, workunit_key).
        self._worker_mappings = {} 

        self.update_interval = 5 # seconds
        self._listeners = []

        if listener:
            attach_listener(listener)

        reactor.callLater(self.update_interval, self.__update_queue)


    def attach_listener(listener):
        if hasattr(listener, 'worker_scheduled') and hasattr(listener,
                'worker_released'):
            self._listeners.append(listener)
        else:
            logger.warn('Ignored to attach an invalid listener')           


    def add_task(self, task_key, args={}, priority=5):
        """
        Adds a (root) task that is to be run.

        If there is an idle worker, this task will move to the short-term
        queue immediately. Otherwise, it is added to the long-term queue.
        In either situation, an execution task_instance (TaskInstance) is created
        for this task.
        """
        logger.info('Task:%s:%s - Queued:  %s' % (task_key, subtask_key, args))

        task_instance = TaskInstance()
        task_instance.task_key = task_key
        task_instance.args = simplejson.dumps(args)
        task_instance.priority = priority
        task_instance.subtask_key = None
        task_instance.queued_time = datetime.now()

        if self._idle_workers:
            # put this task to the shor-term queue immediately
            task_instance.status = STATUS_RUNNING
            heappush(self._short_term_queue, [task_instance.compute_score(), task.id])
            self._allocate_worker(task.id)
        else:
            task_instance.status = STATUS_STOPPED
            heappush(self._long_term_queue, [task_instance.compute_score(), task.id])

        task_instance.save()
        self._task_instances[task.id] = task_instance

        return task_instance


    def remove_task(self, root_task_id, final_status):
        """
        Removes a task from the scheduling queue.
       
        Invocation can happen when
        1) a task fails
        2) a task is cancelled, etc.
        This method MUST be called AFTER the master has successfully stopped
        the task. Essentially, it collects the workers possesed by a cancelled
        task and returns them to the idle pool.
        """
        task_instance = self.get_task_instance(root_task_id)
        if task_instance:
            if task_instance.status == STATUS_RUNNING:
                for worker_key in task_instance.workers:
                    del self._worker_mappings[worker_key]
                    self.add_worker(worker_key) # no need to pass the owner task
                for task in self._short_term_queue:
                    if task[1] == root_task_id:
                        self._short_term_queue.remove(task)
                    break
                heapify(self._short_term_queue)
            else:
                for task in self._long_term_queue:
                    if task[1] == root_task_id:
                        self._short_long_queue.remove(task)
                    break
                heapify(self._long_term_queue)

            task_instance.status = final_status
            task_instance.save()
            del self._task_instances[root_task_id]


    def add_worker(self, worker_key, owner_task_id=None):
        """
        Adds a worker to the idle pool.

        Two possible calling situations: 1) a new worker joins; and 2) a worker
        previously working on a work unit is returned to the pool.
        """
        if worker_key in self._workers_idle:
            logger.warn('Ignored a worker that is already in the pool: %s' %
                    worker_key)
            
        if owner_task_id:
            task_instance = self._task_instances.get(owner_task_id, None)
            if task_instance:
                task_instance.workers.remove(worker_key)

        running_task = self._worker_mapping.get(worker_key, None):
        if running_task:
            logger.info('Task %d gives back a worker: %s' % (running_task[0],
                        worker_key))
            del self._worker_mapping[worker_key]

        if self._long_term_queue:
            # satisfy long-term tasks first to guarantee their completion
            task_id = heappop(self._long_term_queue)[0]
            task_instance = self._task_instances[task_id]
            # move it to the short-term queue
        elif self._short_term_queue:
            task_id = self._short_term_queue[0]
            task_instance = self._task_instances[task_id]
            if task_instance.worker_requests:
                request = task_instance.worker_requests.pop()
                # dispatch a work unit
            else:
                # can we reach here?
                pass
        else:
            self._idle_workers.append(worker)
            logger.info('worker:%s - added to idle workers' % worker_key)


    def remove_worker(self, worker_key):
        pass


    def request_worker(self, root_task_id, args, subtask_key, workunit_key):
        task_instance = self._task_instances.get(root_task_id, None)
        if task_instance:
            if self._idle_workers:
                self._allocate_worker(task_instance, subtask_key, workunit_key)
            else:
                task_instance.worker_requests.append( (args, subtask_key,
                            workunit_key) )
        else:
            # a worker request from an unknown task
            return None


    def get_worker_job(self, worker_key):
        return self._worker_mappings.get(worker_key, None)


    def get_task_instance(self, task_id):
        task_instance = self._task_instances.get(task_id, None)
        return task_instance if task_instance else \
            TaskInstance.objects.get(id=task_id)


    # TODO this should be a non-blocking call
    def _allocate_worker(self, root_task_id, root_task_key, args,
            subtask_key=None, workunit_key=None):
        """
        Allocates a worker to a task/subtask.
        """
        worker_key = self._idle_workers.pop()
        self._worker_mappings[worker_key] = (root_task_id, root_task_key, args,
                subtask_key, workunit_key)
 
        # notify the observers
        for l in self._listeners:
            l.worker_scheduled(worker_key, root_task_id,  args, subtask_key,
                    workunit_key)

        task_instance = self._task_instances[root_task_id]
        if not subtask_key:
            # a root task is started
            task_instance.status = STATUS_RUNNING
            task_instance.started_time = datetime.now()
        task_instance = self._task_instances[root_task_id]
        task_instance.last_succ_time = time.time()
        task_instance.running_workers += 1

        task_instance.save()


    def _update_queue(self):
        for task in self._long_term_queue:
            task_instance = self._task_instances[task_id[1]]
            task[0] = task_instance.compute_score()
        heapify(self._long_term_queue)

        for task in self._long_term_queue:
            task_instance = self._task_instances[task_id[1]]
            task[0] = task_instance.compute_score()
        heapify(self._short_term_queue)

        reactor.callLater(self._update_interval, self._update_queue)

