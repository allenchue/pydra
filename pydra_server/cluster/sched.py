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
import time
import logging

from twisted.internet import reactor

from pydra_server.cluster.tasks import tasks

logger = logging.getLogger('sched')

# TODO make this module thread-safe (with Twisted)


class TaskExecutionRecord:

    def __init__(self):
        self.task_id = None

        # properties that could be decisive in scheduling
        self.priority = 5
        self.scheduled = False
        self.workers = [] # workers allocated to this task (only keys)
        self.started_time = None # for how long this task has been run
        self.last_succ_time = None # when this task last time gets a worker
        self.worker_requests = [] # (args, subtask_key, args, workunit_key)

        # other properties
        self.primary_worker = None


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


class Scheduler:
    """
    The core scheduler class.

    All workers requests must be initiated on behalf of a root task, which is
    started from the master.
    """

    def __init__(self, listener=None):
        self._long_term_queue = []
        self._short_term_queue = []
        self._task_records = {} # scheduling decision reference
        self._idle_workers = [] # all workers are seen equal

        # how workers are allocated to tasks.
        # the key is the worker key, and the value is a tuple of
        # (root_task_id, root_task_key, args, subtask_key, workunit_key).
        self._worker_mappings = {} 

        self._listeners = if listener [listener] else []

        self.update_interval = 5 # seconds

        reactor.callInThread(self._update_queue, self)


    def attach_listener(listener):
        if hasattr(listener, 'worker_scheduled') and hasattr(listener,
                'worker_released'):
            self._listeners.append(listener)
        else:
            logger.warn('Ignored to attach an invalid listener')           


    def add_task(self, task):
        """
        Adds a (root) task to the queue.

        If there is an idle worker, this task will move to the shor-term
        queue immediately. Otherwise, it is added to the long-term queue.
        In either situation, an execution record is created for this task.
        """
        record = TaskExecutionRecord()
        record.priority = task.priority
        record.started_time = time.time()
        self._task_records[task.id] = record

        if self._idle_workers:
            # put this task to the shor-term queue immediately
            record.scheduled = True
            heappush(self._short_term_queue, [record.compute_score(), task.id])
            self._notify(self._idle_workers.pop(), task.id)
        else:
            heappush(self._long_term_queue, [record.compute_score(), task.id])


    def remove_task(self, root_task_id):
        """
        Removes a task from the queue.
       
        This method MUST be called AFTER the master has successfully stopped
        the task. Essentially, it collects the workers possesed by a cancelled
        task and returns them to the idle pool.
        """
        record = self._task_records.get(root_task_id, None)
        if record:
            if record.scheduled:
                for worker_key in record.workers:
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
            del self._task_records[root_task_id]


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
            record = self._task_records.get(owner_task_id, None)
            if record:
                record.workers.remove(worker_key)

        running_task = self._worker_mapping.get(worker_key, None):
        if running_task:
            logger.info('Task %d gives back a worker: %s' % (running_task[0],
                        worker_key))
            del self._worker_mapping[worker_key]

        if self._long_term_queue:
            # satisfy long-term tasks first to guarantee their completion
            task_id = heappop(self._long_term_queue)[1]
            record = self._task_records[task_id]
            # move it to the short-term queue
        elif self._short_term_queue:
            task_id = self._short_term_queue[1]
            record = self._task_records[task_id]
            if record.worker_requests:
                request = record.worker_requests.pop()
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
        record = self._task_records.get(root_task_id, None)
        if record:
            if self._idle_workers:
                self._allocate_worker(record.task_id, record.task_key,
                        args, subtask_key, workunit_key)
            else:
                record.worker_requests.append( (args, subtask_key, workunit_key) )
        else:
            # a worker request from an unknown task
            return None


    # TODO this should be a non-blocking call
    def _allocate_worker(self, root_task_id, root_task_key, args,
            subtask_key=None, workunit_key=None):
        record = self._task_records[root_task_id]
        record.last_succ_time = time.time()
        record.running_workers += 1

        worker_key = self._idle_workers.pop()
        self._worker_mappings[worker_key] = (root_task_id, root_task_key, args,
                subtask_key, workunit_key)
 
        for l in self._listeners:
            l.worker_scheduled(worker_key, args, subtask_key, workunit_key)


    def _update_queue(self):
        for task in self._long_term_queue:
            record = self._task_records[task_id[1]]
            task[0] = record.compute_score()
        heapify(self._long_term_queue)

        for task in self._long_term_queue:
            record = self._task_records[task_id[1]]
            task[0] = record.compute_score()
        heapify(self._short_term_queue)
            
        time.sleep(self.update_interval)

