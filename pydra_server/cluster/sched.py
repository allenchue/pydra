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

from pydra_server.cluster.tasks import tasks


class TaskRecord:

    def __init__(self):
        self.task_id = None

        # properties that could be decisive in scheduling
        self.status = tasks.STATUS_STOPPED
        self.priority = 5
        self.available_workers = 1 # is this really useful?
        self.running_workers = 0 # how many workers has the task already got?
        self.started_time = None # for how long this task has been run?
        self.last_succ_time = None # when this task last time gets a worker
        self.worker_requests = 0 # how many worker_requests are pending?

        # other properties
        self.primary_worker = None
        self.worker_allocated_callback = None


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
    """

    def __init__(self):
        self._task_queue = [] # the priority queue
        self._task_records = {} # scheduling decision reference
        self._idle_workers = [] # all workers are seen equal

    def add_task(self, task):
        """
        Adds a task to the queue.
        """
        record = TaskRecord()
        record.status = tasks.STATUS_STOPPED
        record.priority = task.priority
        record.available_workers = len(self._idle_workers)
        record.started_time = time.time()
        self._task_records[task.id] = record
        heappush(self._task_queue, (record.compute_score(), task.id))

    def remove_task(self, task):
        """
        Removes a task from the queue (inefficient; use with caution)
        """
        pass

    def add_worker(self, worker):
        """
        Gives back a worker to the idle pool. 
        """
        # see if we have a pending a request
        task_id = self.dequeue()
        if task_id:
            task_record = self._task_records[task_id]

    def request_worker(self, root_task_id, worker_allocated_callback):
        task_record = self._task_records.get(root_task_id, None)
        if task_record:
            if self._idle_workers:
                worker = self._idle_workers.pop()
                worker_allocated_callback(root_task_id, worker)
            else:
                # no available workers; increment the request count
                task_record.worker_requests += 1
        else:
            # worker request from an unknown task
            return None

