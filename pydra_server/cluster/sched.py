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

from heapq import heappush, heappop

from pydra_server.cluster.tasks import tasks

class TaskRecord:
    def __init__(self):
        self.status = tasks.STATUS_STOPPED
        self.available_workers = 1
        self.running_workers = 1

    def compute_priority(self):
        """
        Computes a priority score for this task.
        """
        return 0

class Scheduler:
    """
    The core scheduler class.

    The Scheduler keeps a table of task states. The status information
    includes:
    1) status. Could be running or pending (still queued)
    2) owned_workers. The number of workers each task owns
    3) running_workers. The number of workers each task
    """

    def __init__(self):
        self._task_queue = []
        self._task_status = {}

    def enqueue(self, task):
        heappush(self._task_queue, (task.priority, task.id))

    def dequeue(self):
        if self._task_queue:
            task = heappop(self._task_queue)

    def request_worker(self):
        pass

