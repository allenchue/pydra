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

from __future__ import with_statement

from heapq import heappush, heappop, heapify
from datetime import datetime
from threading import Lock
import logging

from django.utils import simplejson

from twisted.internet import reactor

from pydra_server.cluster.tasks.tasks import STATUS_STOPPED, \
         STATUS_RUNNING, STATUS_COMPLETE, STATUS_CANCELLED, STATUS_FAILED

logger = logging.getLogger('pydra_server.cluster.sched')

# TODO make this module work well with Twisted


class DummySchedulerListener:

    def worker_scheduled(self, worker_key):
        pass

    def worker_removed(self, worker_key):
        pass

    def worker_added(self, worker_key):
        pass

    def task_removed(self, root_task_id):
        pass


class WorkerJob:
    """
    Encapsulates a job that runs on a worker.
    """

    def __init__(self, root_task_id, task_key, args, subtask_key=None,
            workunit_key=None):
        self.root_task_id = root_task_id
        self.task_key = task_key
        self.args = args
        self.subtask_key = subtask_key
        self.workunit_key = workunit_key


class Scheduler:
    """
    The core scheduler class.

    It essentially maintains state of workers and keeps assigning work units to
    workers. Any behavior that could cause changes to the worker state or
    worker-workunit mappings should be reported to the scheduler through its
    public interface to make the scheduler internally consistent.
    """

    def __init__(self, listener=None):
        self._worker_lock = Lock()
        self._queue_lock = Lock()

        self._long_term_queue = []
        self._short_term_queue = []
        self._task_instances = {} # caching task instances
        self._idle_workers = [] # all workers are seen equal
        self._worker_mappings = {} # worker-job mappings

        # the value is a bool value indicating if this main worker is working
        # on a workunit
        self._main_worker_status = {}

        self.update_interval = 5 # seconds
        self._listeners = []

        if listener:
            self.attach_listener(listener)

        reactor.callLater(self.update_interval, self._update_queue)


    def attach_listener(self, listener):
        if hasattr(listener, 'worker_scheduled') and hasattr(listener,
                'worker_released'):
            self._listeners.append(listener)
        else:
            logger.warn('Ignored to attach an invalid listener')           


    def add_task(self, task_key, args={}, priority=5):
        """
        Adds a (root) task that is to be run.

        Under the hood, the scheduler creates a task instance for the task, puts
        it into the long-term queue, and then tries to advance the queue.
        """
        logger.info('Task:%s:%s - Queued:  %s' % (task_key, subtask_key, args))

        task_instance = TaskInstance()
        task_instance.task_key = task_key
        task_instance.args = simplejson.dumps(args)
        task_instance.priority = priority
        task_instance.subtask_key = None
        task_instance.queued_time = datetime.now()
        task_instance.status = STATUS_STOPPED
        task_instance.save()

        task_id = task_instance.id

        with self._queue_lock:
            heappush(self._long_term_queue, [task_instance.compute_score(), task_id])

        # cache this task
        self._task_instances[task_id] = task_instance

        self._schedule()

        return task_instance


    def cancel_task(self, root_task_id):
        """
        Cancels a task either in the ltq or in the stq.

        Returns True if the specified task is found in the queue and is
        successfully removed, and False otherwise.

        BE CAUTIOUS to use this method on a task in the stq because that task
        may hold unreleased workers. To safely cancel a task in the stq (i.e.,
        already running), one has to (via the master interface) stop all the
        workers which are working on the task. Scheduler.add_worker() will
        handle the rest. So the advice is to only use this method to cancel a
        running task in case that it does not release workers after being
        notified to stop.
        """
        try:
            with self._queue_lock:
                self._short_term_queue.remove(root_task_id)
                task_instance = self._task_instances[root_task_id]
                task_instance.status = STATUS_CANCELLED
                task_instance.complete_time = datetime.now()
                task_instance.save()
                return True
        except ValueError:
            return False


    def add_worker(self, worker_key, task_status=None):
        """
        Adds a worker to the idle pool.

        Two possible invocation situations: 1) a new worker joins; and 2) a
        worker previously working on a work unit is returned to the pool.
        The latter case can be further categorized into several sub-cases, e.g.,
        task failure, task cancellation, etc. These sub-cases are identified by
        the third parameter, which is the final status of the task running on
        that worker.
        """
        with self._worker_lock:
            if worker_key in self._workers_idle:
                logger.warn('Worker is already in the idle pool: %s' %
                        worker_key)
                return

        job = self.get_worker_job(worker_key)
        if job:
            task_instance = self._task_instances[job.root_task_id]
            main_status = self._main_worker_status.get(worker_key, None)
            if main_status is not None:
                # this is a main worker
                if main_status == True:
                    # this main worker was working on a workunit
                    self._main_worker_status[worker_key] = False
                else:
                    # reaching here means the whole task has been finished
                    with self._worker_lock:
                        del self._main_worker_status[worker_key]
                    status = STATUS_COMPLETE if task_status is None else task_status
                    task_instance.status = status
                    task_instance.completed_time = datetime.now()
                    task_instance.save()

                    with self._queue_lock:
                        if status == STATUS_CANCELLED or status == STATUS_COMPLETE:
                            # safe to remove the task
                            try:
                                self._short_term_queue.remove(task)
                                heapify(self._short_term_queue)
                                logger.info('Task %d: %s is removed from the\
                                        short-term queue' % (job.root_task_id,
                                            job.task_key))
                            except ValueError:
                                pass
                        else:
                            # TODO inspect potential bugs here!
                            # re-queue the worker request
                            task_instance.queue_worker_request(
                                    (task_instance.main_worker, job.args,
                                        job.subtask_key, job.workunit_key) )
            else:
                # not a main worker
                logger.info("Task %d returns a worker: %s" % (job.root_task_id,
                            worker_key))
                if job.subtask_key is not None:
                    # just double-check to make sure
                    with self._worker_lock:
                        task_instance.workers.remove(worker_key) 
                        self._idle_workers.append(worker_key)

        self._schedule()
            

    def remove_worker(self, worker_key):
        """
        Removes a worker from the idle pool.

        Returns True if this operation succeeds and False otherwise.
        """
        with self._worker_lock:
            if self.get_worker_job(worker_key) is None:
                try:
                    self._idle_workers.remove(worker_key)
                    logger.info('Worker:%s has been removed from the idle pool')
                    return True
                except ValueError:
                    pass 
            return False


    def request_worker(self, requester_key, args, subtask_key, workunit_key):
        """
        Requests a worker for a workunit on behalf of a (main) worker.

        Calling this method means that the worker with key 'requester_key' is a
        main worker.
        """
        job = self.get_worker_job(requester_key)
        if job:
            main_status = self._main_worker_status.get(requester_key, None)
            if main_status is None:
                # mark this worker as a main worker.
                # it will be considered by the scheduler as a special worker
                # resource to complete the task.
                self._main_worker_status[requester_key] = False

            task_instance = self._task_instances[job.root_task_id]
            task_instance.queue_worker_request( (requester_key, args,
                        subtask_key, workunit_key) )
        else:
            # a worker request from an unknown task
            pass


    def get_worker_job(self, worker_key):
        """
        Returns a WorkerJob object or None if the worker is idle.
        If the specified worker is currently a primary one, subtask_key and
        workunit_key should be None.
        """
        return self._worker_mappings.get(worker_key, None)


    def get_workers_on_task(self, root_task_id):
        """
        Returns a list of keys of those workers working on a specified task.
        """
        task_instance = self._task_instances.get(root_task_id, None)
        if task_instance is None:
            return []
        else:
            return [x for x in task_instance.workers] + \
                task_instance.main_worker


    def get_task_instance(self, task_id):
        task_instance = self._task_instances.get(task_id, None)
        return TaskInstance.objects.get(id=task_id) if task_instance \
                                           is None else task_instance


    def _schedule(self):
        """
        Allocates a worker to a task/subtask.

        Note that a main worker is a special worker resource for executing
        parallel tasks. At the extreme case, a single main worker can finish
        the whole task even without other workers, albeit probably in a slow
        way.
        """
        worker_key, root_task_id, task_key, args, subtask_key, workunit_key = \
                        None, None, None, None, None, None
        with self._queue_lock:
            # satisfy tasks in the ltq first
            if self._long_term_queue:
                with self._worker_lock:
                    if self._idle_workers:
                        worker_key = self._idle_workers.pop()
                        root_task_id = heappop(self._long_term_queue)[1]
                        task_instance = self._task_instances[root_task_id]
                        task_instance.last_succ_time = datetime.now()
                        task_key = task_instance.task_key
                        args = task_instance.args
                        subtask_key, workunit_key = None, None
                        if task_instance.status == STATUS_STOPPED:
                            # move the task from the ltq to the stq
                            heappush(self._short_term_queue,
                                    [task_instance.compute_score(), root_task_id])
                            task_instance.main_worker = worker_key
                            task_instance.status = STATUS_RUNNING
                            task_instance.started_time = datetime.now()
                        task_instance.save()
            elif self._short_term_queue:
                with self._worker_lock:
                    root_task_id = self._short_term_queue[0][1]
                    task_instance = self._task_instances[root_task_id]
                    worker_key = None
                    requester, args, subtask_key, workunit_key = \
                                                      task_instance.pop_worker_request()
                    if self._main_worker_status[requester]:
                        # the main worker can do a local execution
                        worker_key = requester
                        self._main_worker_status[requester] = True
                    elif self._idle_workers:
                        worker_key = self._idle_workers.pop()
                        task_instance.workers.append(worker_key)


        if worker_key:
            self._worker_mappings[worker_key] = WorkerJob(root_task_id,
                    task_key, args, subtask_key, workunit_key)
     
            # notify the observers
            for l in self._listeners:
                l.worker_scheduled(worker_key, root_task_id, task_key,
                        args, subtask_key, workunit_key)

            return worker_key, root_task_id
        else:
            return None


    def _update_queue(self):
        """
        Periodically updates the scores of entries in both the long-term and the
        short-term queue and subsequently re-orders them.
        """
        with self._queue_lock:
            for task in self._long_term_queue:
                task_instance = self._task_instances[task[1]]
                task[0] = task_instance.compute_score()
            heapify(self._long_term_queue)

            for task_id in self._long_term_queue:
                task_instance = self._task_instances[task[1]]
                task[0] = task_instance.compute_score()
            heapify(self._short_term_queue)

            reactor.callLater(self.update_interval, self._update_queue)

