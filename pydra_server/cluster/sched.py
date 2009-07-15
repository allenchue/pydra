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
from pydra_server.models import TaskInstance

logger = logging.getLogger('pydra_server.cluster.sched')

# TODO make this module work well with Twisted


class WorkerJob:
    """
    Encapsulates a job that runs on a worker.
    """

    def __init__(self, root_task_id, task_key, args, subtask_key=None,
            workunit_key=None, on_main_worker=False):
        self.root_task_id = root_task_id
        self.task_key = task_key
        self.args = args
        self.subtask_key = subtask_key
        self.workunit_key = workunit_key
        self.on_main_worker = on_main_worker


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
        self._active_tasks = {} # caching uncompleted task instances
        self._idle_workers = [] # all workers are seen equal
        self._worker_mappings = {} # worker-job mappings
        self._waiting_workers = {} # task-worker mappings

        # the value is a bool value indicating if this main worker is working
        # on a workunit
        self._main_worker_status = {}

        self.update_interval = 5 # seconds
        self._listeners = []

        if listener:
            self.attach_listener(listener)

        self._init_queue()

        reactor.callLater(self.update_interval, self._update_queue)


    def attach_listener(self, listener):
        if hasattr(listener, 'worker_scheduled'):
            self._listeners.append(listener)
        else:
            logger.warn('Ignored to attach an invalid listener')           


    def add_task(self, task_key, args={}, priority=5):
        """
        Adds a (root) task that is to be run.

        Under the hood, the scheduler creates a task instance for the task, puts
        it into the long-term queue, and then tries to advance the queue.
        """
        logger.info('Task:%s - Queued:  %s' % (task_key, args))

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
        self._active_tasks[task_id] = task_instance

        self._schedule()

        return task_instance


    def cancel_task(self, root_task_id):
        """
        Cancels a task either in the ltq or in the stq.

        Returns True if the specified task is found in the queue and is
        successfully removed, and False otherwise.

        This method does NOT release the workers held by the cancelled task. So
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
                found = False
                # find the task in the ltq
                length = len(self._long_term_queue)
                for i in range(0, length):
                    if self._long_term_queue[i][1] == root_task_id:
                        del self._long_term_queue[i]
                        found = True
                        break
                else:
                    length = len(self._long_term_queue)
                    for i in range(0, length):
                        if self._long_term_queue[i][1] == root_task_id:
                            del self._long_term_queue[i]
                            found = True
                            # XXX release the workers held by this task?
                            break

                if found:
                    task_instance = self._active_tasks.pop(root_task_id)
                    task_instance.status = STATUS_CANCELLED
                    task_instance.completed_time = datetime.now()
                    task_instance.save()
                return True
        except ValueError:
            return False


    def add_worker(self, worker_key, task_status=None):
        """
        Adds a worker to the **idle pool**.

        Two possible invocation situations: 1) a new worker joins; and 2) a
        worker previously working on a work unit is returned to the pool.
        The latter case can be further categorized into several sub-cases, e.g.,
        task failure, task cancellation, etc. These sub-cases are identified by
        the third parameter, which is the final status of the task running on
        that worker.
        """
        with self._worker_lock:
            if worker_key in self._idle_workers:
                logger.warn('Worker is already in the idle pool: %s' %
                        worker_key)
                return

        job = self.get_worker_job(worker_key)
        if job:
            task_instance = self._active_tasks[job.root_task_id]
            main_status = self._main_worker_status.get(worker_key, None)
            if main_status is not None:
                # this is a main worker
                if main_status == True:
                    # this main worker was working on a workunit
                    logger.info('Main worker:%s ready for work again' % worker_key)
                    self._main_worker_status[worker_key] = False
                else:
                    # reaching here means the whole task has been finished
                    logger.info('Last worker held by task %s is returned' \
                            % task_instance.task_key)
                    with self._worker_lock:
                        self._idle_workers.append(worker_key)
                        del self._main_worker_status[worker_key]
                        del self._worker_mappings[worker_key]
                    status = STATUS_COMPLETE if task_status is None else task_status
                    task_instance.status = status
                    task_instance.completed_time = datetime.now()
                    task_instance.save()

                    # TODO release all the waiting workers

                    with self._queue_lock:
                        if status == STATUS_CANCELLED or status == STATUS_COMPLETE:
                            # safe to remove the task
                            length = len(self._short_term_queue)
                            for i in range(0, length):
                                if self._short_term_queue[i][1] == job.root_task_id:
                                    del self._short_term_queue[i]
                                    logger.info(
                                            'Task %d: %s is removed from the short-term queue' % \
                                            (job.root_task_id, job.task_key))
                            heapify(self._short_term_queue)
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
                        del self._worker_mappings[worker_key]
                        task_instance.running_workers.remove(worker_key) 
                        self._idle_workers.append(worker_key)
        else:
            # a new worker
            logger.info('A new worker:%s is added' % worker_key)
            with self._worker_lock:
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
                    logger.info('Worker:%s has been removed from the idle pool'
                            % worker_key)
                    return True
                except ValueError:
                    pass 
            else:
                logger.warn('Removal of worker:%s failed: worker busy',
                        worker_key)
            return False


    def hold_worker(self, worker_key):
        with self._worker_lock:
            if not self._main_worker_status.get(worker_key, None):
                # we don't need to retain a main worker
                job = self._worker_mappings.get(worker_key, None)
                if job:
                    task_instance = self._active_tasks.get(job.root_task_id, None)
                    if task_instance and worker_key <> task_instance.main_worker:
                        task_instance.running_workers.remove(worker_key)
                        task_instance.waiting_workers.append(worker_key)
                        del self._worker_mappings[worker_key]


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

            task_instance = self._active_tasks[job.root_task_id]
            logger.info('queuing one worker request')
            task_instance.queue_worker_request( (requester_key, args,
                        subtask_key, workunit_key) )

            self._schedule()
        else:
            # a worker request from an unknown task
            pass


    def get_worker_job(self, worker_key):
        """
        Returns a WorkerJob object or None if the worker is idle.
        """
        return self._worker_mappings.get(worker_key, None)


    def get_workers_on_task(self, root_task_id):
        """
        Returns a list of keys of those workers working on a specified task.
        """
        task_instance = self._active_tasks.get(root_task_id, None)
        if task_instance is None:
            # finished task or non-existent task
            return []
        else:
            return [x for x in task_instance.running_workers] + \
                [x for x in task_instance.waiting_workers] + \
                task_instance.main_worker


    def get_task_instance(self, task_id):
        task_instance = self._active_tasks.get(task_id, None)
        return TaskInstance.objects.get(id=task_id) if task_instance \
                                           is None else task_instance


    def get_queued_tasks(self):
        return [self._active_tasks[x[1]] for x in self._long_term_queue]


    def get_running_tasks(self):
        return [self._active_tasks[x[1]] for x in self._short_term_queue]


    def get_worker_status(self, worker_key):
        """
        0: idle; 1: working; 2: waiting; -1: unknown
        """
        job = self.get_worker_job(worker_key)
        if job:
            return 1
        elif self._waiting_workers.get(worker_key, None):
            return 2
        elif worker_key in self._idle_workers:
            return 0
        return -1


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
        on_main_worker = False
        with self._queue_lock:
            # satisfy tasks in the ltq first
            if self._long_term_queue:
                with self._worker_lock:
                    if self._idle_workers:
                        # move the task from the ltq to the stq
                        worker_key = self._idle_workers.pop()
                        root_task_id = heappop(self._long_term_queue)[1]
                        task_instance = self._active_tasks[root_task_id]
                        task_instance.last_succ_time = datetime.now()
                        task_key = task_instance.task_key
                        args = simplejson.loads(task_instance.args)
                        subtask_key, workunit_key = None, None
                        heappush(self._short_term_queue,
                                [task_instance.compute_score(), root_task_id])
                        task_instance.main_worker = worker_key
                        task_instance.status = STATUS_RUNNING
                        task_instance.started_time = datetime.now()
                        task_instance.save()

                        self._main_worker_status[worker_key] = False
                        logger.info('Task %d has been moved from ltq to stq' %
                                root_task_id)
            elif self._short_term_queue:
                with self._worker_lock:
                    task_instance, worker_request = None, None
                    for task in self._short_term_queue:
                        root_task_id = task[1]
                        task_instance = self._active_tasks[root_task_id]
                        worker_request = task_instance.poll_worker_request()
                        if worker_request:
                            break
                        else:
                            # this task has no pending worker requests
                            # check if it has waiting workers; and if not,
                            # this task is considered completed
                            if not task_instance.waiting_workers:
                                logger.info('Task %d:%s is finished' %
                                        (root_task_id, task_instance.task_key))
                                # safe to remove the task
                                length = len(self._short_term_queue)
                                for i in range(0, length):
                                    if self._short_term_queue[i][1] == root_task_id:
                                        del self._short_term_queue[i]
                                        logger.info(
                                                'Task %d: %s is removed from the short-term queue' % \
                                                (root_task_id, task_instance.task_key))
                                heapify(self._short_term_queue)
                    else:
                        # no pending worker requests
                        return
                    requester, args, subtask_key, workunit_key = worker_request
                    task_key = task_instance.task_key
                    if task_instance.waiting_workers:
                        logger.info('Re-dispatching waiting worker:%s to' % 
                                worker_key)
                        worker_key = task_instance.waiting_workers.pop()
                        task_instance.running_workers.append(worker_key)
                    elif not self._main_worker_status[requester]:
                        # the main worker can do a local execution
                        task_instance.pop_worker_request()
                        logger.info('Main worker:%s assigned to task %s' %
                                (requester, task_instance.task_key))
                        worker_key = requester
                        self._main_worker_status[requester] = True
                        on_main_worker = True
                    elif self._idle_workers:
                        task_instance.pop_worker_request()
                        worker_key = self._idle_workers.pop()
                        task_instance.running_workers.append(worker_key)
                        logger.info('Worker:%s assigned to task %s' %
                                (requester, task_instance.task_key))

        if worker_key:
            self._worker_mappings[worker_key] = WorkerJob(root_task_id,
                    task_key, args, subtask_key, workunit_key, on_main_worker)
     
            # notify the observers
            for l in self._listeners:
                l.worker_scheduled(worker_key, root_task_id, task_key,
                        args, subtask_key, workunit_key)

            return worker_key, root_task_id
        else:
            return None


    def _init_queue(self):
        """
        Initialize the ltq and the stq by reading the persistent store.
        """
        with self._queue_lock:
            queued = TaskInstance.objects.queued()
            running = TaskInstance.objects.running()
            for t in queued:
                self._long_term_queue.append([t.compute_score(), t.id])
                self._active_tasks[t.id] = t
            for t in running:
                self._short_term_queue.append([t.compute_score(), t.id])
                self._active_tasks[t.id] = t


    def _update_queue(self):
        """
        Periodically updates the scores of entries in both the long-term and the
        short-term queue and subsequently re-orders them.
        """
        with self._queue_lock:
            for task in self._long_term_queue:
                task_instance = self._active_tasks[task[1]]
                task[0] = task_instance.compute_score()
            heapify(self._long_term_queue)

            for task_id in self._long_term_queue:
                task_instance = self._active_tasks[task[1]]
                task[0] = task_instance.compute_score()
            heapify(self._short_term_queue)

            reactor.callLater(self.update_interval, self._update_queue)

