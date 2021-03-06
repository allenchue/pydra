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

import logging
import logging.handlers
from logging import FileHandler
from threading import Lock

import settings
from pydra_server.util import init_dir

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"

INITED_FILES = []
INIT_LOCK = Lock()

def init_logging(filename):
    """
    Utility function that configures the root logger so classes that require
    logging do not have to implement all this code.  After executing this
    function the calling function/class the logging class can be used with
    the root debugger.  ie. logging.info('example')

    This function records initialized loggers so that they are not initalized
    more than once, which would result in duplicate log messages
    """

    global INITED_FILES
    global INIT_LOCK

    with INIT_LOCK:

        logger = logging.getLogger('root')

        # only init a log once.
        if filename in INITED_FILES:
            return logger

        # set up logger  
        logger.setLevel(settings.LOG_LEVEL)

        handler = logging.handlers.RotatingFileHandler(
                 filename, 
                 maxBytes    = settings.LOG_SIZE, 
                 backupCount = settings.LOG_BACKUP)
        handler.setLevel(settings.LOG_LEVEL)

        formatter = logging.Formatter(LOG_FORMAT)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        INITED_FILES.append(filename)

    return logger


def get_task_logger(worker_id, task_id, subtask_key=None, \
    workunit_id=None):
    """
    Initializes a logger for tasks and subtasks.  Logs for tasks are stored as
    in separate files and aggregated.  This allow workunits to be viewed in a
    single log.  Otherwise a combined log could contain messages from many 
    different workunits making it much harder to grok.

    @param worker_id: there may be more than one Worker per Node.  Logs are
                      stored per worker.

    @param task_instance_id: ID of the instance.  Each task instance receives 
                             its own log.

    @param subtask_key: (optional) subtask_key.  see workunit_id

    @param workunit_id: (optional) ID of workunit.  workunits receive their
                         own log file so that the log can be read separately.
                         This is separate from the task instance log.
    """

    log_dir = settings.LOG_DIR[:-1] if settings.LOG_DIR.endswith('/') else \
        settings.LOG_DIR
    task_dir = '%s/worker.%s/task.%s' % (log_dir, worker_id, task_id)
    init_dir(task_dir)

    if workunit_id:
        logger_name = 'workunit.%s.%s' % (task_id, workunit_id)
        filename = '%s/workunit.%s.%s.log' % \
            (task_dir, subtask_key, workunit_id)
    else:
        logger_name = 'task.%s' % task_id
        filename = '%s/task.log' % task_dir

    logger = logging.getLogger(logger_name)
    handler = FileHandler(filename)
    formatter = logging.Formatter(LOG_FORMAT)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(settings.LOG_LEVEL)

    return logger
