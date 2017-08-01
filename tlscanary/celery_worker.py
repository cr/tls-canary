# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

from celery import Celery, current_app, signals, Task, bootsteps
from celery.bin import worker
import logging
import random
import resource
import string

import tlscanary.messaging as msg
import tlscanary.xpcshell_celery_worker as xw


logger = logging.getLogger(__name__)


class NewWorkerStep(bootsteps.StartStopStep):
    requires = {"celery.worker.components:Pool"}

    def __init__(self, woker, **kwargs):
        print "called new worker bootstep with {0!r}".format(kwargs)

    def create(self, worker):
        return self

    def start(self, worker):
        print "worker started"

    def stop(self, worker):
        print "worker stoppped"

    def terminate(self, worker):
        print "worker terminating"


class MessagingCelery(Celery):
    """Subclass of Celery app that uses PipingTask for creating tasks"""
    task_cls = "tlscanary.celery_worker.MessagingTask"


class MessagingTask(Task):
    """Subclass of Celery task that *inherits* a Pipe for XPCShellWorker IPC"""

    def __init__(self):
        # This must be run from the main process
        super(MessagingTask, self).__init__()
        self.__receiver_id, self.__queue = msg.create_receiver()

    def start_listening(self, event_id):
        msg.start_listening(self.__receiver_id, event_id)

    def events_pending(self):
        return not self.__queue.empty()

    def receive(self, block=True, timeout=None):
        return self.__queue.get(block=block, timeout=timeout)

    def stop_events(self):
        """Must be called to avoid dead listeners"""
        msg.remove_receiver(self.__receiver_id)

    @staticmethod
    def dispatch(event):
        msg.dispatch(event)


app = Celery()
xpw = None


@app.task(bind=True)
def run_command(self, mode, **kwargs):
    print self.request
    # self.start_listening(self.request.id)
    # kwargs["request_id"] = self.request.id
    # msg.cm
    # self.dispatch(msg.Event(mode, **kwargs))
    return "I think I got this: %s" % str(self.request)


def start(args, xpcsw, result_backend="rpc://", loglevel="DEBUG"):
    global aapp, xpw, logger
    xpw = xpcsw
    # Increase limit of open files to 500
    # Default 256 on OS X is not enough for a pool of 50 concurrent workers
    resource.setrlimit(resource.RLIMIT_NOFILE, (1000, -1))
    # app = Celery(backend="rpc://", broker="redis://localhost")
    logger.debug("Dumping celery app configuration")
    for config_key in sorted(app.conf.keys()):
        logger.debug("%s: %s" % (config_key, app.conf[config_key]))

    app.conf.update(
        broker_url=args.broker,
        enable_utc=True,
        result_backend=result_backend,
        task_send_set_event=True,
        tast_serializer="pickle",
        worker_concurrency=args.requestsperworker,
        worker_hijack_root_logger=True,
        worker_redirect_stdouts=True,
        worker_redirect_stdouts_level="WARNING",
        worker_send_task_events=True,
        worker_timer_precision=0.1,
    )
    # @signals.setup_logging.connect
    # def setup_celery_logging(**kwargs):
    #     print "something"

    app.log.setup(
        loglevel=loglevel
        # logfile=None,
        # redirect_stdouts=True,
        # redirect_level=logging.WARNING,
        # colorize=None,
        # hostname="foooo"
    )
    app.steps['worker'].add(NewWorkerStep)
    celery_worker = worker.worker(app=app)
    options = {
        "loglevel": loglevel,
        "traceback": True,
        "worker_concurrency": args.requestsperworker,
        "hostname": "worker-%s@%%h" % "".join(random.choice(string.lowercase) for _ in xrange(16))
    }
    celery_worker.run(**options)
