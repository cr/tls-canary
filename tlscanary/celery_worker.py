# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

from celery import Celery, current_app, signals, Task, bootsteps
from celery.bin import worker
from celery.worker.control import control_command
from kombu import Consumer, Exchange, Queue
import logging
import random
import resource
import shutil
import string
from threading import current_thread
import time
import tempfile

import tlscanary.xpcshell_socket_worker as xw
import tlscanary.firefox_downloader as fd
import tlscanary.firefox_extractor as fe


workdir = tempfile.mkdtemp(prefix="tlscanarytest_")

app = Celery()
xpw = None


logger = logging.getLogger(__name__)


def download_firefox_app(build):
    platform = fd.FirefoxDownloader.detect_platform()
    fdl = fd.FirefoxDownloader(workdir, cache_timeout=60*60)
    build_archive_file = fdl.download(build, platform)
    if build_archive_file is None:
        shutil.rmtree(workdir, ignore_errors=True)
        return None
    # Extract candidate archive
    candidate_app = fe.extract(build_archive_file, workdir, cache_timeout=60*60)
    candidate_app.package_origin = fdl.get_download_url(build, platform)
    return candidate_app


class InitFirefoxStep(bootsteps.StartStopStep):
    requires = {"celery.worker.components:Pool"}

    def __init__(self, worker, **kwargs):
        print "called new worker bootstep with {0!r}".format(kwargs)

    def create(self, worker):
        return self

    def start(self, worker):
        global xpw
        print "XPCShell worker starting"
        candidate_app = download_firefox_app("nightly")
        if candidate_app is None:
            print "Unable to download Firefox. Worker is non-functional"
            return
        xpw = xw.XPCShellWorker(candidate_app)
        xpw.spawn()
        print "worker started"

    def stop(self, worker):
        print "worker stopped"

    def terminate(self, worker):
        print "XPCShell worker terminating"
        xpw.terminate()
        while xpw.is_running():
            print "still running"
            time.sleep(0.1)
        shutil.rmtree(workdir)
        print "worker terminated"


@control_command(
    args=[],
    signature="[N=0]"
)
def update_firefox(state):
    global xpw
    xpw.terminate()
    while xpw.is_running():
        print "still running"
        time.sleep(0.1)
    candidate_app = download_firefox_app("nightly")
    if candidate_app is None:
        print "Unable to download Firefox. Worker is non-functional"
        return
    xpw = xw.XPCShellWorker(candidate_app)
    xpw.spawn()


update_firefox_queue = Queue("custom", Exchange("custom"), "update_firefox")


class FirefoxUpdate(bootsteps.ConsumerStep):
    def get_consumers(self, channel):
        return [Consumer(channel, queues=[update_firefox_queue], callbacks=[self.handle_message], accept=["pickle"])]

    def handle_message(self, body, message):
        print "Received message: {0!r}".format(body)
        message.ack()


def send_update_firefox_message(who, producer=None):
    with app.producer_or_acquire(producer) as prod:
        prod.publish(
            {"hello": who},
            serializer="pickle",
            exchange=update_firefox_queue.exchange,
            routing_key=update_firefox_queue.routing_key,
            declare=[update_firefox_queue],
            retry=True)


@app.task(bind=True)
def run_command(self, mode, **kwargs):
    # self.start_listening(self.request.id)
    # kwargs["request_id"] = self.request.id
    # msg.cm
    # self.dispatch(msg.Event(mode, **kwargs))
    time.sleep(1 + random.random()*3)
    return "%s got this: %s" % (str(current_thread()), str(self.request))


def start(args, xpcsw, result_backend="rpc://", loglevel="DEBUG"):
    global aapp, xpw, logger
    xpw = xpcsw
    # Increase limit of open files to 500
    # Default 256 on OS X is not enough for a pool of 50 concurrent workers
    resource.setrlimit(resource.RLIMIT_NOFILE, (1000, -1))
    # app = Celery(backend="rpc://", broker="redis://localhost")
    logger.critical("A"*100)
    print "A"*1000
    logger.debug("Dumping celery app configuration")
    for config_key in sorted(app.conf.keys()):
        logger.debug("%s: %s" % (config_key, app.conf[config_key]))
    logger.critical("B"*100)

    app.conf.update(
        broker_url=args.broker,
        enable_utc=True,
        result_backend=result_backend,
        task_send_set_event=True,
        tast_serializer="pickle",
        timezone="UTC",
        worker_concurrency=args.requestsperworker,
        worker_hijack_root_logger=True,
        worker_pool="solo",
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
    app.steps['worker'].add(InitFirefoxStep)
    celery_worker = worker.worker(app=app)
    options = {
        "loglevel": loglevel,
        "traceback": True,
        "worker_concurrency": args.requestsperworker,
        "hostname": "worker-%s@%%h" % "".join(random.choice(string.lowercase) for _ in xrange(16))
    }
    celery_worker.run(**options)
