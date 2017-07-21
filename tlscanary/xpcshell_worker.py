# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import json
import logging
import os
from Queue import Queue, Empty
import subprocess
from threading import Thread


logger = logging.getLogger(__name__)
module_dir = os.path.split(__file__)[0]


def read_from_worker(worker, response_queue):
    """Reader thread that reads messages from the worker.
       The convention is that all worker output that parses
       as JSON is routed to the response queue, else it is
       interpreted as a JavaScript error or warning.
    """
    global logger

    logger.debug('Reader thread started for worker %s' % worker)
    for line in iter(worker.stdout.readline, b''):
        line = line.strip()
        try:
            response_queue.put(Response(line))
            logger.debug("Received worker message: %s" % line)
        except ValueError:
            if line.startswith("JavaScript error:"):
                logger.error("JS error from worker %s: %s" % (worker, line))
            elif line.startswith("JavaScript warning:"):
                logger.warning("JS warning from worker %s: %s" % (worker, line))
            else:
                logger.critical("Invalid output from worker %s: %s" % (worker, line))
    logger.debug('Reader thread finished for worker %s' % worker)
    worker.stdout.close()


class XPCShellWorker(object):
    """XPCShell worker implementing an asynchronous, JSON-based message system"""

    def __init__(self, app, script=None, profile=None, prefs=None):
        global module_dir

        self.__app = app
        if script is None:
            self.__script = os.path.join(module_dir, "js", "scan_worker.js")
        else:
            self.__script = script
        self.__profile = profile
        self.__prefs = prefs
        self.__worker_thread = None
        self.__reader_thread = None
        self.__response_queue = Queue()

    def spawn(self):
        """Spawn the worker process and its dedicated reader thread"""
        global logger, module_dir

        cmd = [self.__app.exe, '-xpcshell', "-g", self.__app.gredir, "-a", self.__app.browser, self.__script]
        logger.debug("Executing worker shell command `%s`" % ' '.join(cmd))

        self.__worker_thread = subprocess.Popen(
            cmd,
            cwd=self.__app.browser,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1)  # `1` means line-buffered

        # Spawn a reader thread, because stdio reads are blocking
        self.__reader_thread = Thread(target=read_from_worker, name="Reader",
                                      args=(self.__worker_thread, self.__response_queue))
        self.__reader_thread.daemon = True  # Thread dies with worker
        self.__reader_thread.start()

        if self.__profile is not None:
            logger.debug("Changing worker profile to `%s`" % self.__profile)
            if self.send(Command("useprofile", path=self.__profile)):
                response = self.wait()
            else:
                # .wait() would wait forever if .send() was not successful
                response = None
            if response is None or response.original_cmd["mode"] != "useprofile" or response.result != "ACK":
                logger.error("Worker failed to set profile `%s`" % self.__profile)
                return False

        if self.__prefs is not None:
            logger.debug("Setting worker prefs to `%s`" % self.__prefs)
            if self.send(Command("setprefs", prefs=self.__prefs)):
                response = self.wait()
            else:
                # .wait() would wait forever if .send() was not successful
                response = None
            if response is None or response.original_cmd["mode"] != "setprefs" or response.result != "ACK":
                logger.error("Worker failed to set prefs `%s`" % self.__prefs)
                return False

        return True

    def terminate(self):
        """Signal the worker process to quit"""
        # The reader thread dies when the Firefox process quits
        self.__worker_thread.terminate()

    def kill(self):
        """Kill the worker process"""
        self.__worker_thread.kill()

    def is_running(self):
        """Check whether the worker is still fully running"""
        if self.__worker_thread is None:
            return False
        return self.__worker_thread.poll() is None

    def send(self, cmd):
        """Send a command message to the worker"""
        global logger

        cmd_string = str(cmd)
        logger.debug("Sending worker message: `%s`" % cmd_string)
        try:
            self.__worker_thread.stdin.write((cmd_string + "\n").encode("utf-8"))
            self.__worker_thread.stdin.flush()
        except IOError:
            logger.debug("Can't write to worker. Message `%s` wasn't heard." % cmd_string)
            return False
        return True

    def receive(self):
        """Read queued messages from worker. Returns [] if there were none."""

        global logger

        # Read everything from the reader queue
        responses = []
        try:
            while True:
                responses.append(self.__response_queue.get_nowait())
        except Empty:
            pass

        return responses

    def wait(self):
        """Wait for and return the next single message from the worker."""
        return self.__response_queue.get()


class Command(object):

    def __init__(self, mode, id=None, **kwargs):
        if mode is None:
            raise Exception("Refusing to init mode-less command")
        self.__id = id
        self.__mode = mode
        self.__args = kwargs

    def as_dict(self):
        return {"id": self.__id, "mode": self.__mode, "args": self.__args}

    def __str__(self):
        return json.dumps(self.as_dict())


class Response(object):

    def __init__(self, message_string):
        global logger

        self.id = None
        self.worker_id = None
        self.original_cmd = None
        self.success = None
        self.result = None
        self.elapsed_ms = None
        message = json.loads(message_string)  # May throw ValueError
        if "id" in message:
            self.id = message["id"]
        if "original_cmd" in message:
            self.original_cmd = message["original_cmd"]
        if "worker_id" in message:
            self.worker_id = message["worker_id"]
        if "success" in message:
            self.success = message["success"]
        if "result" in message:
            self.result = message["result"]
        if "command_time" in message:
            self.command_time = message["command_time"]
        if "response_time" in message:
            self.response_time = message["response_time"]
        if len(message) != 7:
            logger.error("Worker response has unexpected format: %s" % message_string)

    def as_dict(self):
        return {
            "id": self.id,
            "original_cmd": self.original_cmd,
            "worker_id": self.worker_id,
            "success": self.success,
            "result": self.result,
            "command_time": self.command_time,
            "response_time": self.response_time,
        }
