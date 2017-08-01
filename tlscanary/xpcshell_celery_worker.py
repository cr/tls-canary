# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import json
import logging
from multiprocessing import Lock, Queue
import os
from Queue import Empty
import subprocess
from threading import Thread
from uuid import uuid1

import tlscanary.messaging as msg


logger = logging.getLogger(__name__)
module_dir = os.path.split(__file__)[0]


class WorkerReader(Thread):
    """
    Reader thread that reads messages from the worker.
    The convention is that all worker output that parses
    as JSON is routed to the response queue, else it is
    interpreted as a JavaScript error or warning.
    """
    def __init__(self, worker, daemon=False, name="WorkerReader"):
        """
        WorkerReader constructor

        :param worker: XPCShellWorker parent instance
        """
        super(WorkerReader, self).__init__()
        self.worker = worker
        self.daemon = daemon
        if name is not None:
            self.setName(name)

    def run(self):
        global logger

        logger.debug('Reader thread started for worker %s' % self.worker.id)
        # This thread will automatically end when worker's stdout is closed
        for line in iter(self.worker.worker_thread.stdout.readline, b''):
            line = line.strip()
            try:
                self.worker.response_queue.put(Response(line))
                logger.debug("Received worker message: %s" % line)
            except ValueError:
                if line.startswith("JavaScript error:"):
                    logger.error("JS error from worker %s: %s" % (self.worker.id, line))
                elif line.startswith("JavaScript warning:"):
                    logger.warning("JS warning from worker %s: %s" % (self.worker.id, line))
                else:
                    logger.critical("Invalid output from worker %s: %s" % (self.worker.id, line))
        self.worker.worker_thread.stdout.close()
        logger.debug('Reader thread finished for worker %s' % self.worker.id)
        del self.worker  # Breaks cyclic reference


class WakeupHandler(msg.MessagingThread):
    """
    Handler thread that polls the worker's responses queue which is filled
    by its WorkerReader thread. In between it massages the worker with
    `wakeup` commands to keep it going.

    Responses are forwarded to the response queues in their associated
    command object.
    """
    def __init__(self, worker, daemon=False, name="WakeupHandler"):
        """
        WakeupHandler constructor

        :param worker: XPCShellWorker parent instance
        """
        super(WakeupHandler, self).__init__()
        self.worker = worker
        self.daemon = daemon
        if name is not None:
            self.setName(name)

    def run(self):
        global logger

        wakeup_cmd = Command("wakeup")
        logger.debug('Wakeup handler thread started for worker %s' % self.worker.id)
        # This thread will automatically end when worker's stdout is closed
        while self.worker.is_running():
            try:
                response = self.worker.response_queue.get(timeout=0.05)

                # Respond via event if the original command had `response_event` argument set
                if response.event_requested():
                    # Response objects can be pickled, so dispatching directly
                    self.dispatch(msg.Event(response.original_cmd["id"], response))
                    # If an event response was requested, don't serve command response queues
                    continue

                # If no response event was requested, serve command queues
                cmd = self.worker.get_pending(response)
                logger.error("Command for response %s from worker %s is %s" % (response, self.worker.id, cmd))
                # Note: The worker sends an ACK and at most one response per command
                if cmd is not None:
                    if response.is_ack():
                        # ACK received
                        cmd.put_ack(response)
                    else:
                        # Actual result received, route to associated pending command object
                        cmd.put_result(response)
                    # Remove command from pending listeners if all expected responses received
                    self.worker.check_pending(cmd)
                else:
                    if not response.is_ack():
                        logger.error("Ignoring worker response because no one is listening to response `%s`"
                                     % str(response))
            except Empty:
                # Wake up the worker if there are commands pending,
                # but don't pend for command's response.
                if self.worker.has_pending():
                    # It is important to never pend for wakeup commands,
                    # as this would create a self-feeding wakeup loop.
                    self.worker.send(wakeup_cmd, set_pending=False)

        logger.debug('Wakeup handler thread finished for worker %s' % self.worker.id)
        del self.worker  # Breaks cyclic reference


class EventHandler(msg.MessagingThread):

    def __init__(self, worker, *args, **kwargs):
        super(EventHandler, self).__init__(receiver_id=worker.id, *args, **kwargs)
        self.worker = worker

    def run(self):
        logger.debug("XPCShell event handler started for worker %s" % self.worker.id)
        self.start_listening(["quit", "command:%s" % self.worker.id])
        while True:
            event = self.receive()
            if event.id.startswith("command:"):
                cmd = event.message  # Command() objects come pickled
                self.worker.send(cmd, set_pending=False)
                if "response_event" not in cmd.args:
                    logger.warning("Command `%s` will not generate a response event" % str(cmd))
            elif event.id == "quit":
                break
        self.stop_events()
        logger.debug("XPCShell event handler finished for worker %s", self.worker.id)


class XPCShellWorker(object):
    """XPCShell worker implementing an asynchronous, JSON-based message system"""

    def __init__(self, app, script=None, profile=None, prefs=None, events=False):
        global module_dir

        self.id = str(uuid1())
        self.__app = app
        if script is None:
            self.__script = os.path.join(module_dir, "js", "scan_worker.js")
        else:
            self.__script = script
        self.__profile = profile
        self.__prefs = prefs
        self.worker_thread = None
        self.__reader_thread = None
        self.__wakeup_handler = None
        self.__event_handler = None
        self.__events = events
        self.__write_lock = Lock()
        self.response_queue = Queue()
        self.__pending = dict()
        self.__pending_lock = Lock()

    def spawn(self):
        """Spawn the worker process and its dedicated handler threads"""
        global logger, module_dir

        cmd = [self.__app.exe, '-xpcshell', "-g", self.__app.gredir, "-a", self.__app.browser, self.__script]
        logger.debug("Executing worker shell command `%s`" % ' '.join(cmd))

        self.worker_thread = subprocess.Popen(
            cmd,
            cwd=self.__app.browser,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1)  # `1` means line-buffered

        # Spawn a reader thread, because stdio reads are blocking
        self.__reader_thread = WorkerReader(self, daemon=True)
        self.__reader_thread.start()

        # Spawn wakeup handler
        self.__wakeup_handler = WakeupHandler(self, daemon=True)
        self.__wakeup_handler.start()

        if self.__events:
            # Spawn wakeup handler
            self.__event_handler = EventHandler(self)
            self.__event_handler.start()

        logger.debug("Syncing worker IDs to %s" % repr(self.id))
        set_id_cmd = Command("setid", id=self.id)
        if not self.send(set_id_cmd):
            logger.critical("Failed to set new worker ID")
            return False
        try:
            success, _ = set_id_cmd.get_ack(timeout=2)
        except Empty:
            logger.critical("Worker failed to acknowledge ID change")
            return False
        if not success:
            logger.error("Worker failed to change ID")
            return False

        if self.__profile is not None:
            logger.debug("Changing worker profile to `%s`" % self.__profile)
            profile_cmd = Command("useprofile", path=self.__profile)
            if not self.send(profile_cmd):
                logger.critical("Failed to send worker command for switching profile")
                return False
            try:
                success, _ = profile_cmd.get_ack(timeout=2)

            except Empty:
                logger.critical("Worker failed to acknowledge profile switch")
                return False
            if not success:
                logger.error("Worker failed to switch profile to `%s`" % self.__profile)
                return False

        if self.__prefs is not None:
            logger.debug("Setting worker prefs to `%s`" % self.__prefs)
            prefs_cmd = Command("setprefs", prefs=self.__prefs)
            if not self.send(prefs_cmd):
                logger.critical("Failed to send worker command for setting prefs")
                return False
            try:
                success, _ = prefs_cmd.get_ack(timeout=2)
            except Empty:
                logger.critical("Worker failed to acknowledge new prefs")
                return False
            if not success:
                logger.error("Worker failed to set prefs `%s`" % self.__prefs)
                return False

        return True

    def terminate(self):
        """Signal the worker process to quit"""
        # The reader thread dies when the Firefox process quits
        self.worker_thread.terminate()

    def kill(self):
        """Kill the worker process"""
        self.worker_thread.kill()

    def is_running(self):
        """Check whether the worker is still fully running"""
        if self.worker_thread is None:
            return False
        return self.worker_thread.poll() is None

    def helper_threads(self):
        """Return list of helper threads"""
        helpers = []
        if self.__reader_thread is not None:
            helpers.append(self.__reader_thread)
        if self.__wakeup_handler is not None:
            helpers.append(self.__wakeup_handler)
        if self.__event_handler is not None:
            helpers.append(self.__event_handler)
        return helpers

    def helpers_running(self):
        """Returns whether helpers are still running"""
        for helper in self.helper_threads():
            if helper.is_alive():
                return True
        return False

    def send(self, cmd, set_pending=True):
        """Send a command message to the worker"""
        global logger

        cmd_string = str(cmd)
        logger.debug("Sending command `%s` to worker %s" % (cmd_string, self.id))
        response_queue = None
        with self.__write_lock:
            try:
                self.worker_thread.stdin.write((cmd_string + "\n").encode("utf-8"))
                self.worker_thread.stdin.flush()
                response_queue = Queue()
                if set_pending:
                    logger.debug("Setting command `%s` as pending" % cmd.id)
                    self.set_pending(cmd)
                logger.error("Pending queue: %s" % map(str, self.__pending))
            except IOError:
                logger.debug("Can't write to worker %s. Message `%s` wasn't heard." % (self.id, cmd_string))

        return response_queue

    def set_pending(self, cmd):
        """Register a Command as pending for responses"""
        with self.__pending_lock:
            self.__pending[cmd.id] = cmd
        logger.critical("Now %s is pending: %s [%s]" % (cmd, self.__pending, self.id))

    def get_pending(self, response):
        """Return pending Command associated with a Response's ID"""
        logger.critical("Getting pending %s from %s [%s]" % (response, self.__pending, self.id))
        try:
            return self.__pending[response.id]
        except KeyError:
            return None

    def del_pending(self, cmd):
        """Un-register Command from pending for responses"""
        logger.critical("Deleting %s from pending %s [%s]" % (cmd, self.__pending, self.id))
        with self.__pending_lock:
            if cmd.id in self.__pending:
                del self.__pending[cmd.id]

    def check_pending(self, cmd):
        """Un-register Command if no more responses pending"""
        logger.warning("check_pending for command %s" % cmd.id)
        if not cmd.is_pending():
            logger.warning("removing pending marker for command %s" % cmd.id)
            self.del_pending(cmd)
        else:
            logger.warning("NOT removing pending marker for command %s" % cmd.id)

    def has_pending(self):
        """Check whether commands are pending results"""
        return len(self.__pending) > 0

    def get_result(self, block=True, timeout=None):
        return self.response_queue.get(block=block, timeout=timeout)


class Command(object):

    def __init__(self, mode_or_dict, **kwargs):

        if type(mode_or_dict) is str:
            self.id = str(uuid1())
            self.mode = mode_or_dict
            self.args = kwargs
        elif type(mode_or_dict) is dict:
            self.id = mode_or_dict["id"]
            self.mode = mode_or_dict["mode"]
            self.args = mode_or_dict["args"]
        else:
            raise Exception("Argument must be mode string or dict with command specs")

        # Do not init queues when communicating via events. First, they can't be pickled
        # to be sent as an event, and second, they won't be served anyway.
        if "response_event" in kwargs and kwargs["response_event"]:
            self.__ack_queue = None
            self.__ack_pending = False
            self.__results = None
        else:
            self.__ack_queue = Queue(maxsize=1)  # There will be one ACK
            self.__ack_pending = True
            self.__results = Queue(maxsize=1)  # Currently supporting only 0 or 1 responses

        self.__results_pending = 0

    def put_ack(self, response):
        logger.warn("put ack %s" % response.as_dict())
        if self.__ack_queue.full():
            logger.error("Received multiple ACKs for command %s. Ignoring" % self.id)
        else:
            self.__results_pending = int(response.result[4:])
            self.__ack_queue.put((response.success, self.__results_pending))
            self.__ack_pending = False
            if self.__results_pending > 1:
                logger.critical("Number of expected results must be either 0 or 1. Truncating to 1")
                self.__results_pending = 1

    def get_ack(self, block=True, timeout=None):
        if self.__ack_queue is None:
            raise Empty
        return self.__ack_queue.get(block=block, timeout=timeout)

    def put_result(self, response, block=True, timeout=None):
        if self.__results.full():
            logger.error("Command ID %s response queue is overflowing. Ignoring response" % self.id)
        else:
            self.__results_pending -= 1
            self.__results.put(response, block=block, timeout=timeout)

    def get_result(self, block=True, timeout=None):
        if self.__results is None:
            raise Empty
        return self.__results.get(block=block, timeout=timeout)

    def wait(self, timeout=None):
        results = [self.get_ack(timeout=timeout)]
        while self.is_pending():
            results.append(self.get_result(timeout=timeout))
        return results

    def is_pending(self):
        return self.__ack_pending or self.__results_pending > 0

    def as_dict(self):
        return {"id": self.id, "mode": self.mode, "args": self.args}

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
        if "worker_id" in message:
            self.worker_id = message["worker_id"]
        if "original_cmd" in message:
            self.original_cmd = message["original_cmd"]
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

    def is_ack(self):
        try:
            return self.result.startswith("ACK")
        except AttributeError:
            return False

    def results_pending(self):
        if self.is_ack():
            return int(self.result[4:])
        else:
            # None means: we don't know
            return None

    def event_requested(self):
        return "response_event" in self.original_cmd["args"] and self.original_cmd["args"]["response_event"]

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

    def __str__(self):
        return json.dumps(self.as_dict())
