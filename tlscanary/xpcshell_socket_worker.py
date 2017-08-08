# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import json
import logging
import os
import socket
import subprocess
from threading import Thread
import time
from uuid import uuid1


logger = logging.getLogger(__name__)
module_dir = os.path.split(__file__)[0]


class XPCShellWorker(object):
    """XPCShell worker implementing an asynchronous, JSON-based message system"""

    def __init__(self, app, worker_id=None, script=None, profile=None, prefs=None):
        global module_dir

        self.id = str(worker_id) if worker_id is not None else str(uuid1())
        self.port = None
        self.__app = app
        if script is None:
            self.__script = os.path.join(module_dir, "js", "xpcshell_worker.js")
        else:
            self.__script = script
        self.__profile = profile
        self.__prefs = prefs
        self.worker_process = None
        self.__reader_thread = None

    def spawn(self, port=None):
        """Spawn the worker process and its dedicated handler threads"""
        global logger, module_dir

        if self.is_running():
            logger.warning("Re-spawning worker %s which was already running" % self.id)
            self.terminate()

        if port is None:
            port = self.port if self.port is not None else 0

        cmd = [self.__app.exe, '-xpcshell', "-g", self.__app.gredir,
               "-a", self.__app.browser, self.__script, str(port)]
        logger.debug("Executing worker shell command `%s`" % ' '.join(cmd))

        self.worker_process = subprocess.Popen(
            cmd,
            cwd=self.__app.browser,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1)  # `1` means line-buffered

        if self.worker_process.poll() is not None:
            logger.critical("Unable to start worker %s process. Poll yields %d"
                            % (self.id, self.worker_process.poll()))

        # First line the worker prints to stdout reports success or fail.
        status = self.worker_process.stdout.readline().strip()
        logger.debug("Worker %s reported startup status: %s" % (self.id, status))
        if not status.startswith("INFO:"):
            logger.critical("Worker %s can't get socket on requested port %d" % (self.id, port))
            self.terminate()
            return False

        # Actual port is reported as last word on INFO line
        self.port = int(status.split(" ")[-1])
        logger.debug("Worker %s has PID %s and is listening on port %d"
                     % (self.id, self.worker_process.pid, self.port))

        # Spawn a reader thread for worker log messages,
        # because stdio reads are blocking.
        self.__reader_thread = WorkerReader(self, daemon=True)
        self.__reader_thread.start()

        conn = WorkerConnection(self.port, timeout=2)

        logger.debug("Syncing worker ID to %s" % repr(self.id))
        res = conn.ask(Command("setid", id=self.id))
        if res is None or not res.is_ack() or not res.is_success():
            logger.error("Failed to sync worker ID to `%s`" % self.id)
            self.terminate()
            return False

        if self.__profile is not None:
            logger.debug("Changing worker profile to `%s`" % self.__profile)
            res = conn.ask(Command("useprofile", path=self.__profile))
            if res is None or not res.is_ack() or not res.is_success():
                logger.error("Worker failed to switch profile to `%s`" % self.__profile)
                self.terminate()
                return False

        if self.__prefs is not None:
            logger.debug("Setting worker prefs to `%s`" % self.__prefs)
            res = conn.ask(Command("setprefs", prefs=self.__prefs))
            if res is None or not res.is_ack() or not res.is_success():
                logger.error("Worker failed to set prefs to `%s`" % self.__prefs)
                self.terminate()
                return False

        return True

    def quit(self, timeout=5):
        """Send `quit` command to worker."""
        if self.is_running():
            return self.ask(Command("quit"), timeout=timeout)
        else:
            logger.warning("Not quitting a stopped worker")
            return None

    def terminate(self):
        """Signal the worker process to quit"""
        # The reader thread dies when the Firefox process quits
        self.worker_process.terminate()

    def kill(self):
        """Kill the worker process"""
        self.worker_process.kill()

    def is_running(self):
        """Check whether the worker is still fully running"""
        if self.worker_process is None:
            return False
        return self.worker_process.poll() is None

    def helper_threads(self):
        """Return list of helper threads"""
        helpers = []
        if self.__reader_thread is not None:
            helpers.append(self.__reader_thread)
        return helpers

    def helpers_running(self):
        """Returns whether helpers are still running"""
        for helper in self.helper_threads():
            if helper.is_alive():
                return True
        return False

    def get_connection(self, timeout=None):
        if not self.is_running() or self.port is None:
            return None
        return WorkerConnection(self.port, timeout=timeout)

    def ask(self, cmd, always_reconnect=False, retry=True, timeout=5):
        """Send command or worker and return response"""
        connection = self.get_connection(timeout=timeout)
        if connection is None:
            logger.warning("Asking stopped worker %s", self.id)
            return None
        reply = connection.ask(cmd, always_reconnect=always_reconnect, retry=retry)
        connection.disconnect()
        return reply

    def chat(self, cmds, always_reconnect=False, timeout=5):
        """Send list of commands to worker and return responses"""
        connection = self.get_connection(timeout=timeout)
        if connection is None:
            logger.warning("Chatting stopped worker %s", self.id)
            return [None] * len(cmds)
        replies = connection.chat(cmds, always_reconnect=always_reconnect)
        connection.disconnect()
        return replies


class WorkerReader(Thread):
    """
    Reader thread that reads log messages from the worker's stdout.
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
        for line in iter(self.worker.worker_process.stdout.readline, b''):
            line = line.strip()
            if line.startswith("JavaScript error:"):
                logger.error("JS error from worker %s: %s" % (self.worker.id, line))
            elif line.startswith("JavaScript warning:"):
                logger.warning("JS warning from worker %s: %s" % (self.worker.id, line))
            elif line.startswith("DEBUG:"):
                logger.debug("Worker %s: %s" % (self.worker.id, line[7:]))
            elif line.startswith("INFO:"):
                logger.info("Worker %s: %s" % (self.worker.id, line[6:]))
            elif line.startswith("WARNING:"):
                logger.warning("Worker %s: %s" % (self.worker.id, line[9:]))
            elif line.startswith("ERROR:"):
                logger.error("Worker %s: %s" % (self.worker.id, line[7:]))
            elif line.startswith("CRITICAL:"):
                logger.critical("Worker %s: %s" % (self.worker.id, line[10:]))
            else:
                logger.critical("Invalid output from worker %s: %s" % (self.worker.id, line))

        self.worker.worker_process.stdout.close()
        logger.debug('Reader thread finished for worker %s' % self.worker.id)
        del self.worker  # Breaks cyclic reference


class Command(object):
    """Worker command object"""

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

    def is_success(self):
        return self.success

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


class WorkerConnection(object):
    """
    Worker connection handler that tries to be smart.
    The design philosophy is to open connections ad-hoc,
    but to keep re-using an existing connection until it fails.

    It generally tries to re-send requests when a connection fails.
    """
    def __init__(self, port, host="localhost", timeout=120):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.s = None  # None here signifies closed connection and socket

    def connect(self, reuse=False, timeout=-1):
        timeout = timeout if timeout is None or timeout >= 0 else self.timeout

        if self.s is not None:
            if reuse:
                return
            else:
                logger.warning("Worker connection is already open. Closing existing connection.")
                self.close()

        timeout_time = time.time() + timeout if timeout is not None else None
        while True:

            if timeout_time is not None and time.time() >= timeout_time:
                self.close()
                raise socket.timeout("Worker connect timeout")

            try:
                self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                # logger.error("***  %s" % self.s.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE))
                # self.s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                # logger.error("***  %s" % self.s.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE))
                self.s.settimeout(timeout)
                self.s.connect((self.host, self.port))
                return

            except socket.error as err:
                if err.errno == 61:
                    logger.warning("Worker refused connection. Is it listening?. Retrying")
                    time.sleep(5)
                    continue
                elif err.errno == 49:  # Can't assign requested address
                    # The OS may need some time to garbage collect closed sockets.
                    logger.warning("OS is probably out of sockets. Retrying")
                    time.sleep(1)
                    continue
                else:
                    self.close()
                    raise err

    def close(self):
        """Close the socket itself"""
        if self.s is None:
            logger.warning("Worker socket is already closed")
        else:
            self.s.close()
            self.s = None

    def shutdown(self):
        """Shutdown existing connection on the socket"""
        if self.s is None:
            logger.warning("Can't shutdown closed worker socket")
            return
        try:
            self.s.shutdown(socket.SHUT_RDWR)
        except socket.error as err:
            if err.errno == 57:  # Socket is not connected
                logger.warning("Unable to shutdown unconnected worker socket")
            else:
                raise err

    def disconnect(self):
        self.shutdown()
        self.close()

    def reconnect(self, timeout=-1):
        if self.s is not None:
            self.disconnect()
        self.connect(timeout=timeout)

    def send(self, request, retry=True, timeout=-1):
        timeout = timeout if timeout is None or timeout >= 0 else self.timeout

        request = str(request).strip() + "\n"

        if timeout is None:
            timeout = self.timeout

        reconnected = False
        timeout_time = time.time() + timeout if timeout is not None else None
        while True:

            if timeout_time is not None and time.time() >= timeout_time:
                raise socket.timeout("Worker timeout while sending request")

            try:
                logger.debug("Sending request `%s` on port %d" % (request, self.port))
                self.s.settimeout(timeout)
                self.s.send(request)
                break

            except Exception as err:
                logger.warning("Error sending request: %s Reconnecting" % err)
                self.reconnect()
                reconnected = True
                if not retry:
                    break

        return reconnected

    def receive(self, raw=False, timeout=-1):
        timeout = timeout if timeout is None or timeout >= 0 else self.timeout

        received = u""

        try:
            while not received.endswith("\n"):
                self.s.settimeout(timeout)
                r = self.s.recv(4096)
                if len(r) == 0:
                    logger.warning("Empty read likely caused by peer closing connection")
                    logger.critical("Received %s" % repr(received))
                    return None
                received += r

        except socket.error as err:
            if err.errno == 54:
                # Connection reset by peer
                logger.warning("Connection reset by peer while receiving")
                return None
            else:
                raise err

        received = received.strip()
        logger.debug("Received worker reply `%s` on port %d" % (received, self.port))

        if raw:
            return received
        else:
            return Response(received)

    def ask(self, request, always_reconnect=False, retry=True, timeout=-1):
        """Send single request to worker and wait for and return reply"""
        if always_reconnect:
            self.reconnect()
        else:
            self.connect(reuse=True, timeout=timeout)
        reply = None

        while reply is None:
            self.send(request, timeout=timeout)
            reply = self.receive(timeout=timeout)
            if reply is None and not retry:
                break
        return reply

    def chat(self, requests, always_reconnect=False, timeout=-1):
        """Conduct synchronous chat over commands or verbatim requests"""
        if always_reconnect:
            self.reconnect(timeout=timeout)
        else:
            self.connect(reuse=True, timeout=timeout)
        timeout = timeout if timeout is None or timeout >= 0 else self.timeout

        replies = []
        for request in requests:
            reply = None
            timeout_time = time.time() + timeout if timeout is not None else None
            while reply is None:
                if timeout_time is not None and time.time() >= timeout_time:
                    raise socket.timeout("Worker timeout during chat")
                self.send(request, timeout=timeout)
                reply = self.receive(timeout=timeout)
            replies.append(reply)
        return replies

    def async_chat(self, requests, timeout=-1):
        """
        Conduct asynchronous chat with worker.
        There is no guarantee that replies arrive in order of requests,
        hence all requests must be re-sent if the connection fails at
        any time.
        """
        self.connect(reuse=True, timeout=timeout)
        timeout_time = time.time() + timeout if timeout is not None else None
        while True:

            if timeout_time is not None and time.time() >= timeout_time:
                self.close()
                raise socket.timeout("Worker async chat timeout")

            # Send all the requests
            for request in requests:
                reconnected = self.send(request, timeout=timeout)
                if reconnected:
                    self.reconnect(timeout=timeout)
                    continue
            # Listen for replies until done or connection breaks
            replies = []
            while True:
                received = self.receive(timeout=timeout)
                if received is None:
                    break
                replies.append(received)
                if len(replies) == len(requests):
                    return replies
