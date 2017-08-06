# Echo client program
import socket
import time

import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class WorkerConnection(object):
    """
    Worker connection handler that tries to be smart.
    The design philosophy is to open connections ad-hoc,
    but to keep re-using an existing connection until it fails.

    It generally tries to re-send requests when a connection fails.
    """
    def __init__(self, port=5656, host="localhost", timeout=120):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.s = None  # None here signifies closed connection and socket

    def connect(self, reuse=False):
        if self.s is not None:
            if reuse:
                return
            else:
                logger.warning("Worker connection is already open. Closing existing connection.")
                self.close()

        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.settimeout(self.timeout)

        timeout_time = time.time() + self.timeout

        while True:

            if time.time() >= timeout_time:
                if self.s is not None:
                    self.s.close()
                self.s = None
                raise Exception("Worker connect timeout")

            try:
                self.s.connect((self.host, self.port))
                break

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
                    if self.s is not None:
                        self.s.close()
                    self.s = None
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

    def reconnect(self):
        if self.s is not None:
            self.shutdown()
            self.close()
        self.connect()

    def send(self, request):
        if not request.endswith("\n"):
            request += "\n"

        timeout_time = time.time() + self.timeout
        reconnected = False

        while True:

            if time.time() >= timeout_time:
                raise Exception("Sending worker request timeout")

            try:
                self.s.send(request + "\n")
                break

            except Exception as err:
                logger.warning("Error sending request: %s Reconnecting" % err)
                self.reconnect()
                reconnected = True

        return reconnected

    def receive(self):
        received = u""

        try:
            while not received.endswith("\n"):
                r = self.s.recv(4096)
                if len(r) == 0:
                    logger.warning("Empty read likely caused by peer closing connection")
                    logger.critical("Received %s" % repr(received))
                    # return None
                    return None if len(received) == 0 else received
                received += r

        except socket.error as err:
            if err.errno == 54:
                # Connection reset by peer
                logger.warning("Connection reset by peer while receiving")
                # return None
                return None if len(received) == 0 else received
            else:
                raise err

        return received.strip()

    def chat(self, requests):
        """Conduct synchronous chat"""
        self.connect(reuse=True)
        if type(requests) is str:
            requests = [requests]
        replies = []
        for request in requests:
            reply = None
            while reply is None:
                self.send(request)
                reply = self.receive()
            replies.append(reply)
        return replies if len(requests) > 0 else replies[0]

    def async_chat(self, requests):
        """
        Conduct asynchronous chat with worker.
        There is no guarantee that replies arrive in order of requests,
        hence all requests must be re-sent if the connection fails at
        any time.
        """
        self.connect(reuse=True)
        while True:
            # Send all the requests
            for request in requests:
                reconnected = self.send(request)
                if reconnected:
                    self.reconnect()
                    continue
            # Listen for replies until done or connection breaks
            replies = []
            while True:
                received = self.receive()
                if received is None:
                    break
                replies.append(received)
                if len(replies) == len(requests):
                    return replies


wc = WorkerConnection()
i = 1
while True:

    data = wc.chat("""{"mode":"setid","args":{"id":%d}}\n""" % i)
    logger.critical("received: %s" % repr(data))

    data = wc.chat("""{"mode":"info"}\n""")
    logger.critical("received: %s" % repr(data))

    data = wc.chat("""{"mode":"info"}\n""")
    logger.critical("received: %s" % repr(data))

    i += 1

    # time.sleep(0.001)
