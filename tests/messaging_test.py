# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

from multiprocessing import Queue
from nose.tools import *
from Queue import Empty
import time

import tlscanary.messaging as msg


events_received = Queue()


class MsgProcessA(msg.MessagingProcess):

    def __init__(self, *args, **kwargs):
        # Just to silence warning about missing __init__
        super(MsgProcessA, self).__init__(*args, **kwargs)

    def run(self):
        # Don't dispatch events while no one is listening
        time.sleep(0.03)
        self.dispatch(msg.Event("log", "A one"))
        self.dispatch(msg.Event("echo", "A two"))
        self.dispatch(msg.Event("log", "A three"))
        self.dispatch(msg.Event("invalid", "A four"))
        self.stop_events()


class MsgProcessB(msg.MessagingProcess):

    def __init__(self, *args, **kwargs):
        # Just to silence warning about missing __init__
        super(MsgProcessB, self).__init__(*args, **kwargs)

    def run(self):
        self.start_listening(["quit", "log", "echo"])
        self.stop_listening("echo")
        time.sleep(0.033)
        self.dispatch(msg.Event("echo", "B one"))
        running = True
        while running:
            while self.events_pending():
                event = self.receive()
                events_received.put("by:%s id:%s msg:%s" % ("B", event.id, repr(event.message)))
                if event.id == "quit":
                    running = False
            time.sleep(0.006)
        self.stop_events()


class MsgProcessC(msg.MessagingProcess):

    def __init__(self, *args, **kwargs):
        # Just to silence warning about missing __init__
        super(MsgProcessC, self).__init__(*args, **kwargs)

    def run(self):
        self.start_listening(["quit", "log", "echo"])
        while True:
            event = self.receive()
            events_received.put("by:%s id:%s msg:%s" % ("C", event.id, repr(event.message)))
            if event.id == "quit":
                break
            elif event.id == "echo":
                self.dispatch(msg.Event("log", {"repeating": event}))
        self.stop_events()


proc_a = None
proc_b = None
proc_c = None


def proc_abc_setup():
    """Start messaging processes"""
    global proc_a, proc_b, proc_c
    proc_a = MsgProcessA(name="MsgProcessA")
    proc_b = MsgProcessB(name="MsgProcessB")
    proc_c = MsgProcessC(name="MsgProcessC")


def proc_abc_teardown():
    """Teardown must terminate processes, else nosetests will hang"""
    if proc_a is not None and proc_a.is_alive():
        proc_a.terminate()
    if proc_b is not None and proc_b.is_alive():
        proc_b.terminate()
    if proc_c is not None and proc_c.is_alive():
        proc_c.terminate()


@with_setup(proc_abc_setup, proc_abc_teardown)
def test_messaging_process():
    """IPC messaging system works for sub-processes"""

    assert_true(msg.is_running(), "message system is running")

    while not events_received.empty():
        events_received.get()

    proc_a.start()
    proc_b.start()
    proc_c.start()

    time.sleep(0.06)
    assert_false(proc_a.is_alive(), "process A finished by itself")
    assert_true(proc_b.is_alive(), "process B still running")
    assert_true(proc_c.is_alive(), "process C still running")
    assert_true(msg.is_running(), "message system still running")

    msg.dispatch(msg.Event("quit"))
    time.sleep(0.06)
    assert_false(proc_b.is_alive(), "process B has quit")
    assert_false(proc_c.is_alive(), "process C has quit")

    events = []
    while not events_received.empty():
        events.append(events_received.get())
    expected = ["by:C id:log msg:'A one'",
                "by:C id:echo msg:'A two'",
                "by:C id:log msg:'A three'",
                "by:C id:echo msg:'B one'",
                "by:C id:log msg:{'repeating': <Event('echo', 'A two')>}",
                "by:C id:log msg:{'repeating': <Event('echo', 'B one')>}",
                "by:B id:log msg:'A one'",
                "by:B id:log msg:'A three'",
                "by:B id:log msg:{'repeating': <Event('echo', 'A two')>}",
                "by:B id:log msg:{'repeating': <Event('echo', 'B one')>}",
                "by:C id:quit msg:None",
                "by:B id:quit msg:None"]
    assert_equal(sorted(events), sorted(expected), "produces expected event history")


threads_running = True


class MsgThreadA(msg.MessagingThread):

    def __init__(self, *args, **kwargs):
        # Just to silence warning about missing __init__
        super(MsgThreadA, self).__init__(*args, **kwargs)

    def run(self):
        # Don't dispatch events while no one is listening
        time.sleep(0.03)
        self.dispatch(msg.Event("log", "A one"))
        self.dispatch(msg.Event("echo", "A two"))
        self.dispatch(msg.Event("log", "A three"))
        self.dispatch(msg.Event("invalid", "A four"))
        self.stop_events()


class MsgThreadB(msg.MessagingThread):

    def __init__(self, *args, **kwargs):
        # Just to silence warning about missing __init__
        super(MsgThreadB, self).__init__(*args, **kwargs)

    def run(self):
        self.start_listening(["quit", "log", "echo"])
        self.stop_listening("echo")
        time.sleep(0.033)
        self.dispatch(msg.Event("echo", "B one"))
        running = True
        while running and threads_running:
            while self.events_pending():
                try:
                    event = self.receive(timeout=2)
                except Empty:
                    break
                events_received.put("by:%s id:%s msg:%s" % ("B", event.id, repr(event.message)))
                if event.id == "quit":
                    running = False
            time.sleep(0.006)
        self.stop_events()


class MsgThreadC(msg.MessagingThread):

    def __init__(self, *args, **kwargs):
        # Just to silence warning about missing __init__
        super(MsgThreadC, self).__init__(*args, **kwargs)

    def run(self):
        self.start_listening(["quit", "log", "echo"])
        while threads_running:
            try:
                event = self.receive(timeout=2)
            except Empty:
                break
            events_received.put("by:%s id:%s msg:%s" % ("C", event.id, repr(event.message)))
            if event.id == "quit":
                break
            elif event.id == "echo":
                self.dispatch(msg.Event("log", {"repeating": event}))
        self.stop_events()


thread_a = None
thread_b = None
thread_c = None


def thread_abc_setup():
    """Start messaging threads"""
    global thread_a, thread_b, thread_c
    thread_a = MsgThreadA(name="MsgThreadA")
    thread_b = MsgThreadB(name="MsgThreadB")
    thread_c = MsgThreadC(name="MsgThreadC")


def thread_abc_teardown():
    """Teardown must terminate processes, else nosetests will hang"""
    global threads_running
    threads_running = False


@with_setup(thread_abc_setup, thread_abc_teardown)
def test_messaging_thread():
    """IPC messaging system works for threads"""

    assert_true(msg.is_running(), "message system is running")

    while not events_received.empty():
        events_received.get()

    thread_a.start()
    thread_b.start()
    thread_c.start()

    time.sleep(0.06)
    assert_false(thread_a.is_alive(), "thread A finished by itself")
    assert_true(thread_b.is_alive(), "thread B still running")
    assert_true(thread_c.is_alive(), "thread C still running")
    assert_true(msg.is_running(), "message system still running")

    msg.dispatch(msg.Event("quit"))
    time.sleep(0.06)
    assert_false(thread_b.is_alive(), "thread B has quit")
    assert_false(thread_c.is_alive(), "thread C has quit")

    events = []
    while not events_received.empty():
        events.append(events_received.get())
    expected = ["by:C id:log msg:'A one'",
                "by:C id:echo msg:'A two'",
                "by:C id:log msg:'A three'",
                "by:C id:echo msg:'B one'",
                "by:C id:log msg:{'repeating': <Event('echo', 'A two')>}",
                "by:C id:log msg:{'repeating': <Event('echo', 'B one')>}",
                "by:B id:log msg:'A one'",
                "by:B id:log msg:'A three'",
                "by:B id:log msg:{'repeating': <Event('echo', 'A two')>}",
                "by:B id:log msg:{'repeating': <Event('echo', 'B one')>}",
                "by:C id:quit msg:None",
                "by:B id:quit msg:None"]
    assert_equal(sorted(events), sorted(expected), "produces expected event history")
