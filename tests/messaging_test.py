# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

from multiprocessing import Queue
from nose.tools import *
import time

import tlscanary.messaging as msg


events_received = Queue()


class MsgProcessA(msg.MessagingProcess):

    def __init__(self, *args, **kwargs):
        # Just to silence warning about missing __init__
        super(MsgProcessA, self).__init__(*args, **kwargs)

    def run(self):
        # Don't dispatch events while no one is listening
        time.sleep(0.01)
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
        time.sleep(0.011)
        self.dispatch(msg.Event("echo", "B one"))
        running = True
        while running:
            while self.events_pending():
                event = self.receive()
                events_received.put("by:%s id:%s msg:%s" % ("B", event.id, repr(event.message)))
                if event.id == "quit":
                    running = False
            time.sleep(0.002)
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


m_a = None
m_b = None
m_c = None


def proc_abc_setup():
    """Start messaging processes"""
    global m_a, m_b, m_c
    m_a = MsgProcessA()
    m_b = MsgProcessB()
    m_c = MsgProcessC()


def proc_abc_teardown():
    """Teardown must terminate processes, else nosetests will hang"""
    if m_a is not None and m_a.is_alive():
        m_a.terminate()
    if m_b is not None and m_b.is_alive():
        m_b.terminate()
    if m_c is not None and m_c.is_alive():
        m_c.terminate()


@with_setup(proc_abc_setup, proc_abc_teardown)
def test_messaging_system():
    """IPC messaging system works"""

    assert_true(msg.is_running(), "message system is running")

    while not events_received.empty():
        events_received.get()

    m_a.start()
    m_b.start()
    m_c.start()

    time.sleep(0.02)
    assert_false(m_a.is_alive(), "process A finished by itself")
    assert_true(m_b.is_alive(), "process B still running")
    assert_true(m_c.is_alive(), "process C still running")
    assert_true(msg.is_running(), "message system still running")

    msg.dispatch(msg.Event("quit"))
    time.sleep(0.02)
    assert_false(m_b.is_alive(), "process B has quit")
    assert_false(m_c.is_alive(), "process C has quit")

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
