# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

from nose import SkipTest
from nose.tools import *
from time import sleep

import tests
import tlscanary.messaging as msg
import tlscanary.xpcshell_celery_worker as xw


def test_xpcshell_celery_worker():
    """XPCShell celery worker runs and is responsive"""

    # Skip test if there is no app for this platform
    if tests.test_app is None:
        raise SkipTest("XPCShell worker can not be tested on this platform")

    # Spawn a worker
    w = xw.XPCShellWorker(tests.test_app)
    w.spawn()
    assert_true(w.is_running(), "XPCShell worker starts up fine")
    assert_true(w.helpers_running(), "worker helpers are running")

    # Send info command and check result
    info_cmd = xw.Command("info")
    assert_true(w.send(info_cmd), "info command can be sent")
    info_success, info_pending = info_cmd.get_ack(timeout=2)
    assert_true(info_success, "info command is acknowledged with success")
    assert_equal(info_pending, 1, "info command has one pending result")
    info_response = info_cmd.get_result(timeout=2)
    assert_true(info_response.success, "info command is successful")
    assert_true("appConstants" in info_response.result, "info response contains `appConstants`")
    assert_equal(info_response.result["appConstants"]["MOZ_UPDATE_CHANNEL"], "nightly",
                 "info response has expected value")

    # Send bogus command and check for negative ACK
    bogus_cmd = xw.Command("bogus")
    assert_true(w.send(bogus_cmd), "bogus command can be sent")
    bogus_success, bogus_pending = bogus_cmd.get_ack(timeout=2)
    assert_false(bogus_success, "bogus command is acknowledged with failure")
    assert_equal(bogus_pending, 0, "bogus command is pending no results")

    # Spawn a second worker
    ww = xw.XPCShellWorker(tests.test_app)
    ww.spawn()
    assert_true(ww.is_running(), "second XPCShell worker starts up fine")
    assert_true(ww.helpers_running(), "second worker's helpers are running")

    # Send info command to second worker check result
    info_cmd = xw.Command("info")
    assert_true(ww.send(info_cmd), "info command can be sent to second worker")
    info_success, info_pending = info_cmd.get_ack(timeout=2)
    assert_true(info_success, "info command is acknowledged with success by second worker")
    assert_equal(info_pending, 1, "info command has one pending result from second worker")
    info_response = info_cmd.get_result(timeout=2)
    assert_true("appConstants" in info_response.result, "info response from second worker contains `appConstants`")

    # Send info command to first worker again and check result
    info_cmd = xw.Command("info")
    assert_true(w.send(info_cmd), "info command can be sent again")
    info_success, info_pending = info_cmd.get_ack(timeout=2)
    assert_true(info_success, "info command is acknowledged with success again")
    assert_equal(info_pending, 1, "info command has one pending result again")
    info_response = info_cmd.get_result(timeout=2)
    assert_true("appConstants" in info_response.result, "info response contains `appConstants` again")

    quit_cmd = xw.Command("quit")
    assert_true(w.send(quit_cmd), "quit command can be sent")
    quit_success, quit_pending = quit_cmd.get_ack(timeout=2)
    assert_true(quit_success, "quit command is acknowledged with success")
    assert_equal(quit_pending, 0, "quit command is pending no results")

    helpers = w.helper_threads()
    assert_equal(len(helpers), 2, "helpers are persistent")

    w.terminate()
    sleep(0.5)
    assert_false(w.is_running(), "worker terminates after quit command")
    assert_false(w.helpers_running(), "worker helpers terminate after quit command")

    # Quit second worker
    quit_cmd = xw.Command("quit")
    assert_true(ww.send(quit_cmd), "quit command can be sent to second worker")
    quit_success, quit_pending = quit_cmd.get_ack(timeout=2)
    assert_true(quit_success, "quit command is acknowledged with success by second worker")
    assert_equal(quit_pending, 0, "quit command is pending no results from second worker")

    helpers = ww.helper_threads()
    assert_equal(len(helpers), 2, "second worker'shelpers are persistent")

    ww.terminate()
    sleep(0.5)
    assert_false(ww.is_running(), "second worker terminates after quit command")
    assert_false(ww.helpers_running(), "second worker's helpers terminate after quit command")


def test_xpcshell_celery_messaging_worker():
    """XPCShell celery messaging worker runs and is responsive to messages"""

    # Skip test if there is no app for this platform
    if tests.test_app is None:
        raise SkipTest("XPCShell messaging worker can not be tested on this platform")

    # Spawn a worker
    w = xw.XPCShellWorker(tests.test_app, events=True)
    w.spawn()

    receiver_id, receiver_queue = msg.create_receiver()

    sleep(0.1)
    cmd = xw.Command("info", response_event=True)
    # Responses will be sent as events with ID identical to command ID
    msg.start_listening(receiver_id, cmd.id)
    msg.dispatch(msg.Event("command:%s" % w.id, cmd.as_dict()))

    ack = receiver_queue.get(timeout=1)
    print ack.id, ack.message.as_dict()

    info = receiver_queue.get(timeout=1)
    print info.id, info.message.as_dict()

    msg.dispatch(msg.Event("quit"))
    w.terminate()
