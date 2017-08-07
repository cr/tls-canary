# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

from nose import SkipTest
from nose.tools import *
from time import sleep

import tests
import tlscanary.xpcshell_socket_worker as xw


def test_xpcshell_socket_worker():
    """XPCShell socket worker runs and is responsive"""

    # Skip test if there is no app for this platform
    if tests.test_app is None:
        raise SkipTest("XPCShell worker can not be tested on this platform")

    # Spawn a worker
    w = xw.XPCShellWorker(tests.test_app)
    w.spawn()
    assert_true(w.is_running(), "XPCShell worker starts up fine")
    assert_true(w.helpers_running(), "worker helpers are running")

    # Open connection to worker
    conn = xw.WorkerConnection(w.port, timeout=5)
    assert_true(w.port is not None, "XPCShell worker reports listening on a port")

    # Send info command and check result
    res = conn.ask(xw.Command("info"))
    assert_true(res is not None, "info command can be sent")
    assert_true(type(res) is xw.Response, "worker connection returns Response objects")
    assert_true(res.is_success(), "info command is acknowledged with success")
    assert_equal(res.worker_id, w.id, "worker Python ID matches JS ID")
    assert_true("appConstants" in res.result, "info response contains `appConstants`")
    assert_equal(res.result["appConstants"]["MOZ_UPDATE_CHANNEL"], "nightly",
                 "info response has expected value")

    # Send bogus command and check for negative ACK
    res = conn.ask(xw.Command("bogus"))
    assert_false(res.is_success(), "bogus command is acknowledged with failure")

    # Spawn a second worker
    ww = xw.XPCShellWorker(tests.test_app)
    ww.spawn()
    assert_true(ww.is_running(), "second XPCShell worker starts up fine")
    assert_true(ww.helpers_running(), "second worker's helpers are running")

    # Open connection to second worker
    connn = xw.WorkerConnection(ww.port)

    # Send info command to second worker check result
    res = connn.ask(xw.Command("info"))
    assert_true(res is not None, "info command can be sent to second worker")
    assert_true(res.is_success(), "info command is acknowledged with success by second worker")
    assert_true(res.worker_id != w.id, "response does not come from first worker")
    assert_true("appConstants" in res.result, "info response from second worker contains `appConstants`")

    # Send info command to first worker again and check result
    res = connn.ask(xw.Command("info"))
    assert_true(res is not None, "info command can be sent again")
    assert_true(res.is_success(), "info command is acknowledged with success again")
    assert_true("appConstants" in res.result, "info response contains `appConstants` again")

    # Check whether worker exits cleanly
    assert_true(w.is_running(), "first worker is still alive")
    assert_true(w.helpers_running(), "first worker's helpers are still alive")
    res = w.quit()
    assert_true(res is not None, "quit command can be sent")
    assert_true(res.is_success(), "quit command is acknowledged with success")
    sleep(0.1)
    assert_false(w.is_running(), "first worker terminates after quit command")
    assert_false(w.helpers_running(), "helpers are not persistent")

    # Quit second worker "the harsh way"
    assert_true(ww.is_running(), "second worker is still alive")
    assert_true(ww.helpers_running(), "second worker's helpers are still alive")
    ww.terminate()
    sleep(0.1)
    assert_false(ww.is_running(), "second worker terminates after quit command")
    assert_false(ww.helpers_running(), "second worker's helpers are not persistent")
