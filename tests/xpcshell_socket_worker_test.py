# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

from nose import SkipTest
from nose.tools import *
import socket
import time

import tests
import tlscanary.xpcshell_socket_worker as xw


foxes = None


def set_fox_trap():
    """Refresh the global fox registry"""
    global foxes
    foxes = []


def kill_stray_foxes():
    """Ensure that there are no stray foxes leaking into the environment"""
    global foxes
    for fox in foxes:
        if fox.worker_process is None:
            # Worker was never spawned
            continue
        try:
            fox.kill()
        except OSError:
            # already killed
            pass


def new_worker(*args, **kwargs):
    """Return worker instance that is registered with the global foxes registry"""
    global foxes
    fox = xw.XPCShellWorker(*args, **kwargs)
    foxes.append(fox)
    return fox


@with_setup(set_fox_trap, kill_stray_foxes)
def test_xpcshell_socket_worker():
    """XPCShell socket worker runs and is responsive"""

    # Skip test if there is no app for this platform
    if tests.test_app is None:
        raise SkipTest("XPCShell worker can not be tested on this platform")

    # Spawn a worker
    worker_one = new_worker(tests.test_app)
    assert_false(worker_one.is_running(), "XPCShell worker must explicitly be started")
    assert_false(worker_one.helpers_running(), "worker helpers don't run unless explicitly started")
    assert_true(worker_one.ask(xw.Command("info"), timeout=1) is None, "stopped worker yields None responses")
    worker_one.spawn()
    assert_true(worker_one.is_running(), "XPCShell worker starts up fine")
    assert_true(worker_one.helpers_running(), "worker helpers are running")

    # Open connection to worker
    conn = worker_one.get_connection(timeout=5)
    assert_true(worker_one.port is not None, "XPCShell worker reports listening on a port")

    # Send info command and check result
    res = conn.ask(xw.Command("info"))
    assert_true(res is not None, "info command can be sent")
    assert_true(type(res) is xw.Response, "worker connection returns Response objects")
    assert_true(res.is_success(), "info command is acknowledged with success")
    assert_equal(res.worker_id, worker_one.id, "worker Python ID matches JS ID")
    assert_true("appConstants" in res.result, "info response contains `appConstants`")
    assert_equal(res.result["appConstants"]["MOZ_UPDATE_CHANNEL"], "nightly",
                 "info response has expected value")

    # Send invalid commands and check for negative ACKs
    res = conn.ask(xw.Command("bogus"))
    assert_false(res.is_success(), "bogus command is acknowledged with failure")
    res = conn.ask("""{"mode":"scan"}""")  # missing mandatory argument
    assert_false(res.is_success(), "incomplete command is acknowledged with failure")
    res = conn.ask("""{"mode":"info",broken}""")  # defective JSON
    assert_false(res.is_success(), "broken command is acknowledged with failure")
    res = conn.ask("")  # empty line
    assert_false(res.is_success(), "empty line command is acknowledged with failure")

    # Spawn a second worker
    worker_two = new_worker(tests.test_app)
    worker_two.spawn()
    assert_true(worker_two.is_running(), "second XPCShell worker starts up fine")
    assert_true(worker_two.helpers_running(), "second worker's helpers are running")

    # Open connection to second worker
    conn_two = xw.WorkerConnection(worker_two.port, timeout=5)
    assert_true(worker_one.id != worker_two.id, "workers have different IDs")
    assert_true(conn.port != conn_two.port, "workers listening on different ports")

    # Send info command to second worker check result
    res = conn_two.ask(xw.Command("info"))
    assert_true(res is not None, "info command can be sent to second worker")
    assert_true(res.is_success(), "info command is acknowledged with success by second worker")
    assert_true(res.worker_id != worker_one.id, "response does not come from first worker")
    assert_true("appConstants" in res.result, "info response from second worker contains `appConstants`")

    # Send info command to first worker again and check result
    res = conn_two.ask(xw.Command("info"))
    assert_true(res is not None, "info command can be sent again")
    assert_true(res.is_success(), "info command is acknowledged with success again")
    assert_true("appConstants" in res.result, "info response contains `appConstants` again")

    # Check whether worker exits cleanly
    assert_true(worker_one.is_running(), "first worker is still alive")
    assert_true(worker_one.helpers_running(), "first worker's helpers are still alive")
    res = worker_one.quit()
    assert_true(res is not None, "quit command can be sent")
    assert_true(res.is_success(), "quit command is acknowledged with success")
    time.sleep(0.05)
    assert_false(worker_one.is_running(), "first worker terminates after quit command")
    assert_false(worker_one.helpers_running(), "helpers are not persisting")

    # Quit second worker "the unfriendly way"
    assert_true(worker_two.is_running(), "second worker is still alive")
    assert_true(worker_two.helpers_running(), "second worker's helpers are still alive")
    worker_two.terminate()
    time.sleep(0.05)
    assert_false(worker_two.is_running(), "second worker terminates")
    assert_false(worker_two.helpers_running(), "second worker's helpers are not persisting")


@with_setup(set_fox_trap, kill_stray_foxes)
def test_xpcshell_socket_worker_load():
    """XPCShell socket worker can take load"""

    # Skip test if there is no app for this platform
    if tests.test_app is None:
        raise SkipTest("XPCShell worker load can not be tested on this platform")

    # Spawn a worker
    worker = new_worker(tests.test_app)
    worker.spawn()
    assert_true(worker.is_running() and worker.helpers_running(), "XPCShell worker running for load test")

    conn = worker.get_connection(timeout=5)
    results = []
    timeout_time = time.time() + 5
    while time.time() < timeout_time:
        results.append(conn.ask(xw.Command("info")))

    had_failed_requests = False
    for result in results:
        if result is None:
            had_failed_requests = True
            break
    assert_false(had_failed_requests, "no failed requests during load test")

    worker.quit()

    print " Gathered %d results in 5 seconds " % len(results),


@with_setup(set_fox_trap, kill_stray_foxes)
def test_xpcshell_socket_worker_timeout():
    """XPCShell socket worker has proper timeout behavior"""

    # Skip test if there is no app for this platform
    if tests.test_app is None:
        raise SkipTest("XPCShell worker timeouts can not be tested on this platform")

    # Spawn a worker
    worker = new_worker(tests.test_app)
    worker.spawn()
    assert_true(worker.is_running() and worker.helpers_running(), "XPCShell worker running for timeout test")

    res = worker.ask(xw.Command("test", sleep=0.05), timeout=0.1)
    assert_true(type(res) is xw.Response, "test command yields Response object")

    with assert_raises(socket.timeout):  # "long response delays trigger timeouts"
        worker.ask(xw.Command("test", sleep=0.05), timeout=0.01)

    assert_true(worker.is_running(), "worker still running after timeout")
    assert_true(worker.ask(xw.Command("info"), timeout=0.1) is not None, "worker still responsive after timeout")

    worker.quit()


@with_setup(set_fox_trap, kill_stray_foxes)
def test_xpcshell_socket_worker_scan_command():
    """XPCShell socket worker can scan hosts"""

    # Skip test if there is no app for this platform
    if tests.test_app is None:
        raise SkipTest("XPCShell worker scans not be tested on this platform")

    # Spawn a worker
    worker = new_worker(tests.test_app)
    worker.spawn()
    assert_true(worker.is_running() and worker.helpers_running(), "XPCShell worker running for scan test")

    # Scans bail on first redirect, thus redirecting hosts come
    # through the error handler and are marked non-successful.
    redirecting_host = "www.mozilla.org"

    # Non-redirecting scans come through the load handler
    # This test will fail once twitter.com changes its redirecting behavior.
    direct_host = "twitter.com"

    res_a = worker.ask(xw.Command("scan", host=redirecting_host, timeout=10), timeout=12)
    res_b = worker.ask(xw.Command("scan", host=redirecting_host, timeout=0.001), timeout=1)
    res_c = worker.ask(xw.Command("scan", host=direct_host, include_certificates=True, timeout=10), timeout=12)

    worker.quit()

    assert_true(type(res_a) is xw.Response, "first scan command yields Response object")
    assert_false(res_a.is_success(), "first command fails as expected due to redirect")
    assert_equal(res_a.result["origin"], "error_handler", "first scan comes through error handler")
    assert_equal(res_a.result["info"]["status"], 0x804B0002, "first scan has NS_BINDING_ABORTED status")
    assert_true(res_a.result["info"]["certificate_chain_length"] == 0, "first response includes no certificates")
    assert_true(res_a.result["info"]["certificate_chain"] is None, "first response has no certificate data")

    assert_true(type(res_b) is xw.Response, "second scan command yields Response object")
    assert_false(res_b.is_success(), "second command fails as expected due to timeout")
    assert_equal(res_b.result["info"]["status"], 0, "second scan has no TSL error")
    assert_equal(res_b.result["origin"], "timeout_handler", "second scan was timeout")

    assert_true(type(res_c) is xw.Response, "third scan command yields Response object")
    assert_false(res_c.is_success(), "third command was successful")
    assert_equal(res_c.result["info"]["status"], 0, "third scan has no TSL error")
    assert_equal(res_c.result["origin"], "load_handler", "third scan is fully loaded")
    assert_true(res_c.result["info"]["certificate_chain_length"] > 0, "third response includes certificates")
    assert_equal(res_c.result["info"]["certificate_chain_length"],
                 len(res_c.result["info"]["certificate_chain"]), "third response has certificate data")
