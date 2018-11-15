# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import asyncio
import os
import pkg_resources as pkgr
import pytest
# import resource
import subprocess
import time

import tlscanary.firefox_app as fa


def __check_app(app):
    assert type(app) is fa.FirefoxApp, "App has right type"
    assert os.path.isdir(app.app_dir), "App dir exists"
    assert os.path.isfile(app.exe) and os.access(app.exe, os.X_OK), "App binary is executable"


def __run_app(app, script, includes=None):
    cmd = [app.exe, '-xpcshell', "-g", app.gredir, "-a", app.browser]
    includes = includes or []
    for include in includes:
        cmd += ["-f", include]
    cmd.append(script)
    # return subprocess.Popen(cmd)
    return subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


async def chat(message: str):
    message = message.rstrip("\n")
    reader, writer = await asyncio.open_connection("127.0.0.1", 27357)
    writer.write(message.encode("utf-8") + b"\n")
    await writer.drain()
    reply = await reader.readline()
    reply = reply.decode("utf-8").rstrip("\n")
    writer.close()
    await writer.wait_closed()
    assert reply == message


async def worker(name: str, queue: asyncio.Queue):
    while True:
        cmd = await queue.get()
        print(f"{name} starting command {cmd}")
        await chat(cmd)
        queue.task_done()
        # FIXME: These two get out of sync for .join() to hang.
        print(queue._unfinished_tasks, queue.qsize())
        print(f"{name} finished command {cmd}")


async def run_load_test_suite(app):
    """Test TCPServer on Firefox Nightly"""
    test_js = pkgr.resource_filename(__name__, "files/tcp_server_test.js")
    server_js = pkgr.resource_filename("tlscanary", "js/tcp_server.js")
    assert os.path.isfile(test_js) and os.path.isfile(server_js), "server test files accessible"
    p = __run_app(app, test_js, [server_js])
    time.sleep(0.2)

    # Increase limits for open files, sockets, etc.
    # soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    # print("limits", soft, hard)
    # if soft < 1000:
    #     resource.setrlimit(resource.RLIMIT_NOFILE, (1000, hard))
    #
    # soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    # print("new limits", soft, hard)

    queue = asyncio.Queue(maxsize=100)

    # FIXME: On Python on Mac OS X queue.join() hangs with more than 47 workers
    # FIXME: Under pytest, the maximum number is even lower (32 and randomly less).
    # FIXME: Problem relaxes when using .run(debug=True).
    # FIXME: Why?
    tasks = []
    for i in range(20):
        # Should call asyncio.create_worker, but that requires Python 3.7+
        tasks.append(asyncio.ensure_future(worker(f"worker-{i}", queue)))

    # FIXME: Python on Mac OS X tends to hang after around 2^14 connections. (ulimit?)
    # Let's verify that the command server can chew through at least 5k commands.
    for i in range(6000):
        await queue.put("test %d" % (i + 1))

    # Wait until queue is processed
    print("before join")
    await queue.join()
    print("after join")

    # Stop workers
    for task in tasks:
        task.cancel()

    # Wait for workers to wind down
    await asyncio.gather(*tasks, return_exceptions=True)

    # Tell server test app to quit
    try:
        await chat("quit 1234")
        p.wait(timeout=2)

    except KeyboardInterrupt as e:
        p.terminate()
        raise e

    finally:
        p.terminate()

    assert p.returncode == 0


def test_load_server_on_nightly(nightly_app):
    """Test TCPServer on Firefox Nightly"""
    asyncio.run(run_load_test_suite(nightly_app), debug=False)


@pytest.mark.slow
def test_load_server_on_beta(beta_app):
    """Test TCPServer on Firefox Beta"""
    asyncio.run(run_load_test_suite(beta_app), debug=False)


@pytest.mark.slow
def test_load_server_on_release(release_app):
    """Test TCPServer on Firefox Release"""
    asyncio.run(run_load_test_suite(release_app), debug=False)


@pytest.mark.slow
def test_load_server_on_esr(esr_app):
    """Test TCPServer on Firefox ESR"""
    asyncio.run(run_load_test_suite(esr_app), debug=False)
