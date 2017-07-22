# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

from nose.tools import *
import time

import tlscanary.progress as pr


def test_progress_logger_instance():
    """ProgressTracker does its thing"""

    progress = pr.ProgressTracker(100, unit="bubbles")
    assert_true(type(progress) is pr.ProgressTracker, "ProgressTracker can be instantiated")

    assert_true(len(str(progress)) > 0, "can be turned into string (even when empty)")

    # Make progress
    progress.log_completed(1)
    time.sleep(0.01)
    progress.log_completed(9)
    time.sleep(0.01)
    progress.log_completed(10)
    time.sleep(0.01)
    progress.log_completed(30)
    time.sleep(0.01)
    progress.log_completed(0)
    progress.log_overhead(1)

    # Check results
    status = str(progress)
    assert_true("50/100" in status, "reports correct total progress")
    assert_true("50%" in status, "reports correct progress percentage")
    assert_true("2.0% overhead" in status, "reports correct overhead percentage")
    assert_true("bubbles" in status, "uses custom item unit for speed")

    # See if starting and stopping the monitor thread works
    progress.start_reporting(0.1, 0.1)
    thread = progress.logger_thread
    assert_true(thread.is_alive(), "monitor thread can be started")
    progress.stop_reporting()
    time.sleep(1.1)
    assert_false(thread.is_alive(), "monitor thread can be terminated")
