# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import logging
import time
from worq.pool.thread import WorkerPool
from worq import get_broker, get_queue, TaskSpace

import xpcshell_worker as xw


logger = logging.getLogger(__name__)
ts = TaskSpace(__name__)
pool = None


def init(worq_url):
    global ts
    broker = get_broker(worq_url)
    broker.expose(ts)
    return broker


def start_pool(worq_url, num_workers=1, **kw):
    broker = init(worq_url)
    new_pool = WorkerPool(broker, workers=num_workers)
    new_pool.start(**kw)
    return new_pool


def stop():
    global logger, pool
    logger.debug("Stopping worker pool %s" % pool)
    if pool is not None:
        pool.stop()
        pool = None


class ScanResult(object):
    """Class to hold and evaluate scan responses."""

    def __init__(self, response):
        self.response = response
        self.success = self.evaluate_success(response)
        self.host = self.get_host()
        self.rank = self.get_rank()

    def get_host(self):
        return self.response.original_cmd["args"]["host"]

    def get_rank(self):
        return self.response.original_cmd["args"]["rank"]

    @staticmethod
    def evaluate_success(response):
        global logger

        # if .success is true, the result came through the requests
        # `load` handler with state == 4 (fully loaded).
        if response.success:
            return True

        # Else, check whether the error was due to a redirect error, with the first hop being OK.
        uri = response.result["info"]["original_uri"]
        status = response.result["info"]["status"]
        origin = response.result["origin"]
        if origin == "error_handler" and status == 0:  # NS_OK
            logger.debug("Ignored redirect by `%s`" % uri)
            return True

        # Else, the request had some sort of issue
        return False

    def as_dict(self):
        return {
            "response": self.response.as_dict(),
            "success": self.success,
            "host": self.host,
            "rank": self.rank
        }


@ts.task
def scan_urls(app, target_list, profile=None, prefs=None, get_certs=False, timeout=10):
    global logger

    logger.debug("scan_urls task called with %s" % repr(target_list))

    # Spawn a worker instance
    xpcw = xw.XPCShellWorker(app, profile=profile, prefs=prefs)
    xpcw.spawn()

    # Enqueue all host scans for this worker instance
    wakeup_cmd = xw.Command("wakeup")
    cmd_count = 0
    for rank, host in target_list:
        scan_cmd = xw.Command("scan", host=host, rank=rank, include_certificates=get_certs, timeout=timeout)
        xpcw.send(scan_cmd)
        if cmd_count % 10 == 0:
            xpcw.send(wakeup_cmd)
        cmd_count += 1

    # Fetch results from queue, until all results are in or until the last
    # scan must have into timeout. Note that ACKs come in strict sequence of
    # their respective commands.
    results = {}
    timeout_time = time.time() + timeout + 1
    while time.time() < timeout_time:
        for response in xpcw.receive():
            if response.result == "ACK":
                # Reset timeout when scan commands are ACKed.
                if response.original_cmd["mode"] == "scan":
                    timeout_time = time.time() + timeout + 1
                # Ignore other ACKs.
                continue
            # Else we know this is the result of a scan command.
            result = ScanResult(response)
            results[result.host] = result
        if len(results) >= len(target_list):
            break
        if xpcw.send(wakeup_cmd):
            time.sleep(0.1)
        else:
            break

    if len(results) < len(target_list):
        logger.warning("Worker task dropped results, yielded %d instead of %d" % (len(results), len(target_list)))

    # Wind down the worker
    xpcw.send(xw.Command("quit"))
    xpcw.terminate()

    logger.debug("Worker task finished, returning %d results" % len(results))

    return results


@ts.task
def collect(result_dicts):
    combined_results = {}
    for result in result_dicts:
        combined_results.update(result)
    return combined_results


def __as_chunks(flat_list, chunk_size):
    for i in range(0, len(flat_list), chunk_size):
        yield flat_list[i:i + chunk_size]


# CAVE: run_scans is not re-entrant due to use of global variables.
def run_scans(app, target_list, profile=None, prefs=None, num_workers=4, targets_per_worker=50, worq_url="memory://",
              get_certs=False, timeout=10, progress_callback=None):
    global logger, pool

    pool = start_pool(worq_url, timeout=1, num_workers=num_workers)

    chunks = __as_chunks(target_list, targets_per_worker)
    try:
        queue = get_queue(worq_url, target=__name__)

        # Enqueue tasks to be executed in parallel
        scan_results = [queue.scan_urls(app, targets, profile=profile, prefs=prefs,
                                        get_certs=get_certs, timeout=timeout)
                        for targets in chunks]
        result = queue.collect(scan_results)

        queue_len = len(queue)
        logged_len = 0  # Required to correct for "overlogging" due to chunking

        while True:
            finished = result.wait(timeout=10)
            current_queue_len = len(queue)
            chunks_done = queue_len - current_queue_len
            logger.debug("After queue wait: %d old - %d new = %d done" % (queue_len, current_queue_len, chunks_done))
            queue_len = current_queue_len
            # Check finished first to ensure that the final chunk is not logged,
            # because the final chunk might not have the full chunk size.
            if finished:
                break
            if progress_callback is not None and chunks_done > 0:
                # We must assume the maximum chunk size here to calculate the number of results
                progress_callback(chunks_done * targets_per_worker)
                logged_len += chunks_done * targets_per_worker

    except KeyboardInterrupt:
        logger.critical("Ctrl-C received. Winding down workers...")
        stop()
        logger.debug("Signaled workers to quit")
        raise KeyboardInterrupt

    finally:
        stop()

    # Log the results of the final chunk
    if progress_callback is not None:
        actual_len = len(result.value)
        logger.debug("Chunkwise logging reported on %d results, actually received %d" % (logged_len, actual_len))
        len_correction = actual_len - logged_len
        if len_correction != 0:
            logger.debug("Logging correction for %d results" % len_correction)
            progress_callback(len_correction)

    return result.value
