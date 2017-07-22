# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import logging
import threading
import time

logger = logging.getLogger(__name__)


class ProgressTracker(object):
    """Class that implements progress tracking"""

    def __init__(self, total, unit=None, percent=True, speed=True, eta=True, average=5*60.0):
        """
        Constructor for a ProcessLogger

        The class keeps track of completed and overhead calculations, and
        can estimate current progress speed and time of completion (ETA).

        :param total: int total calculations required for completion
        :param unit: str for unit used for speed
        :param percent: bool whether to show percent
        :param speed: bool whether or not to show speed
        :param eta: bool whether or not to show ETA
        :param average: float length of averaging window
        """
        self.total = total
        self.completed = 0
        self.overhead = 0
        self.unit = "" if unit is None else " %s" % unit
        self.show_percent = percent
        self.show_speed = speed
        self.show_eta = eta
        self.average_window = average
        self.log = []
        self.write_lock = threading.Lock()
        self.start_time = time.time()
        self.logger_thread = None

    def log_completed(self, completed):
        """
        Log a number of completed calculations

        :param completed: Number of completed items
        :return: None
        """
        self.write_lock.acquire()
        try:
            self.log.append((time.time(), completed, 0))
            self.completed += completed
        finally:
            self.write_lock.release()

    def log_overhead(self, overhead):
        """
        Log a number of overhead calculations
        :param overhead:
        :return:
        """
        self.write_lock.acquire()
        try:
            self.log.append((time.time(), 0, overhead))
            self.overhead += overhead
        finally:
            self.write_lock.release()

    def log_window(self, window):
        """
        Return current averaging window

        :param window: float time span for window
        :return: list of (time, int completed, int overhead)
        """
        earliest_time = time.time() - window
        latest_entry = len(self.log)
        earliest_entry = latest_entry
        for i in xrange(latest_entry - 1, -1, -1):
            if self.log[i][0] >= earliest_time:
                earliest_entry = i
            else:
                break
        return self.log[earliest_entry:latest_entry]

    @staticmethod
    def __window_parameters(log_window):
        """
        Calculate timespan and sums of completed and overhead for log window

        :param log_window: list
        :return: float time span, int completed, int overhead
        """
        # Skip first log entry in window, because it logs values from before target timeframe
        earliest, _, _ = log_window[0]
        latest, completed, overhead = reduce(lambda x, y: (y[0], x[1] + y[1], x[2] + y[2]), log_window[1:])
        span = latest - earliest
        return span, completed, overhead

    def __str__(self):
        """
        Return string representation of current progress

        :return: str
        """

        now = time.time()
        overall_time = now - self.start_time

        # Calculate progress and percentages
        net_todo = self.total - self.completed
        gross_factor = 1.0 if self.completed == 0 else (self.completed + self.overhead) / self.completed
        gross_todo = net_todo * gross_factor
        percent = min(100.0 * self.completed / self.total, 100.0)
        overhead_percent = 0.0 if self.completed == 0 else 100.0 * self.overhead / self.completed

        # Get current averaging window
        log_window = self.log_window(self.average_window)

        # Bail out if there is not enough data in the window
        if len(log_window) < 2:
            s = u""
            if self.show_percent:
                s += u"%.0f%% " % percent
            s += u"%d/%d" % (min(self.completed, self.total), self.total)
            s += u", %.0f%% overhead" % overhead_percent
            if self.show_speed:
                s += u", --%s/s net" % self.unit
                s += u", --%s/s gross" % self.unit
            if self.show_eta:
                s += u", ETA --"
            return s

        # Get values for current averaging window
        win_span, win_completed, win_overhead = self.__window_parameters(log_window)

        # Calculate overall and current net and gross speeds, and ETA
        net_speed = float(self.completed) / overall_time
        gross_speed = float(self.completed + self.overhead) / overall_time
        net_win_speed = float(win_completed) / win_span
        gross_win_speed = net_win_speed * gross_factor
        # Speed might have been zero
        if gross_win_speed != 0.0:
            gross_eta = now + gross_todo / gross_win_speed
        else:
            gross_eta = None

        # Build the string according to config
        s = u""
        if self.show_percent:
            s += u"%.0f%% " % percent
        s += u"%d/%d" % (min(self.completed, self.total), self.total)
        s += u", %.1f%% overhead" % overhead_percent
        if self.show_speed:
            match = [
                (0.001, "%s/ms" % self.unit),
                (1.0, "%s/s" % self.unit),
                (60.0, "%s/min" % self.unit),
                (60.0 * 60.0, "%s/h" % self.unit),
                (24.0 * 60.0 * 60.0, "%s/day" % self.unit)
            ]
            scale, unit = match[1]
            for scale, unit in match:
                if gross_speed * scale > 100:
                    break
            s += u", %.0f%s net" % (scale * net_speed, unit)
            s += u", %.0f%s gross" % (scale * gross_speed, unit)
        if self.show_eta:
            if gross_eta is not None:
                s += u", ETA %s" % time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(gross_eta))
            else:
                s += u", ETA --"
        return s

    def start_reporting(self, interval, first_interval=None):
        """
        Spawn logger thread

        :param interval: float seconds between log update
        :param first_interval: optional float seconds before first log update
        :return: None
        """
        global logger

        if self.logger_thread is None:
            logger.debug("Starting progress logger thread")
            self.logger_thread = ProgressLogger(self, first_interval, interval)
            self.logger_thread.setName("Progress")
            self.logger_thread.daemon = True  # Thread dies with worker
            self.logger_thread.start()

    def stop_reporting(self):
        """
        Stop logger thread

        :return: None
        """
        global logger

        if self.logger_thread is not None:
            logger.debug("Terminating progress logger thread")
            try:
                self.logger_thread.quit()
            except AttributeError:
                pass
            finally:
                self.logger_thread = None


class ProgressLogger(threading.Thread):
    """Progress logger thread that logs progress updates"""

    def __init__(self, pr, first_interval, regular_interval):
        """
        Constructor

        :param pr: ProgressTracker instance to monitor
        :param first_interval: float seconds before first log update
        :param regular_interval: float seconds between log update
        """
        super(ProgressLogger, self).__init__()
        self.pr = pr
        self.first_interval = first_interval
        self.regular_interval = regular_interval
        self.updated_at = time.time()
        self.__quit = False

    def __update_time(self):
        """
        Iterator that returns time of next progress update.
        The first update may be different from the rest.

        :return: float iterator
        """
        if self.first_interval is not None:
            yield self.updated_at + self.first_interval
        while True:
            yield self.updated_at + self.regular_interval

    def run(self):
        """
        Start thread

        :return: None
        """
        global logger

        logger.debug("ProgressLogger thread starting")
        update_time = self.__update_time()
        next_update = update_time.next()
        while not self.__quit:
            time.sleep(1)
            now = time.time()
            if now >= next_update:
                logger.info(str(self.pr))
                self.updated_at = now
                next_update = update_time.next()
        logger.debug("ProgressLogger thread exiting")

    def quit(self):
        """
        Signal thread to terminate

        :return: None
        """
        self.__quit = True
