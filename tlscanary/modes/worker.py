# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import datetime
import logging
import pkg_resources as pkgr
import sys

from basemode import BaseMode
import tlscanary.firefox_downloader as fd
import tlscanary.xpcshell_celery_worker as xw

logger = logging.getLogger(__name__)


class CeleryWorkerMode(BaseMode):

    name = "worker"
    help = "Spawn a celery worker"

    @classmethod
    def setup_args(cls, parser):
        """
        Add subparser for the mode's specific arguments.

        This definition serves as default, but modes are free to
        override it.

        :param parser: parent argparser to add to
        :return: None
        """

        # By nature of workdir being undetermined at this point, user-defined test sets in
        # the override directory can not override the default test set. The defaulting logic
        # needs to move behind the argument parser for that to happen.
        release_choice, _, test_default, base_default = fd.FirefoxDownloader.list()

        group = parser.add_argument_group("test candidates selection")
        group.add_argument('-t', '--test',
                           help=("Firefox version to test. It can be one of {%s}, a package file, "
                                 "or a build directory (default: `%s`)") % (",".join(release_choice), test_default),
                           action='store',
                           default=test_default)
        group.add_argument("-b", "--base",
                           help=("Firefox base version to compare against. It can be one of {%s}, a package file, "
                                 "or a build directory (default: `%s`)") % (",".join(release_choice), base_default),
                           action='store',
                           default=base_default)

        group = parser.add_argument_group("profile setup")
        group.add_argument("-o", "--onecrl",
                           help="OneCRL set to test (default: production)",
                           type=str.lower,
                           choices=["production", "stage", "custom"],
                           action="store",
                           default="production")
        group.add_argument("--onecrlpin",
                           help="OneCRL-Tools git commit to use (default: 244e704)",
                           action="store",
                           default="244e704")
        group.add_argument("-p", "--prefs",
                           help="Prefs to apply to all builds",
                           type=str,
                           action="append",
                           default=None)
        group.add_argument("-p1", "--prefs_test",
                           help="Prefs to apply to test build",
                           type=str,
                           action="append",
                           default=None)
        group.add_argument("-p2", "--prefs_base",
                           help="Prefs to apply to base build",
                           type=str,
                           action="append",
                           default=None)

        group = parser.add_argument_group("worker configuration")
        group.add_argument("-n", "--requestsperworker",
                           help="Number of parallel requests per worker (default: 50)",
                           type=int,
                           action="store",
                           default=50)
        group.add_argument("-r", "--broker",
                           help="Celery broker to connect to (default: redis://localhost)",
                           action="store",
                           default="redis://localhost")

    def __init__(self, args, module_dir, tmp_dir):
        global logger

        super(CeleryWorkerMode, self).__init__(args, module_dir, tmp_dir)

        # Define instance attributes for later use
        self.start_time = None
        self.test_app = None
        self.base_app = None
        self.test_metadata = None
        self.base_metadata = None
        self.test_profile = None
        self.base_profile = None
        self.test_worker = None
        self.base_worker = None
        self.celery_app = None

    def setup(self):
        global logger

        # Argument validation logic to make sure user has test build
        if self.args.test is None:
            logger.critical("Must specify test build for regression testing")
            sys.exit(5)
        elif self.args.base is None:
            logger.critical("Must specify base build for regression testing")
            sys.exit(5)

        if self.args.prefs is not None:
            if self.args.prefs_test is not None or self.args.prefs_base is not None:
                logger.warning("Detected both global prefs and individual build prefs.")

        # Get test candidates
        self.test_app = self.get_test_candidate(self.args.test)
        self.base_app = self.get_test_candidate(self.args.base)

        # Extract metadata
        self.test_metadata = self.collect_worker_info(self.test_app)
        self.base_metadata = self.collect_worker_info(self.base_app)

        # Setup custom profiles
        self.test_profile = self.make_profile("test_profile", self.args.onecrl)
        self.base_profile = self.make_profile("base_profile", "production")

        self.test_worker = xw.XPCShellWorker(self.test_app)
        self.base_worker = xw.XPCShellWorker(self.base_app)

        self.test_worker.spawn()
        self.base_worker.spawn()

    def run(self):
        global logger

        logger.info("Worker serving `test` queue with Firefox %s %s" %
                    (self.test_metadata["app_version"], self.test_metadata["branch"]))
        logger.info("Worker serving `base` queue with Firefox %s %s" %
                    (self.base_metadata["app_version"], self.base_metadata["branch"]))

        self.start_time = datetime.datetime.now()

        meta = {
            "tlscanary_version": pkgr.require("tlscanary")[0].version,
            "mode": self.name,
            "args": vars(self.args),
            "argv": sys.argv,
            "test_metadata": self.test_metadata,
            "base_metadata": self.base_metadata,
            "run_start_time": datetime.datetime.utcnow().isoformat()
        }

        try:
            import tlscanary.celery_worker as cw
            cw.start(self.args, self.test_worker)

        except KeyboardInterrupt:
            logger.critical("Ctrl-C received")
            raise KeyboardInterrupt

        meta["run_finish_time"] = datetime.datetime.utcnow().isoformat()

    def teardown(self):
        self.test_worker.terminate()
        self.base_worker.terminate()
