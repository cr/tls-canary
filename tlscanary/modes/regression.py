# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import datetime
import logging
from math import ceil
import pkg_resources as pkgr
import sys

from basemode import BaseMode
import tlscanary.progress_logger as pr
import tlscanary.runlog as rl
import tlscanary.sources_db as sdb


logger = logging.getLogger(__name__)


class RegressionMode(BaseMode):

    name = "regression"
    help = "Run a TLS regression test on two Firefox versions"

    def __init__(self, args, module_dir, tmp_dir):
        global logger

        super(RegressionMode, self).__init__(args, module_dir, tmp_dir)

        # Define instance attributes for later use
        self.start_time = None
        self.test_app = None
        self.base_app = None
        self.test_metadata = None
        self.base_metadata = None
        self.test_profile = None
        self.base_profile = None
        self.sources = None

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

        self.test_app = self.get_test_candidate(self.args.test)
        self.base_app = self.get_test_candidate(self.args.base)

        self.test_metadata = self.collect_worker_info(self.test_app)
        self.base_metadata = self.collect_worker_info(self.base_app)

        # Setup custom profiles
        self.test_profile = self.make_profile("test_profile", self.args.onecrl)
        self.base_profile = self.make_profile("base_profile", "production")

        # Compile the set of hosts to test
        db = sdb.SourcesDB(self.args)
        logger.info("Reading `%s` host database" % self.args.source)
        self.sources = db.read(self.args.source)
        logger.info("%d hosts in test set" % len(self.sources))

    def run(self):
        global logger

        logger.info("Testing Firefox %s %s against Firefox %s %s" %
                    (self.test_metadata["app_version"], self.test_metadata["branch"],
                     self.base_metadata["app_version"], self.base_metadata["branch"]))

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

        rldb = rl.RunLogDB(self.args)
        log = rldb.new_log()
        log.start(meta=meta)
        progress = pr.ProgressLogger(total=len(self.sources), unit="hosts")
        progress.start_reporting(300, 60)  # First update after 1 minute, then every 5 minutes

        limit = len(self.sources) if self.args.limit is None else self.args.limit

        # Split work into 20 chunks to conserve memory, but make no chunk smaller than 1000 hosts
        next_chunk = self.sources.iter_chunks(chunk_size=limit/20, min_chunk_size=1000)

        try:
            while True:
                host_set_chunk = next_chunk(as_set=True)
                if host_set_chunk is None:
                    break

                logger.info("Starting regression run on chunk of %d hosts" % len(host_set_chunk))

                error_set = self.run_regression_passes(host_set_chunk, report_completed=progress.log_completed,
                                                       report_overhead=progress.log_overhead)
                for rank, host, result in error_set:
                    log.log(result.as_dict())

        except KeyboardInterrupt:
            logger.critical("Ctrl-C received")
            progress.stop_reporting()
            raise KeyboardInterrupt

        finally:
            progress.stop_reporting()

        meta["run_finish_time"] = datetime.datetime.utcnow().isoformat()
        self.save_profile(self.test_profile, "test_profile", log)
        self.save_profile(self.base_profile, "base_profile", log)
        log.stop(meta=meta)

    def run_regression_passes(self, host_set, report_completed=None, report_overhead=None):
        global logger

        # Compile set of error hosts in three passes

        # First pass:
        # - Run full test set against the test candidate
        # - Run new error set against baseline candidate
        # - Filter for errors from test candidate but not baseline

        logger.info("Starting first pass with %d hosts" % len(host_set))

        test_error_set = self.run_test(self.test_app, host_set, profile=self.test_profile,
                                       prefs=self.args.prefs_test, report_callback=report_completed)
        logger.info("First test candidate pass yielded %d error hosts" % len(test_error_set))
        logger.debug("First test candidate pass errors: %s"
                     % ' '.join(["%d,%s" % (r, u) for r, u in test_error_set]))

        base_error_set = self.run_test(self.base_app, test_error_set, profile=self.base_profile,
                                       prefs=self.args.prefs_base, report_callback=report_overhead)
        logger.info("First baseline candidate pass yielded %d error hosts" % len(base_error_set))
        logger.debug("First baseline candidate pass errors: %s"
                     % ' '.join(["%d,%s" % (r, u) for r, u in base_error_set]))

        error_set = test_error_set.difference(base_error_set)

        # Second pass:
        # - Run error set from first pass against the test candidate
        # - Run new error set against baseline candidate, slower with higher timeout
        # - Filter for errors from test candidate but not baseline

        logger.info("Starting second pass with %d hosts" % len(error_set))

        test_error_set = self.run_test(self.test_app, error_set, profile=self.test_profile,
                                       prefs=self.args.prefs_test,
                                       num_workers=int(ceil(self.args.parallel / 1.414)),
                                       n_per_worker=int(ceil(self.args.requestsperworker / 1.414)),
                                       report_callback=report_overhead)
        logger.info("Second test candidate pass yielded %d error hosts" % len(test_error_set))
        logger.debug("Second test candidate pass errors: %s"
                     % ' '.join(["%d,%s" % (r, u) for r, u in test_error_set]))

        base_error_set = self.run_test(self.base_app, test_error_set, profile=self.base_profile,
                                       prefs=self.args.prefs_base, report_callback=report_overhead)
        logger.info("Second baseline candidate pass yielded %d error hosts" % len(base_error_set))
        logger.debug("Second baseline candidate pass errors: %s"
                     % ' '.join(["%d,%s" % (r, u) for r, u in base_error_set]))

        error_set = test_error_set.difference(base_error_set)

        # Third pass:
        # - Run error set from first pass against the test candidate with less workers
        # - Run new error set against baseline candidate with less workers
        # - Filter for errors from test candidate but not baseline

        logger.info("Starting third pass with %d hosts" % len(error_set))

        test_error_set = self.run_test(self.test_app, error_set, profile=self.test_profile,
                                       prefs=self.args.prefs_test, num_workers=2, n_per_worker=10,
                                       report_callback=report_overhead)
        logger.info("Third test candidate pass yielded %d error hosts" % len(test_error_set))
        logger.debug("Third test candidate pass errors: %s"
                     % ' '.join(["%d,%s" % (r, u) for r, u in test_error_set]))

        base_error_set = self.run_test(self.base_app, test_error_set, profile=self.base_profile,
                                       prefs=self.args.prefs_base, num_workers=2, n_per_worker=10,
                                       report_callback=report_overhead)
        logger.info("Third baseline candidate pass yielded %d error hosts" % len(base_error_set))
        logger.debug("Third baseline candidate pass errors: %s"
                     % ' '.join(["%d,%s" % (r, u) for r, u in base_error_set]))

        error_set = test_error_set.difference(base_error_set)
        if len(error_set) > 0:
            logger.warning("%d regressions found: %s"
                           % (len(error_set), ' '.join(["%d,%s" % (r, u) for r, u in error_set])))

        # Fourth pass, information extraction:
        # - Run error set from third pass against the test candidate with less workers
        # - Have workers return extra runtime information, including certificates

        logger.debug("Extracting runtime information from %d hosts" % (len(error_set)))
        final_error_set = self.run_test(self.test_app, error_set, profile=self.test_profile,
                                        prefs=self.args.prefs_test, num_workers=1, n_per_worker=10,
                                        get_info=True, get_certs=True,
                                        return_only_errors=False,  # FIXME: Remove this
                                        report_callback=report_overhead)

        # Final set includes additional result data, so filter that out before comparison
        stripped_final_set = set()

        for rank, host, data in final_error_set:
            stripped_final_set.add((rank, host))

        if stripped_final_set != error_set:
            diff_set = error_set.difference(stripped_final_set)
            logger.warning("Domains dropped out of final error set: %s" % diff_set)

        return final_error_set
