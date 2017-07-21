# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import datetime
import logging
import pkg_resources as pkgr
import sys

from basemode import BaseMode
import tlscanary.progress_logger as pr
import tlscanary.runlog as rl
import tlscanary.sources_db as sdb


logger = logging.getLogger(__name__)


class ScanMode(BaseMode):

    name = "scan"
    help = "Collect SSL connection state info on hosts"

    def __init__(self, args, module_dir, tmp_dir):
        global logger

        super(ScanMode, self).__init__(args, module_dir, tmp_dir)

        # Define instance attributes for later use
        self.log = None
        self.start_time = None
        self.test_profile = None
        self.test_app = None
        self.test_metadata = None
        self.sources = None

    def setup(self):
        global logger

        # Argument validation logic to make sure user has specified only test build
        if self.args.test is None:
            logger.critical("Must specify test build for scan")
            sys.exit(5)
        elif self.args.base is not None:
            logger.debug("Ignoring base build parameter")

        # Download app and extract metadata
        self.test_app = self.get_test_candidate(self.args.test)
        self.test_metadata = self.collect_worker_info(self.test_app)
        logger.info("Testing Firefox %s %s scan run" %
                    (self.test_metadata["app_version"], self.test_metadata["branch"]))

        # Create custom profile
        self.test_profile = self.make_profile("test_profile")

        # Compile the set of hosts to test
        db = sdb.SourcesDB(self.args)
        logger.info("Reading `%s` host database" % self.args.source)
        self.sources = db.read(self.args.source)
        logger.info("%d hosts in test set" % len(self.sources))

    def run(self):
        global logger

        logger.info("Testing Firefox %s %s" %
                    (self.test_metadata["app_version"], self.test_metadata["branch"]))

        self.start_time = datetime.datetime.now()

        meta = {
            "tlscanary_version": pkgr.require("tlscanary")[0].version,
            "mode": self.name,
            "args": vars(self.args),
            "argv": sys.argv,
            "test_metadata": self.test_metadata,
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

                logger.info("Starting scan of chunk of %d hosts" % len(host_set_chunk))

                info_uri_set = self.run_test(self.test_app, host_set_chunk, profile=self.test_profile,
                                             prefs=self.args.prefs, get_info=True,
                                             get_certs=True, return_only_errors=False,
                                             report_callback=progress.log_completed)

                for rank, host, result in info_uri_set:
                    log.log(result.as_dict())

        except KeyboardInterrupt:
            logger.critical("Ctrl-C received")
            progress.stop_reporting()
            raise KeyboardInterrupt

        finally:
            progress.stop_reporting()

        meta["run_finish_time"] = datetime.datetime.utcnow().isoformat()
        self.save_profile(self.test_profile, "test_profile", log)
        log.stop(meta=meta)
