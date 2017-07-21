# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.


from distutils import dir_util
import glob
import logging
import os
import shutil
import stat
import sys
import zipfile

import tlscanary.firefox_app as fa
import tlscanary.firefox_downloader as fd
import tlscanary.firefox_extractor as fe
import tlscanary.one_crl_downloader as one_crl
import tlscanary.sources_db as sdb
import tlscanary.worker_pool as wp
import tlscanary.xpcshell_worker as xw


logger = logging.getLogger(__name__)


class BaseMode(object):
    """
    Generic Test Mode
    Base functionality for all tests
    """

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
        src = sdb.SourcesDB()
        testset_default = src.default
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

        group = parser.add_argument_group("host database selection")
        group.add_argument("-s", "--source",
                           help="Test set to run. Use `list` for info. (default: `%s`)" % testset_default,
                           action="store",
                           default=testset_default)
        group.add_argument("-l", "--limit",
                           help="Limit for number of hosts to test (default: no limit)",
                           type=int,
                           action="store",
                           default=100000)

        group = parser.add_argument_group("worker configuration")
        group.add_argument("-j", "--parallel",
                           help="Number of parallel worker instances (default: 4)",
                           type=int,
                           action="store",
                           default=4)
        group.add_argument("-m", "--timeout",
                           help="Timeout for worker requests (default: 10)",
                           type=float,
                           action="store",
                           default=10)
        group.add_argument("-n", "--requestsperworker",
                           help="Number of requests per worker (default: 50)",
                           type=int,
                           action="store",
                           default=50)
        group.add_argument("-x", "--scans",
                           help="Number of scans per host (default: 3)",
                           type=int,
                           action="store",
                           default=3)

        group = parser.add_argument_group("result processing")
        group.add_argument("-f", "--filter",
                           help="Filter level for results 0:none 1:timeouts (default: 1)",
                           type=int,
                           choices=[0, 1],
                           action="store",
                           default=1)

    def __init__(self, args, module_dir, tmp_dir):
        self.args = args
        self.mode = args.mode
        self.module_dir = module_dir
        self.tmp_dir = tmp_dir

    def get_test_candidate(self, build):
        """
        Download and extract a build candidate. build may either refer
        to a Firefox release identifier, package, or build directory.
        :param build: str with firefox build
        :return: two FirefoxApp objects for test and base candidate
        """
        global logger

        platform = fd.FirefoxDownloader.detect_platform()
        if platform is None:
            logger.error("Unsupported platform: `%s`" % sys.platform)
            sys.exit(5)

        # `build` may refer to a build reference as defined in FirefoxDownloader,
        # a local Firefox package as produced by `mach build`, or a local build tree.
        if build in fd.FirefoxDownloader.build_urls:
            # Download test candidate by Firefox release ID
            logger.info("Downloading Firefox `%s` build for platform `%s`" % (build, platform))
            fdl = fd.FirefoxDownloader(self.args.workdir, cache_timeout=1 * 60 * 60)
            build_archive_file = fdl.download(build, platform)
            if build_archive_file is None:
                sys.exit(-1)
            # Extract candidate archive
            candidate_app = fe.extract(build_archive_file, self.args.workdir, cache_timeout=1 * 60 * 60)
            candidate_app.package_origin = fdl.get_download_url(build, platform)
        elif os.path.isfile(build):
            # Extract firefox build from archive
            logger.info("Using file `%s` as Firefox package" % build)
            candidate_app = fe.extract(build, self.args.workdir, cache_timeout=1 * 60 * 60)
            candidate_app.package_origin = build
            logger.debug("Build candidate executable is `%s`" % candidate_app.exe)
        elif os.path.isfile(os.path.join(build, "mach")):
            logger.info("Using Firefox build tree at `%s`" % build)
            dist_globs = sorted(glob.glob(os.path.join(build, "obj-*", "dist")))
            if len(dist_globs) == 0:
                logger.critical("`%s` looks like a Firefox build directory, but can't find a build in it" % build)
                sys.exit(5)
            logger.debug("Potential globs for dist directory: %s" % dist_globs)
            dist_dir = dist_globs[-1]
            logger.info("Using `%s` as build distribution directory" % dist_dir)
            if "apple-darwin" in dist_dir.split("/")[-2]:
                # There is a special case for OS X dist directories:
                # FirefoxApp expects OS X .dmg packages to contain the .app folder inside
                # another directory. However, that directory isn't there in build trees,
                # thus we need to point to the parent for constructing the app.
                logger.info("Looks like this is an OS X build tree")
                candidate_app = fa.FirefoxApp(os.path.abspath(os.path.dirname(dist_dir)))
                candidate_app.package_origin = os.path.abspath(build)
            else:
                candidate_app = fa.FirefoxApp(os.path.abspath(dist_dir))
                candidate_app.package_origin = os.path.abspath(build)
        else:
            logger.critical("`%s` specifies neither a Firefox release, package file, or build directory" % build)
            logger.critical("Valid Firefox release identifiers are: %s" % ", ".join(fd.FirefoxDownloader.list()[0]))
            sys.exit(5)

        logger.debug("Build candidate executable is `%s`" % candidate_app.exe)
        if candidate_app.platform != platform:
            logger.warning("Platform mismatch detected")
            logger.critical("Running a Firefox binary for `%s` on a `%s` platform will probably fail" %
                            (candidate_app.platform, platform))
        return candidate_app

    @staticmethod
    def collect_worker_info(app):
        worker = xw.XPCShellWorker(app)
        worker.spawn()
        worker.send(xw.Command("info"))
        result = worker.wait().result
        worker.terminate()
        # Add convenience shortcuts and metadata not returned by worker
        result["nss_version"] = "NSS %s" % result["nssInfo"]["NSS_Version"]
        result["nspr_version"] = "NSPR %s" % result["nssInfo"]["NSPR_Version"]
        result["branch"] = result["appConstants"]["MOZ_UPDATE_CHANNEL"]
        result["app_version"] = result["appConstants"]["MOZ_APP_VERSION_DISPLAY"]
        result["application_ini"] = app.application_ini
        result["package_origin"] = app.package_origin
        result["platform"] = app.platform
        return result

    def make_profile(self, profile_name, one_crl_env='production'):
        global logger

        # create directories for profiles
        default_profile_dir = os.path.join(self.module_dir, "default_profile")
        new_profile_dir = os.path.join(self.tmp_dir, profile_name)

        if not os.path.exists(new_profile_dir):
            os.makedirs(new_profile_dir)

        # copy contents of default profile to new profiles
        dir_util.copy_tree(default_profile_dir, new_profile_dir)

        logger.info("Updating OneCRL revocation data")
        if one_crl_env == "production" or one_crl_env == "stage":
            # overwrite revocations file in test profile with live OneCRL entries from requested environment
            revocations_file = one_crl.get_list(one_crl_env, self.args.workdir, self.args.onecrlpin)
            profile_file = os.path.join(new_profile_dir, "revocations.txt")
            logger.debug("Writing OneCRL revocations data to `%s`" % profile_file)
            shutil.copyfile(revocations_file, profile_file)
        else:
            # leave the existing revocations file alone
            logger.info("Testing with custom OneCRL entries from default profile")

        # make all files in profiles read-only to prevent caching
        for root, dirs, files in os.walk(new_profile_dir, topdown=False):
            for name in files:
                os.chmod(os.path.join(root, name), stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)

        return new_profile_dir

    @staticmethod
    def save_profile(profile_path, profile_name, log):
        global logger

        log_part = "%s.zip" % profile_name
        zip_file_path = log.part(log_part)
        logger.debug("Saving `%s` profile from `%s` to `%s`" % (profile_name, profile_path, zip_file_path))

        if not log.is_running:
            logger.critical("Can't save profile to closed log. Please file a bug.")
            return

        with zipfile.ZipFile(zip_file_path, "w", zipfile.ZIP_DEFLATED) as z:
            for root, _, files in os.walk(profile_path):
                for f in files:
                    file_name = os.path.join(root, f)
                    arc_name = os.path.relpath(file_name, profile_path)
                    z.write(file_name, arc_name)

        if "profiles" not in log.meta:
            log.meta["profiles"] = []
        log.meta["profiles"].append({"name": profile_name, "log_part": log_part})

    def run_test(self, app, url_list, profile=None, prefs=None, num_workers=None, n_per_worker=None, timeout=None,
                 get_info=False, get_certs=False, return_only_errors=True, report_callback=None):

        global logger

        # Default to values from args
        if num_workers is None:
            num_workers = self.args.parallel
        if n_per_worker is None:
            n_per_worker = self.args.requestsperworker
        if timeout is None:
            timeout = self.args.timeout

        try:
            results = wp.run_scans(app, list(url_list), profile=profile, prefs=prefs, num_workers=num_workers,
                                   targets_per_worker=n_per_worker, timeout=timeout,
                                   get_certs=get_certs, progress_callback=report_callback)

        except KeyboardInterrupt:
            logger.critical('User abort')
            wp.stop()
            sys.exit(1)

        run_results = set()

        for host in results:
            if return_only_errors:
                if not results[host].success:
                    if get_info:
                        run_results.add((results[host].rank, host, results[host]))
                    else:
                        run_results.add((results[host].rank, host))
            else:
                if get_info:
                    run_results.add((results[host].rank, host, results[host]))
                else:
                    run_results.add((results[host].rank, host))

        return run_results

    def setup(self):
        """
        Runs all the steps required before doing the mode runs.
        Put everything here that takes too long for __init__().
        :return: None
        """
        pass

    def run(self):
        """
        Executes the the steps that constitutes the actual mode run.
        Results are kept internally in the class instance.
        :return: None
        """
        pass

    def teardown(self):
        """
        Clean up steps required after a mode run.
        :return: None
        """
        pass
