# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import logging
import shutil
import tempfile

import tlscanary.firefox_downloader as fd
import tlscanary.firefox_extractor as fe

# Silence logging
logging.disable(logging.INFO)
logging.disable(logging.WARNING)
logging.disable(logging.ERROR)
logging.disable(logging.CRITICAL)

# Global variables for all tests
# CAVE: Must be accessed as tests.var to get the dynamic results written by setup_package().
#      `from test import var` on the module level always yields the default values, because
#      the import happens before setup is run.
test_app = None
test_archive = None
tmp_dir = None


# This is a bit of a hack to make nosetests "report" that the download is happening.
# This should have been in setup_package, but there it just makes for an awkward silence.
# CAVE: Need to make sure this is always run as the very first test. So far it is likely
# by its placement in the top file that contains tests.
def test_firefox_download_dummy():
    """Downloading firefox package for tests"""
    global test_app, test_archive
    # Get ourselves a Firefox app for the local platform.
    fdl = fd.FirefoxDownloader(tmp_dir)
    # test_archive = fdl.download("nightly", use_cache=True)
    # test_app = fe.extract(test_archive, tmp_dir)
    test_app = fe.extract("/Users/cruetten/.tlscanary/cache/firefox-nightly_osx.dmg", tmp_dir)


def setup_package():
    """Set up shared test fixtures"""
    global tmp_dir
    tmp_dir = tempfile.mkdtemp(prefix="tlscanarytest_")


def teardown_package():
    """Tear down shared test fixtures"""
    global tmp_dir
    if tmp_dir is not None:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        tmp_dir = None


class ArgsMock(object):
    """
    Mock used for testing functionality that
    requires access to an args-style object.
    """
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __getattr__(self, attr):
        try:
            return self.kwargs[attr]
        except KeyError:
            return None
