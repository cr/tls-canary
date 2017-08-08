import tlscanary.firefox_downloader as fd
import tlscanary.firefox_extractor as fe
import tlscanary.xpcshell_socket_worker as xw
import tempfile
import time
from IPython import embed

import logging
import coloredlogs

# Initialize coloredlogs
logging.Formatter.converter = time.gmtime
logger = logging.getLogger(__name__)
coloredlogs.DEFAULT_LOG_FORMAT = "%(asctime)s %(levelname)s %(threadName)s %(name)s %(message)s"
coloredlogs.install(level="DEBUG")


workdir = tempfile.mkdtemp(prefix="tlscanarytest_")


def download_firefox_app(build):
    platform = fd.FirefoxDownloader.detect_platform()
    fdl = fd.FirefoxDownloader(workdir, cache_timeout=60*60)
    build_archive_file = fdl.download(build, platform)
    if build_archive_file is None:
        shutil.rmtree(workdir, ignore_errors=True)
        return None
    # Extract candidate archive
    candidate_app = fe.extract(build_archive_file, workdir, cache_timeout=60*60)
    candidate_app.package_origin = fdl.get_download_url(build, platform)
    return candidate_app


# app = download_firefox_app("nightly")
app = fe.extract("/Users/cruetten/.tlscanary/cache/firefox-nightly_osx.dmg", workdir)
x = xw.XPCShellWorker(app)

embed()
