# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import argparse
import logging
import coloredlogs
import os
import pkg_resources
import shutil
import sys
import tempfile
import threading
import time

import cleanup
import firefox_downloader as fd
import loader
import modes
import sources_db as sdb


# Initialize coloredlogs
logging.Formatter.converter = time.gmtime
logger = logging.getLogger(__name__)
coloredlogs.DEFAULT_LOG_FORMAT = "%(asctime)s %(levelname)s %(threadName)s %(name)s %(message)s"
coloredlogs.install(level="INFO")


def parse_args(argv=None):
    """
    Argument parsing. Parses from sys.argv if argv is None.
    :param argv: argument vector to parse
    :return: parsed arguments
    """
    if argv is None:
        argv = sys.argv[1:]

    pkg_version = pkg_resources.require("tlscanary")[0].version
    home = os.path.expanduser("~")

    # Set up the parent parser with shared arguments
    parser = argparse.ArgumentParser(prog="tlscanary")
    parser.add_argument("--version", action="version", version="%(prog)s " + pkg_version)
    parser.add_argument("-d", "--debug",
                        help="Enable debug",
                        action="store_true")
    parser.add_argument("-w", "--workdir",
                        help="Path to working directory",
                        type=os.path.abspath,
                        action="store",
                        default="%s/.tlscanary" % home)

    # Set up subparsers, one for each mode
    subparsers = parser.add_subparsers(help="Run mode", dest="mode")
    for mode_name in modes.all_modes:
        mode_class = modes.all_modes[mode_name]
        sub_parser = subparsers.add_parser(mode_name, help=mode_class.help)
        mode_class.setup_args(sub_parser)

    return parser.parse_args(argv)


tmp_dir = None
module_dir = None


def __create_tempdir():
    """
    Helper function for creating the temporary directory.
    Writes to the global variable tmp_dir
    :return: Path of temporary directory
    """
    temp_dir = tempfile.mkdtemp(prefix='tlscanary_')
    logger.debug('Created temp dir `%s`' % temp_dir)
    return temp_dir


class RemoveTempDir(cleanup.CleanUp):
    """
    Class definition for cleanup helper responsible
    for deleting the temporary directory prior to exit.
    """
    @staticmethod
    def at_exit():
        global tmp_dir
        if tmp_dir is not None:
            logger.debug('Removing temp dir `%s`' % tmp_dir)
            shutil.rmtree(tmp_dir, ignore_errors=True)


restore_terminal_encoding = None


def get_terminal_encoding():
    """
    Helper function to get current terminal encoding
    """
    global logger
    if sys.platform.startswith("win"):
        logger.debug("Running `chcp` shell command")
        chcp_output = os.popen("chcp").read().strip()
        logger.debug("chcp output: `%s`" % chcp_output)
        if chcp_output.startswith("Active code page:"):
            codepage = chcp_output.split(": ")[1]
            logger.debug("Active codepage is `%s`" % codepage)
            return codepage
        else:
            logger.warning("There was an error detecting the active codepage")
            return None
    else:
        logger.debug("Platform does not require switching terminal encoding")
        return None


def set_terminal_encoding(encoding):
    """
    Helper function to set terminal encoding.
    """
    global logger
    if sys.platform.startswith("win"):
        logger.debug("Running `chcp` shell command, setting codepage to `%s`", encoding)
        chcp_output = os.popen("chcp %s" % encoding).read().strip()
        logger.debug("chcp output: `%s`" % chcp_output)
        if chcp_output == "Active code page: %s" % encoding:
            logger.debug("Successfully set codepage to `%s`" % encoding)
        else:
            logger.warning("Can't set codepage for terminal")


def fix_terminal_encoding():
    """
    Helper function to set terminal to platform-specific UTF encoding
    """
    global restore_terminal_encoding
    restore_terminal_encoding = get_terminal_encoding()
    if restore_terminal_encoding is None:
        return
    if sys.platform.startswith("win"):
        platform_utf_encoding = "65001"
    else:
        platform_utf_encoding = None
    if restore_terminal_encoding != platform_utf_encoding:
        set_terminal_encoding(platform_utf_encoding)


class ResetTerminalEncoding(cleanup.CleanUp):
    """
    Class for restoring original terminal encoding at exit.
    """
    @staticmethod
    def at_exit():
        global restore_terminal_encoding
        if restore_terminal_encoding is not None:
            set_terminal_encoding(restore_terminal_encoding)


# This is the entry point used in setup.py
def main(argv=None):
    global logger, tmp_dir, module_dir

    module_dir = os.path.split(__file__)[0]

    args = parse_args(argv)

    if args.debug:
        coloredlogs.install(level='DEBUG')

    logger.debug("Command arguments: %s" % args)

    cleanup.init()
    fix_terminal_encoding()
    tmp_dir = __create_tempdir()

    # If 'list' is specified as test, list available test sets, builds, and platforms
    if "source" in args and args.source == "list":
        coloredlogs.install(level='ERROR')
        db = sdb.SourcesDB(args)
        build_list, platform_list, _, _ = fd.FirefoxDownloader.list()
        print "Available builds: %s" % ' '.join(build_list)
        print "Available platforms: %s" % ' '.join(platform_list)
        print "Available test sets:"
        for handle in db.list():
            test_set = db.read(handle)
            if handle == db.default:
                default = " (default)"
            else:
                default = ""
            print "  - %s [%d hosts]%s" % (handle, len(test_set), default)
        return 0

    # Create workdir (usually ~/.tlscanary, used for caching etc.)
    # Assumes that no previous code must write to it.
    if not os.path.exists(args.workdir):
        logger.debug('Creating working directory %s' % args.workdir)
        os.makedirs(args.workdir)

    # Load the specified test mode
    try:
        loader.run(args, module_dir, tmp_dir)

    except KeyboardInterrupt:
        logger.critical("\nUser interrupt. Quitting...")
        return 10

    if len(threading.enumerate()) > 1:
        logger.info("Waiting for background threads to finish")
        while len(threading.enumerate()) > 1:
            logger.debug("Remaining threads: %s" % threading.enumerate())
            time.sleep(2)

    return 0
