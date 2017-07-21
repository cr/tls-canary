# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import logging
import sys

import modes


# Eventually import other future tests, like
# pin, performance


logger = logging.getLogger(__name__)


def run(args, module_dir, tmp_dir):
    global logger

    try:
        current_mode = modes.all_modes[args.mode](args, module_dir, tmp_dir)
    except KeyError:
        logger.critical("Unknown run mode `%s`. Choose one of: %s" % (args.mode, ", ".join(modes.all_mode_names)))
        sys.exit(5)

    logger.debug("Running mode .setup()")
    current_mode.setup()
    logger.debug("Running mode .run()")
    current_mode.run()
    logger.debug("Running mode .teardown()")
    current_mode.teardown()
    logger.debug("Mode finished")
