"""
    Quick Python logging boilerplate.
    Heavily inspired from https://gist.github.com/tomazursic/0247866ec7f030150c833e9a446efddf
"""

import sys
import os
import logging
import logging.handlers
import tqdm

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
print("Writing log file to {0}".format(DIR_PATH))

def setup_logger(name, log_to_file:bool=False, level=logging.DEBUG, tqdm_support:bool=False):
    formatter = logging.Formatter(fmt="%(asctime)s %(levelname)-8s [%(module)s:%(funcName)s] %(message)s")

    logger = logging.getLogger(name)
    logger.propagate = 0        # Prevent propagation to root logger. Stops duplicate output.
    logger.setLevel(level)

    if (tqdm_support == False):
        handler_stdout = logging.StreamHandler(sys.stdout)
        handler_stdout.setFormatter(formatter)
        logger.addHandler(handler_stdout)
    else:
        handler_tqdm = TqdmHandler()
        handler_tqdm.setFormatter(formatter)
        logger.addHandler(handler_tqdm)

    if log_to_file:
        handler_file = logging.handlers.TimedRotatingFileHandler(
            DIR_PATH + "\\{0}.log".format(name), when="midnight"
        )
        handler_file.setFormatter(formatter)
        logger.addHandler(handler_file)

    return logger

# tqdm handler
# Source: https://stackoverflow.com/questions/14897756/python-progress-bar-through-logging-module
class TqdmHandler(logging.StreamHandler):
    def __init__(self):
        logging.StreamHandler.__init__(self)

    def emit(self, record):
        msg = self.format(record)
        tqdm.tqdm.write(msg)