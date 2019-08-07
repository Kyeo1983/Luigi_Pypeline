import sqlite3
from pathlib import Path
# Import config file
sys.path.append('../..')
from configs.appconf import conf
import os

class DBHelper:
    """
    Helper for Database operations
    """
    def __init__(self):
        # Get path of db
        scheduler_db_path = conf.scheduler_db_path
        conn = sqlite3.connect(scheduler_db_path)
