import sqlite3
from sqlite3 import Error
from pathlib import Path
# Import config file
sys.path.append('../')
from configs.appconf import conf


# Get path of db
scheduler_db_path = conf.scheduler_db_path


# Create DB if it doesn't exist
if not os.path.isfile(scheduler_db_path):
    # When connect to an SQLite database file that does not exist, SQLite creates a new database
    conn = sqlite3.connect(scheduler_db_path)
    # Create main table
    sql_create_table = "CREATE TABLE IF NOT EXISTS TBL_JOBS ( "
        + "id integer PRIMARY KEY, "
        + "name text NOT NULL);"
    c = conn.cursor()
    c.execute(sql_create_table)
