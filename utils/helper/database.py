import os

class DBHelper:
    """
    Helper for Database operations
    """
    def __init__(self):
        connstr = 'Server=;Database=;UID=;PWD='
        driver = 'ODBC Driver 17 for SQL Server'
        if os.name == 'nt' :
            # on windows
            driver = 'SQL Server'
