"""
Only has a conf dict object containing configurations of all supported import packages to use in pypeline generated Luigi scripts.
"""
conf = {
    "pandas": {
        "package": "pandas",
        "alias": "pd"
    },
    "numpy": {
        "package": "pandas",
        "alias": "np"
    },
    "helper.database": {
        "package": "helper",
        "importclass": "database"
    },
    "zeep.Client": {
        "package": "zeep.transports",
        "importclass" : "Transport"
    },
    "zeep.transports.Tranport": {
        "package": "zeep.transports",
        "importclass" : "Transport"
    },
    "subprocess": {
        "package": "subprocess"
    },
    "helper.ospath": {
        "package": "helper",
        "importclass": "ospath"
    },
    "subprocess.Popen": {
        "package": "subprocess",
        "importclass": "Popen"
    },
    "time": {
        "package": "time"
    },
    "re" : {
        "package" : "re"
    },
    "zipfile" : {
        "package" : "zipfile"
    },
    "math": {
        "package": "math"
    },
    "requests": {
        "package": "requests"
    },
    "datetime.timedelta": {
        "package": "datetime",
        "importclass" : "timedelta"
    },
    "calendar": {
        "package": "calendar"
    }
}
