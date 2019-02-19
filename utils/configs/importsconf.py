"""
Only has a conf dict object containing configurations of all supported import packages to use in pypeline generated Luigi scripts.
"""
conf = {
    "pandas": {
        "package": "pandas",
        "alias": "pd"
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
    "math": {
        "package": "math"
    },
    "datetime.timedelta": {
        "package": "datetime",
        "importclass" : "timedelta"
    }
}
