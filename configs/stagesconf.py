"""
Only has a conf dict object containing configurations of all supported stages for use in pypeline generated Luigi scripts.
"""
conf = {
    "custom" : {
        "path" : "./stages/operations/custom.py",
        "dependencies": []
    },
    "spawn_job_process": {
        "path": "./stages/process/spawn_job_process.py",
        "dependencies": ["pandas", "numpy", "math", "calendar", "time", "datetime.timedelta", "subprocess.Popen", "helper.database"]
    },
    "cmd_line_process": {
        "path": "./stages/process/cmd_line_process.py",
        "dependencies": ["subprocess.Popen"]
    },
    "email": {
        "path": "./stages/operations/email.py",
        "dependencies": []
    }
}
