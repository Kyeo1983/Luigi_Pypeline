conf = {
    "sample_stage": {
        "path": "./stages/operations/sample_stage.py",
        "dependencies": ["pandas"]
    },
    "long_stage": {
        "path": "./stages/operations/long_stage.py",
        "dependencies": ["subprocess"]
    },
    "spawn_job_process": {
        "path": "./stages/process/spawn_job_process.py",
        "dependencies": ["pandas", "math", "time", "datetime.timedelta", "subprocess.Popen"]
    },
    "cmd_line_process": {
        "path": "./stages/process/cmd_line_process.py",
        "dependencies": ["subprocess.Popen"]
    }
}
