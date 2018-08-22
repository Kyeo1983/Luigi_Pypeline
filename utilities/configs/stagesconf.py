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
        "dependencies": []
    }
}
