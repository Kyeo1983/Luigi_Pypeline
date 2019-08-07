#!/bin/sh
rm -f -R /home/kyeoses/pypeline/jobs/jobmarkers/sample_stage/run/*
python -m luigi --module sample_stage sample_stage_end
