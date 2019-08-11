#!/bin/sh
rm -f -R /home/kyeoses/pypeline/jobs/jobmarkers/sample_stage/run/*
python -m luigi --module sample_stage sample_stage_end
chmod 777 /home/kyeoses/pypeline/var/logs/luigi/luigi.log
chmod 777 /home/kyeoses/pypeline/var/logs/luigi/luigi-server.log
chmod 777 /home/kyeoses/pypeline/var/logs/luigi/luigi-stages.log
