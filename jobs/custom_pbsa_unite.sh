#!/bin/sh
export GOOGLE_APPLICATION_CREDENTIALS=/home/kyeoses/pypeline/keys/compute_engine_svc_acct_key_api.json
#rm -f -R /home/kyeoses/pypeline/jobs/jobmarkers/custom_officee_scrape/run/*
#cp -f -R /home/kyeoses/pypeline/jobs/jobmarkers/custom_officee_scrape/run/save /home/kyeoses/pypeline/jobs/jobmarkers/custom_officee_scrape/run/*
python -m luigi --module custom_pbsa_unite custom_pbsa_unite_end