#!/bin/sh
export GOOGLE_APPLICATION_CREDENTIALS=/kyeo/home/pypeline/keys/compute_engine_svc_acct_key_api.json
#rm -f -R /home/kyeoses/pypeline/jobs/jobmarkers/custom_officee_scrape/run/*
#cp -f -R /home/kyeoses/pypeline/jobs/jobmarkers/custom_officee_scrape/run/save /home/kyeoses/pypeline/jobs/jobmarkers/custom_officee_scrape/run/*
python -m luigi --module custom_officee_scrape custom_officee_scrape_end
