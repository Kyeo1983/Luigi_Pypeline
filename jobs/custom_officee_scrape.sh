#!/bin/sh
rm -f -R /home/kyeoses/pypeline/jobs/jobmarkers/custom_officee_scrape/run/*
cp -f -R /home/kyeoses/pypeline/jobs/jobmarkers/custom_officee_scrape/run/save /home/kyeoses/pypeline/jobs/jobmarkers/custom_officee_scrape/run/*
python -m luigi --module custom_officee_scrape custom_officee_scrape_end
