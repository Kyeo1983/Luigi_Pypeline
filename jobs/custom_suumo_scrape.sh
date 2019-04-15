#!/bin/sh
rm -f -R /home/kyeoses/pypeline/jobs/jobmarkers/my_test_job/run/*
cp -f -R /home/kyeoses/pypeline/jobs/jobmarkers/my_test_job/run/save /home/kyeoses/pypeline/jobs/jobmarkers/custom_suumo_scrape/run/*
python -m luigi --module custom_suumo_scrape custom_suumo_scrape_end
