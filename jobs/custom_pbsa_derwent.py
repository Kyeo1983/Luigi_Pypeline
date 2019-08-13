
import random
import math
import pickle
import json
from urllib.request import urlopen as uReq
from bs4 import BeautifulSoup as soup
import csv
import re
import luigi, json, os, sys, time, shutil, subprocess, logging
import logging.config
from datetime import datetime
####################################
# BASE LIBRARIES REQUIRED BY SCRAPER
####################################
import requests
from pathlib import Path
import pandas as pd

sys.path.append('../utils')
sys.path.append('../configs')
from appconf import conf
from google.cloud import translate_v3beta1 as translate

project_id = conf["project_id"]
workingdir = conf["working_dir"]
ctx = {'sysFolder' : workingdir + '/jobs/jobmarkers/custom_pbsa_derwent'}
ctx['sysRunFolder'] = workingdir + '/jobs/jobmarkers/custom_pbsa_derwent/run'
ctx['sysSaveFolder'] = workingdir + '/jobs/jobmarkers/custom_pbsa_derwent/run/save'
ctx['sysJobName'] = 'custom_pbsa_derwent'
ctx['sysLogConfig'] = workingdir + '/luigi_central_scheduler/luigi_log.cfg'
logging.config.fileConfig(ctx['sysLogConfig'])
logger = logging.getLogger('luigi-interface')

## EMAIL CONFIG ##


########################################
# START LUIGI PIPELINE
########################################

class custom_pbsa_derwent_start(luigi.Task):
    def run(self):
        ctx['sysStatus'] = 'running'
        ctx['sysTempFolder'] = ctx['sysRunFolder']
        with open(self.output().path, 'w') as out:
            out.write('started successfully')

    def output(self):
        return luigi.LocalTarget(str(ctx['sysFolder']) + '/run/started.mrk')


def getSoup(url, attemptlimit, waittime):
    attempts = 1
    page_soup = None
    while attempts < attemptlimit:
        try:
            uClient = uReq(url)
            page = uClient.read()
            uClient.close()
            page_soup = soup(page, "html.parser")
        except:
            logger.warning('Connecion Error: {0}'.format(str(sys.exc_info())))
            logger.warning('Number of Attempts: {0}'.format(str(attempts)))
            logger.warning('Sleep for 30 sec')
            attempts += 1
            time.sleep(waittime)
    return page_soup

def clean(string):
    string = string.replace("\n", "").replace("\r","").replace("\t", "").replace("  ", "").replace("\\","")
    return string

class custom_pbsa_derwent_1(luigi.Task):
    def requires(self):
        return custom_pbsa_derwent_start()

    def run(self):
        logger.info('Initiating Scrape for Derwent')
        logger.info('Step 1: Initialise output file')
        filename = 'derwent_output.csv'
        f = open(filename, "w", newline='')
        writer = csv.writer(f)
        # columns of attributes to collect

        logger.info('Writing to file now')
        writer.writerow(("city url", "property url", "property address", "latitude", "longitude", "features", "room type", "start date", "rent per week", "total cost","availability"))   

    def output(self):
        return luigi.LocalTarget(str(ctx['sysFolder']) + '/run/derwent_output.csv')


class custom_pbsa_derwent_end(luigi.Task):
    def requires(self):
        return custom_pbsa_derwent_1()
    def run(self):
        # extract list of cities from the home page
        home = getSoup("https://www.derwentstudents.com", 20, 30)
        time.sleep(3)
        sub_menu = home.find("ul", class_ = 'sub-menu').find_all("li")
        city_urls = []
        for element in sub_menu:
            try:
                city_urls.append('https://www.derwentstudents.com' + element.span.a['href'])
            except:
                continue
        logger.info('the number of urls of all cities to scrape is ' + str(len(city_urls)))

        # iterate through city
        for city_url in city_urls:
            logger.info('scraping the following city ' + city_url)
            city_soup = getSoup(city_url, 10, 30)
            listed_properties = city_soup.find_all("div", class_ = 'location-title')
            list_of_property_urls = []
            # iterate through property
            for listed_property in listed_properties:
                list_of_property_urls.append('https://www.derwentstudents.com' + listed_property.a['href'])
            logger.info('the total number of urls of properties to scrape in the city is ' + str(len(list_of_property_urls)))

            for listed_property_url in list_of_property_urls:
                logger.info('scraping the following property ' + listed_property_url)
                property_soup = getSoup(listed_property_url, 10, 30)
                #scrape from a property, derwent to extract the address, lon, lat and list of featuers
                address = clean(property_soup.find("div", class_ = "property-wrap").address.text)
                latitude = re.findall("[-+]?\d+\.?\d*" ,property_soup.find("script", type="text/javascript").text)[0]
                longitude = re.findall("[-+]?\d+\.?\d*" , property_soup.find("script", type="text/javascript").text)[1]
                features_soup = property_soup.find_all("div", class_ = "icon-with-desc")
                # property overview
                feature_list = []
                for feature in features_soup:
                    feature_list.append(clean(feature.text))
        
                rooms_soup = property_soup.find_all("div", class_ = "room-item")
                # extract room type, start date, rent per week, total cost into a list(ordered)
                content = []
                for i in range(len(rooms_soup)):
                    try:
	                    price_soups = rooms_soup[i].find_all("div", class_ = "room-strip")
	                    for price_soup in price_soups:
	                        content.append({'room_type': rooms_soup[i].h3.text,
	                                     'start_date': price_soup.find("div", class_="col one").span.text,
	                                     'rent_per_week': clean(price_soup.find("div", class_="col two").text.replace("Rent per week", "")),
	                                     'total_cost': clean(price_soup.find("div", class_="col three").text.replace("Total cost", "")),
	                                     'availability':clean(price_soup.find("a").text)})     
                    except:
	                    content.append({'room_type': None,
	                                 'start_date': None,
	                                 'rent_per_week': None,
	                                 'availability':None,
	                                 'total_cost': None})

	                    ####                    
                for d in content:
                    writer.writerow((city_url, listed_property_url, address, latitude, longitude, feature_list,
                                    d['room_type'], d['start_date'], d['rent_per_week'], d['total_cost'],d['availability']))
                logger.info('the property ' + listed_property_url + ' is completed')
            logger.info('the city ' + city_url + 'is completed')
        f.close()


    def output(self):
        return luigi.LocalTarget(str(ctx['sysFolder']) + '/run/derwent_output.csv')