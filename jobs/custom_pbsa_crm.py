from urllib.request import urlopen as uReq
from bs4 import BeautifulSoup as soup
import csv
import re
import luigi
import json
import os
import sys
import time
import shutil
import subprocess
import logging
import logging.config
from datetime import datetime
####################################
# BASE LIBRARIES REQUIRED BY SCRAPER
####################################
import random
import math
import pickle
from multiprocessing.dummy import Pool as ThreadPool
import requests
from fake_useragent import UserAgent
from http_request_randomizer.requests.proxy.requestProxy import RequestProxy
from tqdm import tqdm
sys.path.append('../utils')
from pathlib import Path
import pandas as pd


ctx = {'sysFolder' : '/home/kyeoses/pypeline/jobs/jobmarkers/custom_pbsa_crm'}
ctx['sysRunFolder'] = '/home/kyeoses/pypeline/jobs/jobmarkers/custom_pbsa_crm/run'
ctx['sysSaveFolder'] = '/home/kyeoses/pypeline/jobs/jobmarkers/custom_pbsa_crm/run/save'
ctx['sysJobName'] = 'custom_pbsa_crm'
ctx['sysLogConfig'] = '/home/kyeoses/pypeline/luigi_central_scheduler/luigi_log.cfg'
logging.config.fileConfig(ctx['sysLogConfig'])
logger = logging.getLogger('luigi-interface')



# Config classes should be camel cased
class email(luigi.Config):
    sender = luigi.Parameter(default="Luigi<luigi-noreply@pypeline.com>")
    receiver = luigi.Parameter()
class smtp(luigi.Config):
    password = luigi.Parameter()
    username = luigi.Parameter()
    host = luigi.Parameter()
    port = luigi.Parameter()


# Attempts before giving up on a URL due to connection problems
NUM_ATTEMPTS = 8
def getSoup(url):
    a=True
    count = 0
    page_soup = None
    while a and count < NUM_ATTEMPTS :
        try:
            uClient = uReq(url)
            page = uClient.read()
            uClient.close()
            page_soup = soup(page, "html.parser")
            time.sleep(2)
            a = False
        except:
            count += 1
            logger.warning('Connecion Error: {0}'.format(str(sys.exc_info())))
            logger.warning('Number of Attempts: {0}'.format(str(attempts)))
            logger.warning('Sleep for 30 sec')
            time.sleep(20)
            a = True
    return page_soup

class custom_pbsa_crm_start(luigi.Task):
    def run(self):
        ctx['sysStatus'] = 'running'
        ctx['sysTempFolder'] = ctx['sysRunFolder']
        with open(self.output().path, 'w') as out:
            out.write('started successfully')

    def output(self):
        return luigi.LocalTarget(str(ctx['sysFolder']) + '/run/started.mrk')


#### TASK: SCRAPE CRM PROPERITIES ####
class custom_pbsa_crm_1(luigi.Task):

    def requires(self):
        return custom_officee_scrape_start()

    def run(self):
        logger.info("Initialising scrape for crm")
        def get_city_list():
            website = "http://www.crm-students.com/"
            soup = getSoup(website)
            cities = [i['href'] for i in soup.find('div',{'id':'cities'}).find_all('a')]
            return cities
        logger.info('starting to scrape for a list of url for cities')
        city = get_city_list()
        logger.info('the length of the list of url for cities is ' + str(len(city)) )

        def get_property_city(city_list):
            properties = []
            for url in city_list:
                city = url.split('/')[-2]
                soup = getSoup(url)
                for card in soup.find_all('div',{'class','card-wrapper'}):
                    property_dict = dict()
                    property_dict['city'] = city
                    property_dict['property_url'] = card.find('a')['href']
                    properties.append(property_dict)
            return properties
        logger.info('starting to scrape for a dictionary of properties in each city')
        property_dict = get_property_city(city)
        logger.info('the dictionary of city and property has been obtained')
        # export the url list for each property
        ctx['property_list'] = pd.DataFrame(property_dict)
        ctx['property_list'].to_csv(self.output().path)
        ctx['property_dict'] = property_dict
        logger.info('scrape of property list is completed')
        logger.info('property list successfully exported')
    def output(self):
        return luigi.LocalTarget(str(ctx['sysFolder']) + '/run/property_list.csv')

class custom_pbsa_crm_2(luigi.Task):
	    # intake soup of a property location page such as 
	    # https://www.crm-students.com/student-accommodation/norwich/crown-place-norwich/#location
	    # return [longitude, latitude] (float data type)
        def find_coord(soup):
            lng = float(soup.find('input', {'id': 'mapLocationLng'})['value'])
            lat = float(soup.find('input', {'id': 'mapLocationLat'})['value'])
            return [lng, lat]
        def extract_room1(tag):
            result = {}
            try:
                result['room_url'] = tag['href'] 
            except:
                result['room_url'] = None
            try:
                result['availability'] = tag['data-availability']
            except:
                result['availability'] = None

            try:
                result['end_date'] = tag['data-end-date']
            except:
                result['end_date'] = None
            try:
                result['start_date'] = tag['data-start-date']
            except:
                result['start_date'] = None
            try:
                result['price'] = tag['data-price-per-week']
            except:
                result['price'] = None
            try:
                result['title'] = tag['data-title']
            except:
                result['title'] = None
            return result
        def extract_room2(soup):
            try: 
                room_size = soup.find('article',{'class','col-6'}).find('header').find('p', class_ = 'room-size').text
            except: 
                room_size = None 
            return room_size
        filename = "_CRM (Room Details).csv"
        f = open(filename, "w", newline='')
        writer = csv.writer(f)
        # columns of attributes to collect
        writer.writerow(("city", "property url", "coordinate", "room url", "start date", "end date","rent per week","room type", "availability",'room size', 'features'))
        property_dict = ctx['property_dict']
        for l in property_dict:
            city = l['city']
            url = l['property_url']
            coordinate = find_coord(getSoup(url + '#location'))
            logger.info('the current city is ' + city)
            logger.info('the url of the current city is ' + url)
            prop = getSoup(url)
            logger.info('the soup for the current city has been obtained')
            features = []
            try:
                for i in prop.find_all('div', class_ = 'faq-question'):
                    if 'top' and '?' and '/' and '\n' not in i.text:
                        features.append(i.text) 
            except:
                pass
            try:
                next_url = prop.find('div', class_ = 'property-ctas').a['href']
                next_soup = getSoup(next_url)
                for tag in next_soup.find('aside', class_ = 'col-4 miniCards').findAll('a'):
                    result = extract_room1(tag)
                    room_size = extract_room2(getSoup(result['room_url']))

                    writer.writerow((city, url, coordinate, result['room_url'], result['start_date'], result['end_date'],result['price'], result['title'], result['availability'],room_size, features))
                except:
                    writer.writerow((city, url, coordinate, None,None,None,None,None,None,None, None ))
            f.close()

class custom_pbsa_crm_end(luigi.Task):
    def requires(self):
        return[custom_pbsa_crm_2()]

    def run(self):
        foldername = str(ctx['sysFolder'])
        if os.path.exists(foldername):
            shutil.copytree(foldername, ctx['sysEndFolder'])

        emailconf = email()
        smtpconf = smtp()
        cmd = 'echo "Success" | s-nail -s "Job Success: {}" -r "{}" -S smtp="{}:{}" -S smtp-use-starttls -S smtp-auth=login -S smtp-auth-user="{}" -S smtp-auth-password="{}" -S ssl-verify=ignore {}'.format(ctx['sysJobName'], emailconf.sender, smtpconf.host, smtpconf.port, smtpconf.username, smtpconf.password, emailconf.receiver)
        subprocess.call(cmd, shell=True)


        with open(self.output().path, 'w') as out:
            out.write('ended successfully')

    def output(self):
        global ctx

        #Make directories if not exists
        ctx['sysEndFolder'] = os.path.join(ctx['sysRunFolder'] + '_' + datetime.now().strftime('%Y%m%d%H%M%S'))
        for f in ['sysFolder', 'sysRunFolder']:
            foldername = str(ctx[f])
            if not os.path.exists(foldername):
                os.makedirs(os.path.join(foldername))

        return luigi.LocalTarget(ctx['sysRunFolder'] + '/ended.mrk')
