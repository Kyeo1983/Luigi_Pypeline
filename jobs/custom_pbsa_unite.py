import csv
import requests
import luigi 
import datetime, os
from luigi.contrib.simulate import RunAnywayTarget
from urllib.request import urlopen as uReq
from bs4 import BeautifulSoup
import re
from math import radians, cos, sin, asin, sqrt
import time
import pandas as pd
from selenium.common.exceptions import NoSuchElementException
import os
import pandas as pd
import numpy as np
import warnings
import json
# warnings.filterwarnings("ignore")
import sys, shutil, subprocess, logging
import logging.config
from datetime import datetime
####################################
# BASE LIBRARIES REQUIRED BY SCRAPER
####################################
import requests
from pathlib import Path


sys.path.append('../utils')
sys.path.append('../configs')
from appconf import conf
from google.cloud import translate_v3beta1 as translate

project_id = conf["project_id"]
workingdir = conf["working_dir"]
ctx = {'sysFolder' : workingdir + '/jobs/jobmarkers/custom_officee_scrape'}
ctx['sysRunFolder'] = workingdir + '/jobs/jobmarkers/custom_officee_scrape/run'
ctx['sysSaveFolder'] = workingdir + '/jobs/jobmarkers/custom_officee_scrape/run/save'
ctx['sysJobName'] = 'custom_officee_scrape'
ctx['sysLogConfig'] = workingdir + '/luigi_central_scheduler/luigi_log.cfg'
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


#########################################################################################
############################### LIST GLOBAL VARIABLES HERE ##############################
#########################################################################################


########################################
# START LUIGI PIPELINE
########################################
class custom_pbsa_unite_start(luigi.Task):
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
            page_soup = BeautifulSoup(page, "html.parser")
        except:
            logger.warning('Connecion Error: {0}'.format(str(sys.exc_info())))
            logger.warning('Number of Attempts: {0}'.format(str(attempts)))
            logger.warning('Sleep for 30 sec')
            attempts += 1
            time.sleep(waittime)
    return page_soup



class custom_pbsa_unite_1(luigi.Task):
    def requires(self):
        return custom_pbsa_unite_start()

    def run(self):
        logger.info('Initiating Scrape for Unite')
        logger.info('Step 1. Scrape city links in UK')
        url = "https://www.unitestudents.com/"

        soup = getSoup(url, 20, 30)
        
        city_list = []

        # get the city list
        city_list = ['https://www.unitestudents.com/' + soup.find_all("select", {"class":"form-base__input"})[0].find_all("option")[i].text.lower() for i in range(len(soup.find_all("select", {"class":"form-base__input"})[0].find_all("option")))][1:]
        logger.info('Step 1. Scrape of City is completed')


        # open a file to write information in
        filename1 = "Building Links.csv"
        f1 = open(filename1, "w", newline='')
        writer1 = csv.writer(f1)
        # columns of attributes to collect
        writer1.writerow(('link', 'property name', 'lon', 'lat', 'postcode', 'address', 'city', 'country'))
        
        logger.info('Step 2. Scrape property listings of each city')
        for url in city_list:
            s = getSoup(url, 20, 30)

            logger.info('-- Scraping into city: ' + url)

            scripts = s.find_all("script") 
            for script in scripts: 
                if "ReactDOM.hydrate" in script.text:
                    param = re.findall(r"ReactDOM.hydrate.*;", script.text)[1]
                    param = param.replace("ReactDOM.hydrate(React.createElement(CityPageFilterMapListings.default, ", "")
                    param = param.replace('), document.getElementById("CityPageFilterMapListings"));', '')
                    param_js = json.loads(param)
                    
            blds = param_js['data']['PropertyData']
            for bld in blds:
                link = 'https://www.unitestudents.com' + bld['PropertyUrl']
                name = bld['PropertyName']
                lon = bld['Long']
                lat = bld['Lat']
                city = bld['PropertyAddress']['City']
                country = bld['PropertyAddress']['Country']
                code = bld['PropertyAddress']['Postcode']
                addr = bld['PropertyAddress']['AddressLine2']
                
                rooms = bld['RoomTypes']
                
                writer1.writerow((link, name, lon, lat, code, addr, city, country))
                logger.info('------ Scrape building link: ' + link)
            logger.info('-- Scrape of city completed: ' + url)

            
        f1.close()
        logger.info('Step 2. Scrape of Buildings is completed')
        return


    def output(self):
        return luigi.LocalTarget(str(ctx['sysFolder']) + '/run/Building Links.csv')





class custom_pbsa_unite_2(luigi.Task):
    def requires(self):
        return custom_pbsa_unite_1()

    def run(self):
        # open a file to write information in
        filename1 = "Room Details.csv"
        f1 = open(filename1, "w", newline='')
        writer1 = csv.writer(f1)
        # columns of attributes to collect
        writer1.writerow(('link', 'property name', 'lon', 'lat', 'postcode', 'address', 'city', 'coutry',
                         'university', 'type group name', 'room type', 'room facilities', 'flatmates', 'room size', 'room class',
                         'tenancy type', 'booking type', 'price per week', 'availability', 'number of total room',
                         'tenancy (nights)', 'tenancy (weeks)', 'extra days', 'total price'))
        
        BldLinks = str(ctx['sysFolder']) + '/run/BuildingLinks.csv'

        blds = pd.read_csv('Building Links.csv') # TO CHANGE
        
        logger.info('Step 3. Scrape room details of each property')
        for index, row in blds.iterrows():
            link = row['link']
            name = row['property name']
            lon = row['lon']
            lat = row['lat']
            city = row['city']
            country = row['country']
            code = row['postcode']
            addr = row['address']
            

            s = getSoup(link, 20, 30)
            logger.info('-- Scraping Room: ' + link)

            scripts = s.find_all("script")
            for script in scripts: 
                if "ReactDOM.hydrate" in script.text:
                    param = re.findall(r"ReactDOM.hydrate.*;", script.text)[3]
                    param = param.replace("ReactDOM.hydrate(React.createElement(PropertyPageFilterListingsMap.default, ", "")
                    param = param.replace(', document.getElementById("PropertyPageFilterListingsMap"));', '')
                    param_js = json.loads(param[:-1]) 


            roomdata = param_js['propertyData']['RoomData']
            for i in range(len(roomdata)):
                type_group = roomdata[i]['TypeGroupName']
                booking_choices = roomdata[i]['RoomTypeAndClasses']

                # details of each room type and class
                for j in range(len(booking_choices)):

                    # param_js['propertyData']['RoomData'][i]['RoomTypeAndClasses'][j][xxx]
                    room_type = booking_choices[j]['RoomType']
                    facilities = booking_choices[j]['RoomFacilities']
                    flatmate = booking_choices[j]['FlatMates']
                    size = booking_choices[j]['RoomSize']
                    room_class = booking_choices[j]['RoomClassName']


                    try:
                        test = booking_choices[j]['TenancyTypes'][0]['AvailabilityBands']
                    except:
                        writer1.writerow((link, name, lon, lat, code, addr, city, country,
                                          uni, type_group, room_type, facilities, str(flatmate), size, room_class,
                                          '', '', '', 'SOLD OUT', '', '', '', '', ''))
                        logger.info('------ Room is sold out: ' + link)

                        continue

                    room_list = booking_choices[j]['TenancyTypes']
                    for tType in room_list:
                        tenancy_name = tType['Name']
                        rooms = tType['AvailabilityBands']                     

                        # extract the info in current AY only
                        for room in rooms:
                            if room['AcademicYear'] =='19/20':
                                bType = room['BookingType']
                                wPrice = room['PricePerWeek']
                                status = room['AvailableRooms']
                                totalR = room['TotalRooms']
                                nights = room['Nights']
                                tPrice = room['TotalPrice']
                                weeks = room['Weeks']
                                uni = room['UniName']
                                extra = room['ExtraDays']

                                writer1.writerow((link, name, lon, lat, code, addr, city, country,
                                                  uni, type_group, room_type, facilities, str(flatmate), size, room_class,
                                                  tenancy_name, bType, wPrice, status, totalR, nights, weeks, extra, tPrice))
                                logger.info('------ Scrape of current room is completed: ' + link)

                    
        f1.close()
        logger.info('Step 3. Scrape of Room Details is completed')


    def output(self):
        return luigi.LocalTarget(str(ctx['sysFolder']) + '/run/Room Details.csv')



class custom_pbsa_unite_end(luigi.Task):
    def requires(self):
        return[custom_pbsa_unite_2()]

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

















 class custom_pbsa_unite_1(luigi.Task):
    def requires(self):
        return custom_pbsa_unite_start()

    def run(self):
        logger.info('Initiating Scrape for Unite')
        logger.info('Step 1: Scrape property listing site of each area')
        url = 'https://officee.jp/area/'
        r = scrape(url, 20, 30)

        # Parse result into HTML using BS
        soup = BeautifulSoup(r.text, "html.parser")

        cityLinks = soup.select("a[href]")
        subCity = cityLinks[39:153]
        subCityLinks = [a["href"] for a in subCity]
        subCityName = [a.text for a in subCity]
        logger.info('Writing to file now')

        ctx['Areadf'] = pd.DataFrame({'Link':subCityLinks, 'City':subCityName})
        ctx['Areadf'].to_csv(self.output().path)
        logger.info('Scrape of Area is completed')
    def output(self):
        return luigi.LocalTarget(str(ctx['sysFolder']) + '/run/Area_Links.csv')










class unite_scrape(luigi.Task):

	def run(self):

	    
		def getSoup(url):
		    uClient = uReq(url)

		    page = uClient.read()
		    uClient.close()
		    page_soup = BeautifulSoup(page, "html.parser")
		    return page_soup
		def get_city_list():
		    url = "https://www.unitestudents.com/"
		    
		    soup = getSoup(url)
		    
		    city_list = []
		    # get the city list
		    # get the city list
		    city_list = ['https://www.unitestudents.com/' + soup.find_all("select", {"class":"form-base__input"})[0].find_all("option")[i].text.lower() for i in range(len(soup.find_all("select", {"class":"form-base__input"})[0].find_all("option")))][1:]
		    
		    return city_list

		def PropertyLinks():
		    city_list = get_city_list()
		    
		    # open a file to write information in
		    filename1 = "BuildingLinks.csv"
		    f1 = open(filename1, "w", newline='')
		    writer1 = csv.writer(f1)
		    # columns of attributes to collect
		    writer1.writerow(('link', 'property name', 'lon', 'lat', 'postcode', 'address', 'city', 'country'))
		    
		    for url in city_list:
		        get_property(url, writer1)
		        
		    f1.close()
		    return

		def get_property(url, writer1):
		    
		    s = getSoup(url)
		    scripts = s.find_all("script") 
		    for script in scripts: 
		        if "ReactDOM.hydrate" in script.text:
		            param = re.findall(r"ReactDOM.hydrate.*;", script.text)[1]
		            param = param.replace("ReactDOM.hydrate(React.createElement(CityPageFilterMapListings.default, ", "")
		            param = param.replace('), document.getElementById("CityPageFilterMapListings"));', '')
		            param_js = json.loads(param)
		            
		    blds = param_js['data']['PropertyData']
		    for bld in blds:
		        link = 'https://www.unitestudents.com' + bld['PropertyUrl']
		        name = bld['PropertyName']
		        lon = bld['Long']
		        lat = bld['Lat']
		        city = bld['PropertyAddress']['City']
		        country = bld['PropertyAddress']['Country']
		        code = bld['PropertyAddress']['Postcode']
		        addr = bld['PropertyAddress']['AddressLine2']
		        
		        rooms = bld['RoomTypes']
		        
		        writer1.writerow((link, name, lon, lat, code, addr, city, country))
		     
		    return

		def RoomDetails():
		    
		    # open a file to write information in
		    filename1 = "RoomDetails.csv"
		    f1 = open(filename1, "w", newline='')
		    writer1 = csv.writer(f1)
		    # columns of attributes to collect
		    writer1.writerow(('link', 'property name', 'lon', 'lat', 'postcode', 'address', 'city', 'coutry',
		                     'university', 'type group name', 'room type', 'room facilities', 'flatmates', 'room size', 'room class',
		                     'tenancy type', 'booking type', 'price per week', 'availability', 'number of total room',
		                     'tenancy (nights)', 'tenancy (weeks)', 'extra days', 'total price'))
		    
		    blds = pd.read_csv('BuildingLinks.csv') # TO CHANGE
		    for index, row in blds.iterrows():
		        link = row['link']
		        name = row['property name']
		        lon = row['lon']
		        lat = row['lat']
		        city = row['city']
		        country = row['country']
		        code = row['postcode']
		        addr = row['address']
		        
		        a = True
		        while a:
		            try:
		                s = getSoup(link)
		                time.sleep(2)
		                a = False
		                scripts = s.find_all("script")
		                for script in scripts: 
		                    if "ReactDOM.hydrate" in script.text:
		                        param = re.findall(r"ReactDOM.hydrate.*;", script.text)[3]
		                        param = param.replace("ReactDOM.hydrate(React.createElement(PropertyPageFilterListingsMap.default, ", "")
		                        param = param.replace(', document.getElementById("PropertyPageFilterListingsMap"));', '')
		                        param_js = json.loads(param[:-1]) 


		                roomdata = param_js['propertyData']['RoomData']
		                for i in range(len(roomdata)):
		                    type_group = roomdata[i]['TypeGroupName']
		                    booking_choices = roomdata[i]['RoomTypeAndClasses']

		                    # details of each room type and class
		                    for j in range(len(booking_choices)):

		                        # param_js['propertyData']['RoomData'][i]['RoomTypeAndClasses'][j][xxx]
		                        room_type = booking_choices[j]['RoomType']
		                        facilities = booking_choices[j]['RoomFacilities']
		                        flatmate = booking_choices[j]['FlatMates']
		                        size = booking_choices[j]['RoomSize']
		                        room_class = booking_choices[j]['RoomClassName']

		        #                 print("param_js['propertyData']['RoomData'][i]['RoomTypeAndClasses'][j]")
		        #                 print('i: ' + str(i))
		        #                 print('j: ' + str(j))
		        #                 print(booking_choices[j]['TenancyTypes'])

		                        try:
		                            # param_js['propertyData']['RoomData'][0]['RoomTypeAndClasses'][0]['TenancyTypes'][0]['AvailabilityBands'] 
		                            test = booking_choices[j]['TenancyTypes'][0]['AvailabilityBands']
		                        except:
		                            writer1.writerow((link, name, lon, lat, code, addr, city, country,
		                                              uni, type_group, room_type, facilities, str(flatmate), size, room_class,
		                                              '', '', '', 'SOLD OUT', '', '', '', '', ''))

		                            continue

		                        room_list = booking_choices[j]['TenancyTypes']
		                        for tType in room_list:
		                            tenancy_name = tType['Name']
		                            rooms = tType['AvailabilityBands']                     

		                            # extract the info in current AY only
		                            for room in rooms:
		                                if room['AcademicYear'] =='19/20':
		                                    bType = room['BookingType']
		                                    wPrice = room['PricePerWeek']
		                                    status = room['AvailableRooms']
		                                    totalR = room['TotalRooms']
		                                    nights = room['Nights']
		                                    tPrice = room['TotalPrice']
		                                    weeks = room['Weeks']
		                                    uni = room['UniName']
		                                    extra = room['ExtraDays']

		                                    writer1.writerow((link, name, lon, lat, code, addr, city, country,
		                                                      uni, type_group, room_type, facilities, str(flatmate), size, room_class,
		                                                      tenancy_name, bType, wPrice, status, totalR, nights, weeks, extra, tPrice))

		                                    print(link)
		                                    print(name)
		                                    print(room_type)
		                                    print(room_class)
		                                    print(wPrice)
		                                    print('*** DONE ***')
		                                    
		            except:
		                a = True
		                print('*** sleeping ***')
		                time.sleep(20)
		                
		    f1.close()
		    print('******* ' + name + ' DONE *******')
		    return
		PropertyLinks()
		RoomDetails()

        


