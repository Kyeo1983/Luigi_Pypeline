
import luigi 
import datetime, os
from luigi.contrib.simulate import RunAnywayTarget
import random
import logging
import math
import pickle
import sys
import time
from multiprocessing.dummy import Pool as ThreadPool
import requests
import pandas as pd
import json
from urllib.request import urlopen as uReq
from bs4 import BeautifulSoup as soup
import csv
import re
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException


### CREATE NEW FOLDER BASED ON SCRAPE DATE AND SCRAPE VERSION ###
date = datetime.datetime.now().strftime("%Y-%m-%d")

# Attempts before giving up on a URL due to connection problems
NUM_ATTEMPTS = 8

class campus_scrape(luigi.Task):
	def run(self):
	    


		def getSoup(url):
		    uClient = uReq(url)
		    page = uClient.read()
		    uClient.close()
		    page_soup = soup(page, "html.parser")
		    return page_soup

		def BuildingLinks():
		    
		    # open a file to write information in
		    filename1 = "BuildingLinks.csv"
		    f1 = open(filename1, "w", newline='')
		    writer1 = csv.writer(f1)
		    # columns of attributes to collect
		    writer1.writerow(('link',' '))
		    
		    a = True
		    while a:
		        try:
		            s = getSoup('https://www.campuslivingvillages.com/village-locations/#uk')
		            uk = s.find_all('div', class_='block-inner')[17:21]

		            bld = []
		            for i in uk:
		                bld.append(i.find_all('a'))

		            accom = []
		            for j in bld:
		                for k in j:
		                    link = k['href']
		                    accom.append(link)
		                    writer1.writerow((link, ' '))
		            a = False
		            
		        except:
		            print('*** sleeping ***')
		            a = True
		            time.sleep(15)
		    
		    f1.close()
		    return accom



		def findRooms():
		     # open a file to write information in
		    filename1 = "RoomLinks.csv"
		    f1 = open(filename1, "w", newline='')
		    writer1 = csv.writer(f1)
		    # columns of attributes to collect
		    writer1.writerow(('name', 'lon', 'lat', 'address', 'contact', 'room type', 'link'))
		    
		    bld = pd.read_csv('BuildingLinks.csv')['link']
		    
		    ## loop through links of buildings
		    for i in bld:
		        RoomLinks(i, writer1)
		    
		    f1.close()
		    return

		## INPUT: url of a building
		def RoomLinks(i, writer1):
		        
		    a = True
		    while a:
		        try:
		            s = getSoup(i)
		            time.sleep(2)

		            scripts = s.find_all("script") 
		            for script in scripts: 
		                if "var RT_API =" in script.text: 

		                    # extract the dictionary 
		                    try:
		                        param = re.findall(r"var RT_API = .*;", script.text)[0] 
		                        param = param.replace("var RT_API = ", "") 
		                        param_js = json.loads(param[:-1]) 

		                        body = param_js['response']['body']
		                        body_js = json.loads(body)

		                        try:
		                            name = body_js['slug']
		                        except:
		                            name = ''

		                        try:
		                            loc = body_js['acf']['map_data']
		                            lon = loc['location_latitude']
		                            lat = loc['location_longitude']
		                        except:
		                            lon=''
		                            lat=''

		                        try:
		                            info = body_js['acf']['village_contact_information']
		                            addr = info['village_address']
		                            cont = info['village_contact_number']
		                        except:
		                            addr = ''
		                            cont = ''


		                        try:
		                            rooms = body_js['acf']['room_type']
		                            for room in rooms:
		                                rtype = room['title']
		                                link = room['linked_room']
		                                writer1.writerow((name, lon, lat, addr, cont, rtype, link))
		                                
		                                a = False

		                        # different HTML
		                        except:
		                            
		                            try:

		                                # display a list of buildings
		                                box = s.find_all('a', class_='bg--cta btn--no-border btn--rounded-more text-uppercase btn-fluid text--white margin--bottom')
		                                titles = s.find_all('header', class_='header header--village padding--md bg--white')

		                                for n in range(len(box)):

		                                    link = box[n]['href']
		                                    rtype = titles[n].find('h2', class_='heading heading--h2 text--600 text--lg').text

		                                    # href is incomplete
		                                    if 'http' not in link:
		                                        link = s.find('meta', {'name': 'og:url'})['content']+link+'/'
		                                        
		                                    RoomLinks(link, writer1)


		                            except:
		                                a = False

		                    except:
		                        a = False
		                        continue

		            a = False

		        except:
		            print('*** sleeping ***')
		            a = True
		            time.sleep(15)

		    return

		def RoomDetails():
		    chrome_path = r"C:\Users\caojiahu\Downloads\chromedriver.exe"
		    driver = webdriver.Chrome(chrome_path)
		    
		    roomlinks = pd.read_csv('RoomLinks.csv')
		    
		    
		    # open a file to write information in
		    filename1 = "RoomDetails.csv"
		    f1 = open(filename1, "w", newline='')
		    writer1 = csv.writer(f1)
		    # columns of attributes to collect
		    writer1.writerow(('link', 'name', 'lon', 'lat', 'address',
		                      'room type', 'description', 'features', 'price', 'tenancy period',
		                      'check-in date', 'check-out date', 'availability'))
		    
		    for index, row in roomlinks.iterrows():
		        link = row['link']
		        name = row['name']
		        lon = row['lon']
		        lat = row['lat']
		        addr = row['address']
		        rtype = row['room type']
		        
		        driver.get(link)
		        driver.implicitly_wait(2)
		        
		        descr = driver.find_element_by_css_selector('.text--md.text--300.no-margin--bottom').text
		        
		        feat = []
		        features = driver.find_elements_by_css_selector('.text--500.text--sm.no-margin--bottom.feature__list__list-item__description__title')
		        
		        for f in features:
		            feat.append(f.text)
		            
		        rooms = driver.find_elements_by_css_selector('.padding--vertical.col-12.col-sm-6.col-md-6')
		        
		        for room in rooms:
		            price = room.find_element_by_css_selector('.contact-room-lease-rate.text--600.text--md.contract-details-label').text
		            period = room.find_element_by_css_selector('.contact-room-lease-length.text--300.text--md.contract-details-label').text
		            checkin = room.find_element_by_css_selector('.contact-room-checkin-date.text--300.text--sm.contract-details-label').text
		            checkout = room.find_element_by_css_selector('.contact-room-checkout-date.text--300.text--sm.contract-details-label').text
		            
		#             try:
		            avl = room.find_elements_by_class_name("contract-breakdown__table_data")[-1].find_element_by_tag_name('a').text
		#                 avl = room.find_element_by_css_selector('.bg--cta.btn--cta.btn--no-border.btn--block.btn--rounded-more.text-uppercase.text--white.text--center.amber').text
		#             except NoSuchElementException:
		#                 try:
		#                     avl = room.find_element_by_class_name('contract-breakdown__table_data').find_element_by_tag_name('a').text
		#                 except:
		#                     avl = ''
		                    
		            writer1.writerow((link, name, lon, lat, addr,
		                             rtype, descr, feat, price, period, checkin, checkout, avl))
		        
		        
		    f1.close()
		    return

		def clean():
		    details = pd.read_csv('RoomDetails.csv', encoding = "ISO-8859-1")
		    
		    for index, row in details.iterrows():
		        details['features'][index] = eval(details['features'][index])
		#         details['price'][index] = row['price'][1:]
		    
		    details = newColumns(details)
		    details = fillProperty(details)
		    
		    details.to_csv('RoomDetails_cleaned.csv')
		    return
				    
		def findUniqueFeatures(df):
		    output = {}
		    new = ''
		    for index, row in df.iterrows():
		        if type(row['features']) == list:
		            for feature in row['features']:
		                if feature not in output:
		                    output[feature] = 1
		        else:
		            continue
		                    
		    unique = list(output.keys())
		    return unique


		def newColumns(df):
		    uniqueFeatures = findUniqueFeatures(df)
		    for feature in uniqueFeatures:
		        df[feature] = 0
		    return df

		def fillProperty(df):
		    for index, row in df.iterrows():
		        if type(row['features']) == list:
		            for feature in row['features']:
		#                 if ':' in feature:
		#                     s = feature.split(':')

		#     #                  size = re.findall("[-+]?\d+\.?\d*", feature)
		#     #                  nsw.set_value(index, 'building size', size[0])

		#                     key = s[0]
		#                     value = re.findall("[-+]?\d+\.?\d*", s[1])[0]
		#                     df[key][index] = value
		#                 else:
		                df[feature][index] = 1
		        else:
		            continue

		    return df

		BuildingLinks()
		findRooms()
		RoomDetails()
		clean()

	