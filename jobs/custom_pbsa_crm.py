
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


### CREATE NEW FOLDER BASED ON SCRAPE DATE AND SCRAPE VERSION ###
date = datetime.datetime.now().strftime("%Y-%m-%d")

# Attempts before giving up on a URL due to connection problems
NUM_ATTEMPTS = 8

#### TASK: SCRAPE CRM PROPERITIES ####
class crm_scrape(luigi.Task):

	def run(self):
	    # function 1
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
	                print("sleep")
	                time.sleep(20)
	                a = True
	        return page_soup

	    # function 2
	    def get_city_list():
	        website = "http://www.crm-students.com/"
	        soup = getSoup(website)
	        cities = [i['href'] for i in soup.find('div',{'id':'cities'}).find_all('a')]
	        return cities



	    city = get_city_list()

	    def get_property_city(city_list):
	        properties = []
	        for url in city_list:
	            city = url.split('/')[-2]
	            soup = getSoup(url)
	            #print(city)
	            for card in soup.find_all('div',{'class','card-wrapper'}):
	                property_dict = dict()
	                property_dict['city'] = city
	                property_dict['property_url'] = card.find('a')['href']
	                properties.append(property_dict)
	                print(property_dict)
	        return properties

	    property_dict =get_property_city(city)
	    # export the url list for each property
	    property_list = pd.DataFrame(property_dict)
	    property_list.to_csv( date + '_property list.csv')
	    print('property list obtained')


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


	    filename1 = date + "_CRM (Room Details).csv"
	    f1 = open(filename1, "w", newline='')
	    writer1 = csv.writer(f1)
	    # columns of attributes to collect
	    writer1.writerow(("city", "property url", "coordinate", 
	                      "room url", "start date", "end date","rent per week","room type", "availability", 'features'))

	    for l in property_dict:
	        city = l['city']
	        url = l['property_url']
	        coordinate = find_coord(getSoup(url + '#location'))
	        print('city is ' + city)
	        print('url is ' + url)
	        prop = getSoup(url)
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

	                writer1.writerow((city, url, coordinate, result['room_url'], result['start_date'], result['end_date'],
	                                 result['price'], result['title'], result['availability'],room_size, features))
	        except:
	            writer1.writerow((city, url, coordinate, None,None,None,None,None,None,None, None ))
	    f1.close()

	        