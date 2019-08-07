

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

filename1 = date + "_derwent.csv"
f1 = open(filename1, "w", newline='')
writer1 = csv.writer(f1)

# columns of attributes to collect
writer1.writerow(("city url", "property url", "property address", "latitude", "longitude", 
	"features", "room type", "start date", "rent per week", "total cost","availability"))    


class derwent_scrape(luigi.Task):
	def run(self):
		
		# function 1
		def getSoup(url):
		    a=True
		    while a:
		        try:
		            uClient = uReq(url)
		            page = uClient.read()
		            uClient.close()
		            page_soup = soup(page, "html.parser")
		            a = False
		        except:
		            print("sleep")
		            time.sleep(20)
		            a = True
		    return page_soup
		    
		def clean(string):
		    string = string.replace("\n", "").replace("\r","").replace("\t", "").replace("  ", "").replace("\\","")
		    return string
		def derwent(writer):

		    # extract list of cities from the home page
		    home = getSoup("https://www.derwentstudents.com")
		    time.sleep(3)
		    sub_menu = home.find("ul", class_ = 'sub-menu').find_all("li")
		    city_urls = []
		    for element in sub_menu:
		        try:
		            city_urls.append('https://www.derwentstudents.com' + element.span.a['href'])
		        except:
		            continue
		    print(city_urls)
		    # iterate through city
		    for city_url in city_urls:
		        city_soup = getSoup(city_url)
		        listed_properties = city_soup.find_all("div", class_ = 'location-title')
		        list_of_property_urls = []
		        # iterate through property
		        for listed_property in listed_properties:
		            list_of_property_urls.append('https://www.derwentstudents.com' + listed_property.a['href'])
		        print(list_of_property_urls)
		        for listed_property_url in list_of_property_urls:
		            property_soup = getSoup(listed_property_url)
		            #scrape from a property, derwent to extract the address, lon, lat and list of featuers
		            address = clean(property_soup.find("div", class_ = "property-wrap").address.text)
		            latitude = re.findall("[-+]?\d+\.?\d*" ,property_soup.find("script", type="text/javascript").text)[0]
		            longitude = re.findall("[-+]?\d+\.?\d*" , property_soup.find("script", type="text/javascript").text)[1]
		            features_soup = property_soup.find_all("div", class_ = "icon-with-desc")
		            # property overview
		            feature_list = []
		            for feature in features_soup:
		                feature_list.append(clean(feature.text))
		            print(address)
		            print(latitude)
		            print(longitude)
		            print(feature_list)
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
		derwent(writer1)
		f1.close()