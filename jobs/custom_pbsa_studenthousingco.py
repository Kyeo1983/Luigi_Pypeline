
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


date = datetime.datetime.now().strftime("%Y-%m-%d")

class studenthousingco_scrape(luigi.Task):

	def run(self):
		def clean(string):
		    string = string.replace("\n", "").replace("\r","").replace("\t", "").replace("  ", "").replace("\\","")
		    return string
		# for 403 website
		def getSoup2(url):
		    a = True
		    while a:
		        try:
		            request = Request(url,headers={
		                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:55.0) Gecko/20100101 Firefox/55.0'})
		            html = uReq(request).read().decode()
		            html_soup = soup(html, "html.parser")
		            time.sleep(2)
		            a = False
		        except:
		            print("sleep")
		            time.sleep(200)
		            a = True
		    return html_soup 
		studenthousingco = getSoup2("https://thestudenthousingcompany.com/")
		property_url = {}
		filename3 = date + "studenthousingco.csv"
		f3 = open(filename3, "w", newline='')
		writer3 = csv.writer(f3)
		# columns of attributes to collect
		writer3.writerow(("property title", "property url", "property type", "price", "tenancy(start)","tenancy(end)" 
		                  "status",'lat', 'lng',  "location", 'features'))
		for city in studenthousingco.findAll('ul', class_="padding-reset"):
		    for prop in city.findAll('li'):
		        if 'https:' not in prop.a['href']:
		            property_url[clean(prop.a.text)] = 'https://thestudenthousingcompany.com' + prop.a['href']
		        else:
		            continue
		for title in property_url.keys():
		#title = 'Bentley House'
		    soup = getSoup2(property_url[title])
		    #print(title)

		    lat = soup.find("div", class_ = "tshc-section-expand__map")['data-mapbox-latt']
		    long = soup.find("div", class_ = "tshc-section-expand__map")['data-mapbox-long']
		    print(lat)
		    print(long)

		    features_list = soup.find('ul', class_ = 'owl-carousel').findAll('li')
		    features = []
		    for feature in features_list:
		        features.append(feature.h4.text)

		    location_ = ''
		    location = soup.find('div', class_ = 'row tshc-building-infos__contact')
		    for loc in location.findAll("div"):
		        if 'Address' in loc.text:
		            location_ = clean(loc.text.replace("Address", ""))
		    #print(location_)

		    property_types = soup.findAll("a", class_ ="tshc-rooms__container__inner")
		    type_list = []
		    for property_type in property_types:
		        type_list.append('https://thestudenthousingcompany.com' + property_type['href'])
		    #print(type_list)
		    
		    for type_url in type_list:
		        #print(type(curr_soup))
		        curr_soup = getSoup2(type_url)
		        room_type = curr_soup.findAll("span", class_ ='field field--name-title field--type-string field--label-hidden')[0].text
		        price = curr_soup.find("div", class_ = 'tshc-booking').div.div.div.h2.text
		        start_date = ''
		        end_date = ''
		        tenancies = curr_soup.find('ul', class_ = 'tshc-booking__infos').findAll('li')
		        for tenancy in tenancies:
		            if 'Tenancy start' in tenancy.text:
		                start_date = tenancy.text.replace('Tenancy start','')
		            elif 'Tenancy ends' in tenancy.text:
		                end_date = tenancy.text.replace('Tenancy ends','')
		        status = 1
		        if 'Sold Out' in curr_soup.text:
		            status = 0
		        #print('status is ' + str(status))

		        writer3.writerow((title, property_url[title], room_type, price, start_date, end_date, status, lat, long, location_,features))
