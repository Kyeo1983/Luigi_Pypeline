import requests
from urllib.request import urlopen as uReq
from bs4 import BeautifulSoup as soup
import time
import csv
import re
import pandas as pd
from urllib.request import Request
import luigi 
import datetime, os
from luigi.contrib.simulate import RunAnywayTarget
import random
import logging
import math
import pickle
import sys
from multiprocessing.dummy import Pool as ThreadPool
import json

date = datetime.datetime.now().strftime("%Y-%m-%d")

class fresh_scrape(luigi.Task):
     
    def requires(self):
        return None 
    
    def run(self):
        def getSoup2(url):
        	a = True
        	while a:
        		try:
        			request = Request(url,headers={
		                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:55.0) Gecko/20100101 Firefox/55.0'})
        			html =  uReq(request).read().decode()
        			html_soup = soup(html, "html.parser")
        			
        			
        			return(html_soup)
        			time.sleep(2)
        			a = False
		        except:
		            print("sleep")
		            time.sleep(200)
		            a = True

        def clean(string):
            string = string.replace('\n', '').replace('\r', '').replace('\t', '').replace('  ', '').replace('\\', '')
            return string

        homeurl = 'https://freshstudentliving.co.uk/'

        filename2 = date + "_fresh.csv"
        f2 = open(filename2, "w", newline='')
        writer2 = csv.writer(f2)
        # columns of attributes to collect
        writer2.writerow(("city", "property url", "property type", "price", "tenancy", 
              "status", "location"))

        home_soup = getSoup2(homeurl)

        city_url_dict = {} #key is name of the city and value is url of the city

        for element in home_soup.find_all("ul", class_ = "city-sub-nav__list")[0].find_all("li"):
            #print(element.text)
            # print(element.a['href'])
            city_url_dict[element.text] = element.a['href']


        print('city url dict completed')


        city_prop_dict = {} # key is city and value is list of property urls

        for city in city_url_dict.keys(): # iterate through cities
        #for city in ['Aberdeen']:
            curr_url = city_url_dict[city]
            curr_soup = getSoup2(curr_url)
            prop_url = []
            properties = curr_soup.find("ul", {'id': 'sortlist'}).findAll("li")
            
            for prop in properties:
                # print(prop.a['href'])
                prop_url.append(prop.a['href'])
            city_prop_dict[city] = prop_url

        lis = city_prop_dict.keys() 
        for city in lis:
            prop_list = city_prop_dict[city]
            
            for prop_url in prop_list:
                prop_soup = getSoup2(prop_url + 'rooms/')
                # print(prop_url + ' prop soup obtained')
                prop_loc_soup = getSoup2(prop_url + 'location/')
                loc = prop_loc_soup.find("div", {'id':'gmap'})['data-map']
                types = prop_soup.find("tbody", class_ = "room-row-list").find_all("tr")
                for room_type in types:
                    property_type = room_type.find("h4").strong.text
                    tenancies = room_type.find_all("dt")
                    prices = room_type.find_all('dd')
                    try:
                        status = room_type.find_all('td')[-1].span.text
                    except:
                        status = room_type.find_all('td')[-1].div.div.a.text
                    for i in range(len(prices)):
                        p = prices[i].text
                        tenancy = tenancies[i].text
                        # print("prce is " + p)
                        writer2.writerow((city, prop_url, property_type, p, tenancy, clean(status),loc))
        f2.close()





