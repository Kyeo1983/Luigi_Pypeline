
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
from selenium.common.exceptions import NoSuchElementException

date = datetime.datetime.now().strftime("%Y-%m-%d")

class chapter_scrape(luigi.Task):

	def run(self):

		def clean(string):
		    string = string.replace("\n", "").replace("\r","").replace("\t", "").replace("  ", "").replace("\\","")
		    return string

		filename2 = date + "chapterliving.csv"
		f2 = open(filename2, "w", newline='')
		writer = csv.writer(f2)
		# columns of attributes to collect
		writer.writerow(('url', 'r', 'lon', 'lat', 'features','duration' ,'title', 'name', 'price', 'status'))
		url = 'https://www.chapter-living.com/home'
		chrome_path = r"C:\Users\caojiahu\Downloads\chromedriver.exe"
		driver = webdriver.Chrome(chrome_path)
		driver.get(url)
		driver2 = webdriver.Chrome(chrome_path)

		items = driver.find_element_by_class_name('main').find_elements_by_tag_name('a')
		url_list = []
		for item in items:
		    url_list.append(item.get_attribute('href'))

		    
		for url in url_list:
		    print('scraping ' + url)
		    driver2.get(url)
		    try:
		        lon = driver2.find_element_by_id('map').get_attribute('data-ol-start-lon')
		        lat = driver2.find_element_by_id('map').get_attribute('data-ol-start-lat')
		    
		        sections = driver2.find_element_by_id('features').find_elements_by_class_name('title')
		        features = []
		        for s in sections:
		            #print(s.text)
		            features.append(s.text)
		    except:
		        continue
		    rooms = driver2.find_element_by_class_name('rooms-listing').find_elements_by_tag_name('article')
		    rooms_url = []
		    for room in rooms:
		        rooms_url.append(room.find_element_by_tag_name('a').get_attribute('href'))
		    for r in rooms_url:
		        print('room type ' + r)
		        driver2.get(r)
		        time.sleep(2)
		        #scrape room type and price
		        title = driver2.find_element_by_class_name('display-title').text
		        classes = driver2.find_elements_by_class_name('product')
		        
		        durations = driver2.find_element_by_class_name('heading').find_elements_by_tag_name('h3')
		        for dur in durations:
		            duration = dur.text
		            
		            if '43' in duration:
		                #print('43 week price')
		                for clas in classes:
		                    name = clean(clas.find_element_by_class_name('tip-top-content').text)
		                    try:
		                        price = clean(clas.find_elements_by_tag_name('p')[0].text)
		                    except:
		                        price = None
		                    #print('price is ' + price)
		                    status = clean(clas.find_element_by_class_name('text').text)
		                    writer.writerow((url, r, lon, lat, features,duration, title, name, price, status))
		            
		            elif '51' in duration:
		                #print('51 week price')
		                for clas in classes:
		                    name = clean(clas.find_element_by_class_name('tip-top-content').text)
		                    try:
		                        price = clean(clas.find_element_by_class_name('price-high').text)
		                    except:
		                        price = None
		                    #print('price is ' + price)
		                    status = clean(clas.find_element_by_class_name('text').text)
		                    writer.writerow((url, r, lon, lat, features,duration, title, name, price, status))
		f2.close()