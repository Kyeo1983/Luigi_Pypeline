import csv
import requests
from urllib.request import urlopen as uReq
from bs4 import BeautifulSoup as soup
import re
from math import radians, cos, sin, asin, sqrt
import time
import pandas as pd
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException
import os
import pandas as pd
import numpy as np
import warnings
import json

warnings.filterwarnings("ignore")



### CREATE NEW FOLDER BASED ON SCRAPE DATE AND SCRAPE VERSION ###
date = datetime.datetime.now().strftime("%Y-%m-%d")

# Attempts before giving up on a URL due to connection problems
NUM_ATTEMPTS = 8

#### TASK: SCRAPE CRM PROPERITIES ####
class collegiate_scrape(luigi.Task):

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
        def CityLinks():
        	filename1 =  "CityLinks.csv"
        	f1 =  open(filename1, "w", newline='')
        	writer1 = csv.writer(f1)
        	writer1.writerow(('link','city'))
        	s =  getSoup('https://www.collegiate-ac.com/uk-student-accommodation/')
        	lst =  s.find('ul', class_='location-list columned cols-3')
        	for city in cities:
        		link =city['href']
        		name =city.text
        		writer1.writerow((link, name))
        	f1.close()
        	return

		CityLinks()
		def BuildingLinks():
		    cities = pd.read_csv('CityLinks.csv')
		    
		    # open a file to write information in
		    filename1 = "BuildingLinks.csv"
		    f1 = open(filename1, "w", newline='')
		    writer1 = csv.writer(f1)
		    # columns of attributes to collect
		    writer1.writerow(('link','building name', 'city', 'address', 'lon', 'lat'))
		    
		    for index, row in cities.iterrows():
		        url = row['link']
		        city = row['city']
		        a = True
		        while a:
		            try:
		                s = getSoup(url)
		                
		                link = s.find('a', {'title': 'View property'})['href']
		                
		                scripts = s.find_all("script") 
		                for script in scripts: 
		                    if "var wp =" in script.text:
		                        param = re.findall(r"var acf = .*;", script.text)[0] 
		                        param = param.replace("var acf = ", "") 
		                        param_js = json.loads(param[:-1]) 

		                        name = param_js['map']['acf-map-6']['places']['map_locations'][0]['location_title']
		                        loc = param_js['map']['acf-map-6']['places']['map_locations'][0]['map_location']
		                        addr = loc['address']
		                        lon = loc['lng']
		                        lat = loc['lat']
		                        
		                writer1.writerow((link, name, city, addr, lon, lat))
		                print(link)
		                
		                a = False
		                
		            except:
		                print('*** sleeping ***')
		                time.sleep(10)
		                a = True
		    
		    f1.close()
		    return
		def RoomDetails():
		    
		    # open a file to write information in
		    filename1 = "RoomDetails.csv"
		    f1 = open(filename1, "w", newline='')
		    writer1 = csv.writer(f1)
		    # columns of attributes to collect
		    writer1.writerow(('room type','tenancy period','start date','end date',
		                      'lease', 'price per week', 'total price', 'reservation fee', 'building facilities'
		                      'link','building name', 'city', 'address', 'lon', 'lat', ''))

		    
		    chrome_path = r"C:\Users\caojiahu\Downloads\chromedriver.exe"
		    driver = webdriver.Chrome(chrome_path)
		    
		    df = pd.read_csv('BuildingLinks.csv')
		    
		    for index, row in df.iterrows():
		        url = row['link']
		        bld = row['building name'] 
		        city = row['city']
		        addr = row['address']
		        lon = row['lon']
		        lat = row['lat']
		        
		        print('curr: ' + url)
		        
		        driver.get(url)
		        driver.implicitly_wait(5)
		        
		        try:
		            driver.find_element_by_id('dropdown-filter').find_element_by_class_name('dk-select ').click()
		        
		#             element = WebDriverWait(driver, 50).until(EC.element_to_be_clickable((By.CLASS_NAME, 'dk-selected ')))
		#             element.click()
		#             print('select')

		            selectAll = driver.find_element_by_class_name('dk-select-options').find_elements_by_class_name('dk-option ')[1]
		            selectAll.click()
		            print('all')
		            
		        except:
		            print('no select button')

		#         except:
		#             print(url)
		#             print('*** no button ***')
		        
		        imgs = driver.find_element_by_css_selector('.og-grid.feature-list').find_elements_by_tag_name('li')
		        feature = []
		        for f in imgs:
		            feature.append(f.find_element_by_tag_name('a').text)
		        
		        
		        lst = driver.find_element_by_css_selector('.cf.pack-list.three-col-flex.grid-container.accom-list').find_elements_by_xpath("//li[@data-mh='pack']")
		        for room in lst:
		            rtype = room.find_element_by_tag_name('h3').text
		            detail = room.find_element_by_class_name('tenancy-length').text
		            period = detail.split('(')[0]
		            start = detail.split('(')[1].split(' - ')[0]
		            end = detail.split(' - ')[1].split(')')[0]
		            lease = detail.split(') ')[1]

		            cost = driver.find_element_by_class_name('cost-list').find_elements_by_tag_name('li')
		            price = cost[0].text.split('\n')[0]
		            total = cost[1].text.split('\n')[0]
		            rsv = cost[2].text.split('\n')[0]

		            writer1.writerow((rtype, period, start, end, 
		                              lease, price, total, rsv, feature, 
		                              url, bld, city, addr, lon, lat))
		            print('*** DONE!! ***')
		            
		    f1.close()
		    return
		CityLinks()
		BuildingLinks()
		RoomDetails()