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
warnings.filterwarnings("ignore")

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

        


