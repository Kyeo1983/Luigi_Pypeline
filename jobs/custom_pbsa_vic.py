from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
import time
import datetime, os
import csv

date = datetime.datetime.now().strftime("%Y-%m-%d")


class vic_scrape(luigi.Task):


	def run(self):
		def clean(string):
		    string = string.replace("\n", "").replace("\r","").replace("\t", "").replace("  ", "").replace("\\","")
		    return string

		filename2 = date + "vic.csv"
		f2 = open(filename2, "w", newline='')
		writer2 = csv.writer(f2)
		# columns of attributes to collect
		writer2.writerow(("project_url",'url', 'address', 'status', 'title',
		                                 'feature_d', 'duration', 'price', 'coordinates'))

		vicurl ="https://host-students.com/"
		chrome_path = r"C:\Users\caojiahu\Downloads\chromedriver.exe"
		driver = webdriver.Chrome(chrome_path)
		driver.get(vicurl)
		driver2 = webdriver.Chrome(chrome_path)
		driver3 = webdriver.Chrome(chrome_path)
		articles = driver.find_elements_by_tag_name("article")

		project_list = [] # List of all projects
		for i in range(len(articles)):
		#for i in range(1):
		    print(articles[i].text)
		    driver2.get(articles[i].find_element_by_tag_name('a').get_attribute('href'))
		    projects = driver2.find_elements_by_class_name('listed-property__header')
		    for proj in projects:
		        project_list.append(proj.find_element_by_tag_name('a').get_attribute('href'))
		    #print(articles[0].find_element_by_tag_name('a').get_attribute('href'))


		for project_url in project_list:
		    driver2.get(project_url) # property url
		    try:
		        address = driver2.find_element_by_class_name("carosel-infobox__primary").find_element_by_tag_name('p').text
		    except:
		        address = ''
		    #print(address)
		    #print(' ** ')
		    room_types = driver2.find_elements_by_class_name("listed-room__content")
		    room_urls = {}
		    for room_type in room_types:
		        room_url = room_type.find_element_by_tag_name('a').get_attribute('href')
		        room_title = room_type.find_element_by_tag_name('h3').text
		        status = 'available'
		        
		        if 'Sold out' in room_type.text:
		            status = 'sold out'
		        elif 'Limited availability' in room_type.text:
		            status = 'limited availability'
		        room_urls[room_url] = {'status' : status, 'title':room_title}

		    for url in room_urls.keys():
		        driver3.get(url)
		        time.sleep(2)
		        # find features and prices of a room in the project of a city
		        features = []
		        feature_d = {}
		        lis = driver3.find_element_by_class_name('room-details-container').find_elements_by_tag_name('li')
		        for li in lis:
		            features.append(clean(li.text))
		            if li.find_element_by_tag_name('strong').text not in feature_d.keys():
		                feature_d[li.find_element_by_class_name('room-detail-item__label').text] = [li.find_element_by_class_name('room-detail-item__value').text]
		            else:
		                feature_d[li.find_element_by_class_name('room-detail-item__label').text].append(li.find_element_by_class_name('room-detail-item__value').text)
		        #print(features)
		        #print(feature_d)
		        try:
		            prices = driver3.find_element_by_class_name('pricing__option').find_elements_by_tag_name('ul')
		            coordinates = driver3.find_element_by_id('map').find_element_by_tag_name('div').get_attribute('data-centre')
		            for price in prices:
		                try:
		                    duration = price.find_element_by_class_name('pricing-body__smallprint').text
		                    p = price.find_elements_by_tag_name('div')[-1].text
		                    print(duration)
		                    print(p)
		                except:
		                    continue
		                writer2.writerow((project_url,url, address, room_urls[url]['status'], room_urls[url]['title'],
		                                 feature_d, duration, p, coordinates))
		        except:
		            print('not able to scrape: ' + url)
		f2.close()