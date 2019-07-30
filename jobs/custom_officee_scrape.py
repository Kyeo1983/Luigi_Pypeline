import luigi, json, os, sys, time, shutil, subprocess, logging
import logging.config
from datetime import datetime
####################################
# BASE LIBRARIES REQUIRED BY SCRAPER
####################################
import requests
from pathlib import Path
import pandas as pd
from bs4 import BeautifulSoup
sys.path.append('../utils')
from google.cloud import translate_v3beta1 as translate


ctx = {'sysFolder' : '/home/kyeoses/pypeline/jobs/jobmarkers/custom_officee_scrape'}
ctx['sysRunFolder'] = '/home/kyeoses/pypeline/jobs/jobmarkers/custom_officee_scrape/run'
ctx['sysSaveFolder'] = '/home/kyeoses/pypeline/jobs/jobmarkers/custom_officee_scrape/run/save'
ctx['sysJobName'] = 'custom_officee_scrape'
ctx['sysLogConfig'] = '/home/kyeoses/pypeline/luigi_central_scheduler/luigi_log.cfg'
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
class custom_officee_scrape_start(luigi.Task):
    def run(self):
        ctx['sysStatus'] = 'running'
        ctx['sysTempFolder'] = ctx['sysRunFolder']
        with open(self.output().path, 'w') as out:
            out.write('started successfully')

    def output(self):
        return luigi.LocalTarget(str(ctx['sysFolder']) + '/run/started.mrk')



def scrape(url, attemptlimit, waittime):
    attempts = 1
    while attempts < attemptlimit:
        try:
            r = requests.get(url)
            return r
        except:
            logger.warning('Connecion Error: {0}'.format(str(sys.exc_info())))
            logger.warning('Number of Attempts: {0}'.format(str(attempts)))
            logger.warning('Sleep for 30 sec')
            attempts += 1
            time.sleep(waittime)
    return None


class custom_officee_scrape_1(luigi.Task):
    def requires(self):
        return custom_officee_scrape_start()

    def run(self):
        logger.info('Initiating Scrape for Officee')
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



class custom_officee_scrape_2(luigi.Task):
    def requires(self):
        return custom_officee_scrape_1()

    def run(self):
        df = ctx['Areadf']
        logger.info('# of Areas to scrape is {0}'.format(len(df['Link'])))

        # Concat the Links
        df['Link'] = "https://officee.jp" + df['Link']
        cityLinks = df['Link']

        for x in range(len(cityLinks)):
            logger.info('Scraping area: {0}'.format(cityLinks[x]))
            # Scrape first page of each area link
            r = scrape(cityLinks[x], 20, 30)

            # Successfully requested URL, parse them now
            soup = BeautifulSoup(r.text, 'html.parser')

            # Get the number of pages for the Area
            paging = soup.find('div', class_="pageArea").find_all('a', href=True)

            # Area name
            areaName = soup.find('div', id='MainArea').find('h1').find('span').text
            logger.info('There are {0} number of pages to be scrapped for {1}'.format(str(len(paging)), areaName))

            # Area Links
            areaLink = cityLinks[x]
            logger.info('There are {0} pages to scrape for {1}'.format(str(len(paging)), areaLink))
            for y in range(len(paging)):
                link = "https://officee.jp" + paging[y]['href']

                # Scrape first page of each area link
                r = scrape(link, 20, 30)

                soup = BeautifulSoup(r.text, "html.parser")

                # Get All Property Box
                PropertyBox = soup.find('div', id="PropertyArea").find_all('div', class_='property')

                # Get Property Link
                PropertyLink = [x.find('dl').find('h3').find('a')['href'] for x in PropertyBox]

                # Get Property Names
                PropertyName = [x.find('dl').find('h3').text for x in PropertyBox]

                # Put into a DataFrame
                smallPicture = pd.DataFrame({
                    "Area Name": areaName,
                    "areaLink": areaLink,
                    "Property Link": PropertyLink,
                    "Property Name": PropertyName
                    })

                # Append them together
                if (x == 0 and y == 0):
                    bigPicture = smallPicture
                else:
                    bigPicture = bigPicture.append(smallPicture, ignore_index=True)

                logger.info('Page Number {0} out of {1} is Completed'.format(str(y+1), str(len(paging))))
                time.sleep(1)

        # Save them into Excel
        # CSV cannot recognize Japanese Words whereas Excel can
        ctx['office_links_df'] = bigPicture
        bigPicture.to_csv(self.output().path)
        logger.info('Scrape at Office Level is completed')
    def output(self):
        return luigi.LocalTarget(str(ctx['sysFolder']) + '/run/Office Links.csv')



class custom_officee_scrape_3(luigi.Task):
    def requires(self):
        return custom_officee_scrape_2()

    def run(self):
        logger.info('Initialise STEP 4: Scrape Rental Links of Offices')
        df = ctx['office_links_df']

        OfficeLinks = 'https://officee.jp' + df['Property Link']
        logger.info('# of links to scrape is {0}'.format(str(len(OfficeLinks))))

        for x in range(len(OfficeLinks)):
            r = scrape(OfficeLinks[x], 20, 30)
            soup = BeautifulSoup(r.text, "html.parser")

            # Property Name
            PropertyName = df.loc[x, "Property Name"]

            # lat/lng
            try:
                text = soup.find_all('script', attrs={"type":"text/javascript"})
                for y in text:
                    if (y.text.find('latlng') != -1):
                        item = y.text

                LatStart = item.find("(")
                LatEnd = item.find(")")
                LatMiddle = item[LatStart:LatEnd].find(",") + LatStart
                Lat = item[LatStart+1:LatMiddle]
                Lng = item[LatMiddle+1:LatEnd]
            except:
                Lat = 'NA'
                Lng = 'NA'

            # Get Rental links
            try:
                RentalLinks = [a['href'] for a in soup.find('table', class_='propertyTable').find_all('a')]
            except:
                logger.error('There is error at rental table')
                time.sleep(30)
                continue

            # Number Tracker
            Number = x
            SmallPicture = pd.DataFrame({
                "Office Name": PropertyName,
                "Rental Link": RentalLinks,
                "Office Link": OfficeLinks[x],
                "Latitude": Lat,
                "Longtitude": Lng,
                "Number": Number
                })

            logger.info('Links Index: {0} out of {1} is completed'.format(str(x), str(len(OfficeLinks))))
            if (x == 0):
                BigPicture = SmallPicture
            else:
                BigPicture = BigPicture.append(SmallPicture, ignore_index=True)

        ctx['rental_links_df'] = BigPicture
        BigPicture.to_csv(self.output().path)
        logger.info('Rental Links Scrape is Completed')
    def output(self):
        return luigi.LocalTarget(str(ctx['sysFolder']) + '/run/Rental Links.csv')



class custom_officee_scrape_4(luigi.Task):
    def requires(self):
        return custom_officee_scrape_3()

    def run(self):
        logger.info('Initialise STEP 4: Scrape Rental Details for Rental Links')
        df = ctx['rental_links_df']

        RentalLinks = "https://officee.jp" + df.loc[:, "Rental Link"]
        logger.info('There are {0} links to be scrapped'.format(str(len(RentalLinks))))
        for x in range(len(RentalLinks)):
            r = scrape(RentalLinks[x], 20, 30)
            soup = BeautifulSoup(r.text, "html.parser")

            # Scrape Date and Time
            now = datetime.now()
            date = now.strftime('%d/%m/%Y %H:%M')

            # Property Name
            OfficeName = df.loc[x, "Office Name"]
            # An error here means that we have exceeded the rate limit lol

            # Get Lat Lng Details
            Lat = df.loc[x, "Latitude"]
            Lng = df.loc[x, "Longtitude"]

            # Rental Conditions
            rentalTest = 0
            rentalTable = pd.DataFrame()
            try:
                rentalConditions = soup.find_all('div', attrs={"class": "detailBox mb10"})[0].find_all('dl')
                header = [dt.find('dt').text for dt in rentalConditions]
                values = [dt.find('dd').text for dt in rentalConditions]
                rentalTable = pd.DataFrame(values).T
                rentalTable.columns = header
                rentalTest = 1
            except:
                rentalTest = 0


            # Property Descriptions
            propTest = 0
            propTable1 = pd.DataFrame()
            try:
                propDesc = soup.find_all('div', attrs={"class": "detailBox mb10"})[1].find_all('dl')
                header = [dt.find('dt').text for dt in propDesc]
                values = [dt.find('dd').text for dt in propDesc]
                propTable1 = pd.DataFrame(values).T
                propTable1.columns = header
                propTest = 1
            except:
                propTest = 0

            # Facility Details
            facilTest = 0
            try:
                facilDetails = soup.find_all('div', attrs={"class":"detailBox mb5"})[0].find_all('dl')
                header = [dt.find('dt').text for dt in facilDetails]
                values = [dt.find('dd').text for dt in facilDetails]
                facilTable = pd.DataFrame(values).T
                facilTable.columns = header
                facilTest = 1
            except:
                facilTest = 0


            # Neighboring Area/station
            negTest = 0
            values = []
            values1= []
            try:
                negAreaStation = soup.find('ul', class_="tagLinkList").find_all('li')
                values = [z.text for z in negAreaStation]
                values1 = values1
                values1 = ", ".join(values1)
                negTest = 1
            except:
                negTest = 0
                values = 'NA'
                values1 = 'NA'

            # Similar Properties
            # Need to convert and clean them (refer to JunRay codes)
            simPropTest = 0
            try:
                simProp = soup.find('div', class_='boxPropertySlider').find_all('li')
                simPropName = [x.find('div', class_='name').text for x in simProp]
                simPropAdd = [x.find('div', class_='address').text for x in simProp]
                simPropArea = [x.find('div', class_='tsubo').text for x in simProp]
                simPropPrice = [x.find('div', class_='price').text for x in simProp]

                # Convert them from a list into a string
                simPropName = ", ".join(simPropName)
                simPropAdd = ", ".join(simPropAdd)
                simPropArea = ", ".join(simPropArea)
                simPropPrice = ", ".join(simPropPrice)

                simPropTable = pd.DataFrame({
                    "simPropName": simPropName,
                    "simPropAdd": simPropAdd,
                    "simPropArea": simPropArea,
                    "simPropPrice": simPropPrice
                }, index = [0])
                simPropTest = 1

            except:
                simPropTest = 0

            # Staff Comment Desc
            try:
                reviewDesc = soup.find('div', class_="stuffComment clearfix").find('span', itemprop="description").text
            except:
                reviewDesc = 'NA'

            # Staff Review Rating
            try:
                reviewRating = soup.find('div', class_="stuffComment clearfix").find('span', itemprop="ratingValue").text
            except:
                reviewRating = 'NA'

            # Tenant information
            tenantTest = 0
            try:
                tenant = soup.find('div', class_='tenantBox mb10').find_all('li')
                tenantName = [x.find('p', class_='tenant').text for x in tenant]
                tenantLink = [x.find('a', class_='linkBlank').text for x in tenant]
                tenantName = ", ".join(tenantName)
                tenantLink = ", ".join(tenantLink)

                tenantTable = pd.DataFrame({"Tenant Name": tenantName, "Tenant Link": tenantLink}, index=[0])
                tenantTest = 1
            except:
                tenantTest = 0


            # Construct Basic table
            smallPicture = pd.DataFrame({
                "Office Name": OfficeName, "Rental Link": RentalLinks[x],
                "Latitude": Lat, "Longtitude": Lng,
                "Neighboring Area/Station JP": values1, "Scrapped Date": date,
                "Review Description": reviewDesc, "Review Rating": reviewRating
            }, index=[0])

            # Join rest of the information together
            def concatsmallpic(smallpic, df):
                return pd.concat([smallpic, df], axis = 1, join = 'outer')

            if (rentalTest == 1):
                smallPicture = concatsmallpic(smallPicture, rentalTable)
            if (propTest == 1):
                smallPicture = concatsmallpic(smallPicture, propTable1)
            if (facilTest == 1):
                smallPicture = concatsmallpic(smallPicture, facilTable)
            if (simPropTest == 1):
                smallPicture = concatsmallpic(smallPicture, simPropTable)
            if (tenantTest == 1):
                smallPicture = concatsmallpic(smallPicture, tenantTable)

            # Must remove duplicate if not append will have error if there is too many duplicates
            smallPicture = smallPicture.loc[:,~smallPicture.columns.duplicated()]

            if (x == 0):
                bigPicture = smallPicture
            else:
                bigPicture = bigPicture.append(smallPicture, ignore_index = True)
            logger.info('Links Index: {0} out of {1} is completed'.format(str(x), str(len(RentalLinks))))
            time.sleep(5)

        ctx['raw_scrape_df'] = bigPicture
        bigPicture.to_csv(self.output().path)
        logger.info('Scraping of Rental Details Completed')
    def output(self):
        return luigi.LocalTarget(str(ctx['sysFolder']) + '/run/Raw Scrape.csv')



class custom_officee_scrape_5(luigi.Task):
    def requires(self):
        return custom_officee_scrape_4()

    def run(self):
        logger.info('Now moving on to translate columns')
        client = translate.TranslationServiceClient()
        if 'raw_scrape_df' not in ctx:
            translation = pd.read_csv(str(ctx['sysFolder']) + '/run/Raw Scrape.csv')
        else:
            translation = ctx['raw_scrape_df']

        for i in range(8):
            try:
                translation.columns = [client.translate_text(contents=[x], target_language_code="en", source_language_code="ja")['translatedText'] for x in translation.columns]
                break
            except:
                logger.warning('Translation failed. Warning: {0}'.format(str(sys.exc_info())))
                logger.info('Attempt: {0}'.format(str(i)))
                logger.info('Rest for 60 seconds and retry')
                time.sleep(60)
        logger.info('Translation completed successfully')

        ##### Cleaning and Restructuring of Data #####

        logger.info('Writing Final Output File')
        try:
            the_cols = ['Office Name','Rental Link','Scrapped Date','Street address',
                        'Latitude','Longtitude','Monthly cost','Tsubo unit price (rent) (common expenses)',
                        'Basis number','Brokerage fee','Security deposit','Amortization','key money',
                        'Available days','Tsuba unit price','Use','renewal','Rank','Completion',
                        'Application / specification','Nearest station','Standard floor plan number',
                        'Earthquake resistance','Elevator','entrance','air conditioning',
                        'Floor specification','Ceiling height','toilet','Parking facility',
                        'Neighboring Area / Station JP','simPropAdd','simPropArea','simPropName','simPropPrice',
                        'Review Description','Review Rating','Tenant Link','Tenant Name']
            dfdata = {}
            for x in the_cols:
                dfdata[x] = translation[x]
            translation = pd.DataFrame(dfdata)
        except:
            logger.warning('Restructuring fail as some fields are not present. Not going to restructure.')

        translation.to_csv(self.output().path)

    def output(self):
        return luigi.LocalTarget(str(ctx['sysFolder']) + '/run/{0} Officee Scrape (Final).csv'.format(datetime.now().strftime('%Y-%m-%d')))


class custom_officee_scrape_end(luigi.Task):
    def requires(self):
        return[custom_officee_scrape_5()]

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
