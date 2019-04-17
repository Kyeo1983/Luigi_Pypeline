import luigi
import json
import os
import sys
import time
import shutil
import subprocess
import logging
import logging.config
from datetime import datetime
####################################
# BASE LIBRARIES REQUIRED BY SCRAPER
####################################
import random
import math
import pickle
from multiprocessing.dummy import Pool as ThreadPool
import requests
from fake_useragent import UserAgent
from http_request_randomizer.requests.proxy.requestProxy import RequestProxy
from tqdm import tqdm
from bs4 import BeautifulSoup
sys.path.append('../utils')
from pathlib import Path
import pandas as pd


ctx = {'sysFolder' : '/home/kyeoses/pypeline/jobs/jobmarkers/custom_suumo_scrape'}
ctx['sysRunFolder'] = '/home/kyeoses/pypeline/jobs/jobmarkers/custom_suumo_scrape/run'
ctx['sysSaveFolder'] = '/home/kyeoses/pypeline/jobs/jobmarkers/custom_suumo_scrape/run/save'
ctx['sysJobName'] = 'custom_suumo_scrape'
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


#############################################
# Suumo logic
#############################################
VALID_AREAS = [ 'Tokyo_23','Tokyo_50','Kanagawa_Yokohama','Kanagawa_Kawasaki', 'Kanagawa_Sagamihara','Kanagawa_others','Saitama_city','Saitama_others','Chiba_city','Chiba_others','Osaka_city','Osaka_others','Nagoya','Fukuoka']
EXECUTION_AREAS = VALID_AREAS
class Suumo_Scraper:
    def run(self, in_stageNum, in_area):
        # [MODIFY IF NEEDED] #######
        ########## GLOBAL PARAMETERS
        # Number of concurrent scraping threads to use in the pool
        # As scraping is network IO-bound, increase this for faster scrapes
        NUM_THREADS = 8

        # TODO: future work: use list of premium proxies. Free proxies are too unreliable.
        # Use rotating free proxies to scrape if enabled
        # Disable if http_request_randomizer is unable to get any proxies
        USE_PROXY = False

        # Attempts before giving up on a URL due to connection problems
        NUM_ATTEMPTS = 3
        TIMEOUT_SECONDS = 31

        # Split CSV/List into of links chunks of size CHUNKSIZE.
        # Links in each chunk is then scraped in a pool.
        # Results are written to disk after each chunk. Order is preserved.
        # To reduce slowdowns due to disk IO, set this to a large-ish number
        CHUNKSIZE = 200

        # Min and max backoff after each thread's scrape, in seconds
        BACKOFF_MIN = 1
        BACKOFF_MAX = 5

        UA = UserAgent()



        ##### Start of main logic #####
        def get_content_from_url(url):
            """
            Scrapes the page in two modes:
            USE_PROXY=True: random proxies and user-agents
            USE_PROXY=False: random user-agents and sleep time

            Retries for connection problems including timeouts, up to NUM_ATTEMPTS
            Will not retry for HTTP errors (e.g. 404, 500)
            """
            attempts = NUM_ATTEMPTS
            response = None
            while response is None and (attempts > 0):
                attempts -= 1
                logger.debug("Requesting URL {0}".format(url))
                if USE_PROXY:
                    # No need to wait before tries since it'll be using a different proxy and UA string
                    # Just keep retrying until request_result is not none
                    # Timeouts already handled by this
                    response = REQ_PROXY.generate_proxied_request(url)
                else:
                    # Need to wait before tries because of static IP and UA string
                    # Wait randomly to avoid clashing with other threads (if there are)
                    time.sleep(random.uniform(BACKOFF_MIN, BACKOFF_MAX))
                    try:
                        response = requests.get(url, headers={"user-agent": UA.random}, timeout=TIMEOUT_SECONDS)
                        response.raise_for_status()
                    except requests.exceptions.HTTPError as e:
                        # Generally an error that won't resolve itself if we try. Return empty response.
                        logger.warning("Request failed with exception {}. Will not retry".format(e))
                    except requests.exceptions.RequestException as e:
                        # Try again. Use earlier random delay as backoff
                        logger.warning("Request failed with exception {0}. Retrying with {1} attempts left".format(e, attempts))
                        response = None

            try:
                logger.debug("Request completed with status code {0}".format(response.status_code))
                return response.content
            except:
                logger.warning("No content returned for URL {}".format(url))
            return ""


        def _scrape_chunk(in_list, index, out_csv_filename, scraping_function):
            def _scrape_map_function(row):
                try:
                    result = scraping_function(row)
                    return result
                except:
                    e = sys.exc_info()[0]
                    logger.critical(
                        "The scraping function raised an unhandled exception for row {0}.\nException: {1}".format(row, e))
                    return [row]

            # Initialize pool
            logger.info("Initializing scraping")
            pool_result = list()

            # Start scraping
            with ThreadPool(NUM_THREADS) as p:
                wrapped_list_results = list(p.imap(_scrape_map_function, in_list))
                    #tqdm(p.imap(_scrape_map_function, in_list), desc="Pages in Chunk", total=len(in_list), leave=False, unit="pg"))
                try:
                    pool_result.extend([item for sublist in wrapped_list_results for item in sublist])
                except:
                    e = sys.exc_info()[0]
                    logger.critical(
                        "Error extending pool_result with chunk result {0}.\nException: {1}\nThis row_result will be discarded."
                        .format(wrapped_list_results[0], e))

            logger.info("Scraping completed. Appending {0} results".format(len(pool_result)))
            out_df = pd.DataFrame(pool_result)
            out_df.to_csv(out_csv_filename, mode="a", header=(index == 0), index=False)


        def _write_pickle(obj, filename):
            try:
                with open(filename, "wb") as f:
                    pickle.dump(obj, f, protocol=pickle.HIGHEST_PROTOCOL)
                logger.debug("Pickle saved as {}".format(filename))
                return True
            except:
                e = sys.exc_info()[0]
                logger.warning("Error saving pickle. The resulting pickle may be corrupted!\nException: {0}".format(e))
                return False


        def _read_pickle(filename):
            try:
                with open(filename, "rb") as f:
                    logger.info("Pickle {} found. Loading".format(filename))
                    return pickle.load(f)
            except:
                e = sys.exc_info()[0]
                logger.info("Failed to find or load pickle.\nException: {0}".format(e))
                return None


        def write_index_chunk_completed(index, filename):
                logger.debug("Saving status of chunk {0}".format(index))
                _write_pickle(index, filename)


        def get_index_chunk_completed(filename):
            logger.info("Checking for completed chunks")
            pickle_filename = filename
            index_chunk_completed = _read_pickle(pickle_filename)
            if (index_chunk_completed is None):
                index_chunk_completed = -1

            logger.info("Last stopped at chunk index {0}".format(index_chunk_completed))
            return index_chunk_completed


        def start_scrape_by_chunk_csv(in_csv_filename, out_csv_filename, scraping_function):
            logger.info("Reading CSV")
            in_chunks = pd.read_csv(in_csv_filename, chunksize=CHUNKSIZE)
            total_chunks = math.ceil(sum(1 for row in open(in_csv_filename, "r")) / CHUNKSIZE)

            pickle_filename = str(out_csv_filename) + ".chunks.pkl"
            index_chunk_completed = get_index_chunk_completed(pickle_filename)

            for index, in_df in tqdm(enumerate(in_chunks, start=0), desc="Total", total=total_chunks, unit="chunk"):
                if (index > index_chunk_completed):
                    logger.info("Scraping chunk {0}".format(index))
                    _scrape_chunk(in_df.to_dict("records"), index, out_csv_filename, scraping_function)

                    index_chunk_completed += 1
                    write_index_chunk_completed(index_chunk_completed, pickle_filename)
                    if (index_chunk_completed % 10 == 0):
                        logger.info('Scrape completed {}'.format(index_chunk_completed))
                    if (index_chunk_completed % 20 == 0):
                        emailconf = email()
                        smtpconf = smtp()
                        cmd = 'echo "Indexed {}" | s-nail -s "Job Update: {} indexed {}" -r "{}" -S smtp="{}:{}" -S smtp-use-starttls -S smtp-auth=login -S smtp-auth-user="{}" -S smtp-auth-password="{}" -S ssl-verify=ignore {}'.format(index_chunk_completed, ctx['sysJobName'], index_chunk_completed, emailconf.sender, smtpconf.host, smtpconf.port, smtpconf.username, smtpconf.password, emailconf.receiver)
                        subprocess.call(cmd, shell=True)
                else:
                    logger.debug("Skipping chunk {0}".format(index))


        def start_scrape_by_chunk_list(in_list, out_csv_filename, scraping_function):
            def _list_to_chunks(in_list, n):
                for i in range(0, len(in_list), n):
                    yield in_list[i: i + n]

            logger.info("Chunking list")
            list_chunks = _list_to_chunks(in_list, CHUNKSIZE)
            total_chunks = math.ceil(len(in_list) / CHUNKSIZE)

            pickle_filename = str(out_csv_filename) + ".chunks.pkl"
            logger.info("Saving to pickle {}".format(pickle_filename))
            index_chunk_completed = get_index_chunk_completed(pickle_filename)

            for index, list_chunk in enumerate(list_chunks, start=0):
            #tqdm(enumerate(list_chunks, start=0), desc="Total", total=total_chunks, unit="chunk"):
                if (index > index_chunk_completed):
                    logger.info("Scraping chunk {0}".format(index))
                    _scrape_chunk(list_chunk, index, out_csv_filename, scraping_function)
                    index_chunk_completed += 1
                    write_index_chunk_completed(index_chunk_completed, pickle_filename)
                    if (index_chunk_completed % 10 == 0):
                        logger.info('Scrape completed {}'.format(index_chunk_completed))
                    if (index_chunk_completed % 20 == 0):
                        emailconf = email()
                        smtpconf = smtp()
                        cmd = 'echo "Indexed {}" | s-nail -s "Job Update: {} indexed {}" -r "{}" -S smtp="{}:{}" -S smtp-use-starttls -S smtp-auth=login -S smtp-auth-user="{}" -S smtp-auth-password="{}" -S ssl-verify=ignore {}'.format(index_chunk_completed, ctx['sysJobName'], index_chunk_completed, emailconf.sender, smtpconf.host, smtpconf.port, smtpconf.username, smtpconf.password, emailconf.receiver)
                        subprocess.call(cmd, shell=True)


                else:
                    logger.debug("Skipping chunk {0}".format(index))


        # [MODIFY THIS] #########################################################################
        ############################ WRITE YOUR OWN FUNCTIONS HERE ##############################
        # Use get_content_from_url() to scrape a single site
        # Use start_scrape_by_chunk() and modify scrape_row() to scrape a CSV list of sites

        def pagelist(url, pagecount):
            pagelink = list()

            for i in range(1, pagecount + 1):
                link = url + str(i)
                pagelink.append(link)

            return pagelink


        def get_content_page_urls(file, areas=None):
            '''
            file: path of content page url list (manually extracted out)
            areas: [area1,area2,...]
                choices:
                    'Tokyo_23' 'Tokyo_50' 'Kanagawa_Yokohama' 'Kanagawa_Kawasaki'
                    'Kanagawa_Sagamihara' 'Kanagawa_others' 'Saitama_city' 'Saitama_others'
                    'Chiba_city' 'Chiba_others' 'Osaka_city' 'Osaka_others' 'Nagoya'
                    'Fukuoka'
                if None: all areas are included
            '''

            fulllink = pd.read_excel(file)
            if areas is not None:
                fulllink = fulllink[fulllink['area'].isin(areas)]

            sublink = fulllink[['url', 'pagecount']]
            tuples = [tuple(x) for x in sublink.values]
            pagelink = [pagelist(*x) for x in tuples]
            flatlinks = [item for sublist in pagelink for item in sublist]

            return flatlinks


        def scrape_row_for_prop_links(in_url):
            # Input: string (since this originated from a list, and not a DataFrame)
            # Output: dict (since this will be written as a DF in the end)
            content = get_content_from_url(in_url)

            indexsoup = BeautifulSoup(content, "html.parser")
            links = indexsoup.find_all('h2', {'class': 'property_inner-title'})
            logger.debug("No. of links found: {0}".format(len(links)))

            linklst = list()
            try:
                for linkk in links:
                    try:
                        lk = linkk.find('a').get('href')
                        row = dict()
                        row["links"] = 'https://suumo.jp' + lk
                        linklst.append(row)
                    except:
                        logger.warning("Error getting href for a link in URL {}".format(in_url))
            except:
                logger.warning("No links found in URL {0}".format(in_url))

            return linklst


        def scrape_row_content(in_dict):
            # Do whatever you want here, just return a list of dict(s) back
            # Be sure to call get_content_from_url to get your content

            main_content = process_main_content(get_content_from_url(in_dict["links"]), in_dict["links"])

            geo_content = process_geo_content(get_content_from_url(in_dict["links"] + "kankyo/"), in_dict["links"])

            return [{**main_content, **geo_content}]


        # soup = BeautifulSoup(content, "lxml")
        # in_dict["result"] = [title.text for title in soup.find_all("h2")]
        # return in_dict

        def process_main_content(content, url):
            """
            :param content: string output from get_content_from_url()
            :param url: string
            :return: a dictionary with key being item header, value being item content
            """
            contentlst = dict()
            contentlst['links'] = url

            soup = BeautifulSoup(content, 'html.parser')
            ## Part 1 content including basic characteristics of house
            content = soup.find_all("div", {"class": "property_data-body"})
            header1 = soup.find_all("div", {"class": "property_data-title"})
            try:
                for ct, hd in zip(content, header1):
                    key = hd.text
                    contentlst[key] = ct.text.replace("\n", "").replace("\r", "").replace("\xa0", "").replace("\u3000", "").replace("\t", "")
            except:
                logger.debug("Listing not available for a link in URL {}".format(url))

            ## Part 2 table content
            table_content = soup.find_all("table", {"class": "data_table table_gaiyou"})
            try:
                data = table_content[0].find_all('td')
                header2 = table_content[0].find_all('th')
                if len(data) > 0:
                    for d, h in zip(data[:12], header2[:12]):
                        key = h.text
                        contentlst[key] = d.text.replace("\n", "").replace("\r", "").replace("\xa0", "").replace("\u3000", "").replace("\t", "")
            except:
                logger.debug("No table content found in URL {}".format(url))

            ## '部屋の特徴・設備' - free text
            try:
                contentlst['部屋の特徴・設備'] = soup.find_all("div", {"class": "bgc-wht ol-g"})[0].text.replace("\n", "")
            except:
                logger.debug("No house characteristics found in URL {}".format(url))

            ## Rent
            try:
                contentlst['Rent'] = soup.find_all("div", {"class": "property_view_main-emphasis"})[0].text.replace("\n", "").replace("\r", "").replace("\xa0", "").replace("\u3000", "").replace("\t", "")
            except:
                logger.debug("No rent information found in URL {}".format(url))

            ## Nearest station & location in Japanese

            trains = soup.find_all("div", {"class": "property_view_detail-text"})
            try:
                contentlst['Location'] = trains[-1].text.replace("\n", "").replace("\r", "").replace("\xa0", "").replace("\u3000", "").replace("\t", "")
                for i in range(len(trains)):
                    if trains[i].text[-1] == '分':
                        key = 'Train' + str(i)
                        contentlst[key] = trains[i].text
            except:
                logger.debug("No train and location information found in URL {}".format(url))



            ## get photo url
            try:
                contentlst['Image_url'] = soup.find('div',{'class','property_view_gallery-thumbnail-list'}).find_all('li')[-1].find('img')['src']
            except:
                logger.debug('No image url found in URL {}'.format(url))

            return contentlst


        def process_geo_content(content, url):
            """
            :param geo_content: string output from get_content_from_url()
            :param geo_url: normal url+'kankyo/' which contains lat & lng of the property
            :return: a dictionary with key being lat and lng, value being numeric values
            """
            geo_soup = BeautifulSoup(content, 'html.parser')
            contentlst = dict()
            contentlst['links_geo'] = url.split('kankyo')[0]

            ## lat lng info
            geo_info = geo_soup.find_all("script", {"id": "js-gmapData"})
            try:
                info = json.loads(geo_info[0].text)['center']
                contentlst['lat'] = info['lat']
                contentlst['lng'] = info['lng']
            except:
                logger.debug("No lat and lng found in URL {}".format(url))
            return contentlst

        ##### End of main logic #####

        ##### Start of execution #####
        content_link_file = Path('../data/suumolinks.xlsx')
        areas = [in_area]
        output_name = Path(str(ctx['sysRunFolder'])) / '{}.csv'.format(in_area).lower()
        logger.info('Output CSV at {}'.format(output_name))
        # First read in the link list of all the cities
        logger.info("Generating list of links to scrape")
        if (in_stageNum == 1):
            fulllink = get_content_page_urls(content_link_file, areas)
            logger.info("{0} links to scrape".format(len(fulllink)))
            logger.info ("Beginning scraping")
            start_scrape_by_chunk_list(fulllink, output_name, scrape_row_for_prop_links)
            logger.info("Scraping complete")
            ##### End of stage 1 execution #####

        else:
            # Stage 2: scraping each prop link
            detail_input_name = output_name
            detail_output_name = Path(str(ctx['sysRunFolder'])) / '{}_detail.csv'.format(in_area).lower()
            start_scrape_by_chunk_csv(detail_input_name, detail_output_name, scrape_row_content)
            ##### End of stage 2 execution #####
########################################
# End of Suumo class
########################################



########################################
# START LUIGI PIPELINE
########################################
class custom_suumo_scrape_start(luigi.Task):
    def run(self):
        ctx['sysStatus'] = 'running'
        ctx['sysTempFolder'] = ctx['sysRunFolder']
        with open(self.output().path, 'w') as out:
            out.write('started successfully')

    def output(self):
        return luigi.LocalTarget(str(ctx['sysFolder']) + '/run/started.mrk')



class custom_suumo_scrape_1(luigi.Task):
    def requires(self):
        return custom_suumo_scrape_start()

    def run(self):
        suumo = Suumo_Scraper()
        emailconf = email()
        smtpconf = smtp()
        for area in EXECUTION_AREAS:
            cmd = 'echo "Indexed" | s-nail -s "Job Update: {} ({}) started Stage 1" -r "{}" -S smtp="{}:{}" -S smtp-use-starttls -S smtp-auth=login -S smtp-auth-user="{}" -S smtp-auth-password="{}" -S ssl-verify=ignore {}'.format(ctx['sysJobName'], area, emailconf.sender, smtpconf.host, smtpconf.port, smtpconf.username, smtpconf.password, emailconf.receiver)
            subprocess.call(cmd, shell=True)
            suumo.run(1, area)
            cmd = 'echo "Indexed" | s-nail -s "Job Update: {} ({}) completed Stage 1" -r "{}" -S smtp="{}:{}" -S smtp-use-starttls -S smtp-auth=login -S smtp-auth-user="{}" -S smtp-auth-password="{}" -S ssl-verify=ignore {}'.format(ctx['sysJobName'], area, emailconf.sender, smtpconf.host, smtpconf.port, smtpconf.username, smtpconf.password, emailconf.receiver)
            subprocess.call(cmd, shell=True)
        with open(self.output().path, 'w') as out:
            out.write('done')
    def output(self):
        return luigi.LocalTarget(str(ctx['sysFolder']) + '/run/1.mrk')



class custom_suumo_scrape_1(luigi.Task):
    def requires(self):
        return custom_suumo_scrape_1()

    def run(self):
        suumo = Suumo_Scraper()
        emailconf = email()
        smtpconf = smtp()
        for area in EXECUTION_AREAS:
            cmd = 'echo "Indexed" | s-nail -s "Job Update: {} ({}) started Stage 1" -r "{}" -S smtp="{}:{}" -S smtp-use-starttls -S smtp-auth=login -S smtp-auth-user="{}" -S smtp-auth-password="{}" -S ssl-verify=ignore {}'.format(ctx['sysJobName'], area, emailconf.sender, smtpconf.host, smtpconf.port, smtpconf.username, smtpconf.password, emailconf.receiver)
            subprocess.call(cmd, shell=True)
            suumo.run(2, area)
            cmd = 'echo "Indexed" | s-nail -s "Job Update: {} ({}) completed Stage 1" -r "{}" -S smtp="{}:{}" -S smtp-use-starttls -S smtp-auth=login -S smtp-auth-user="{}" -S smtp-auth-password="{}" -S ssl-verify=ignore {}'.format(ctx['sysJobName'], area, emailconf.sender, smtpconf.host, smtpconf.port, smtpconf.username, smtpconf.password, emailconf.receiver)
            subprocess.call(cmd, shell=True)
        with open(self.output().path, 'w') as out:
            out.write('done')
    def output(self):
        return luigi.LocalTarget(str(ctx['sysFolder']) + '/run/2.mrk')


class custom_suumo_scrape_end(luigi.Task):
    def requires(self):
        return[custom_suumo_scrape_2()]

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
