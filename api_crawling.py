from time import sleep
import requests
import json
import os
import csv
import random
import re
import unicodedata
from urllib3.exceptions import HTTPError as BaseHTTPError

import bs4
from http.client import IncompleteRead as http_incompleteRead
from urllib3.exceptions import IncompleteRead as urllib3_incompleteRead
import time
from subprocess import call


from http.client import IncompleteRead


from spider import FirmWebsiteCrawler

# Define crawler #
class ArchiveFirmWebsiteCrawler():

    def __init__(self,
                 archive_input_file_path,
                 output_file_path,
                 archive_name_url_list):

        self.archive_input_file_path = archive_input_file_path
        self.output_file_path = output_file_path
        self.archive_name_url_list = archive_name_url_list

    def crawl_archive_urls(self):
        counter = 0
        cc = 0
        # create directory to store all produced files in
        filename = self.archive_name_url_list

        if not os.path.isdir(self.output_file_path):
            os.makedirs(self.output_file_path)

        companies_not_in_archive = {}
        companies_not_in_archive['companies'] = []

        # write urls to an output .csv file
        # companyname;city;postcode;streetnobuildingetcline1;websiteaddress;bvdidnumber;country;timegate_url;datetime

        with open(os.path.join(self.output_file_path, filename), 'a') as outfile:
            writer = csv.writer(outfile, delimiter =';', quotechar='"', quoting=csv.QUOTE_ALL)
            writer.writerow(["companyname", "archive_url", "datetime"])


        with open(self.archive_input_file_path) as cwl:
            csvReader = csv.reader(cwl, delimiter=';')
                # skip header line
            next(csvReader)

            for row in csvReader:
                # loop through company_urls & get mementos-urls
                # Example: row = ['Walmart', 'http://www.stock.walmart.com']
                base_url = 'http://labs.mementoweb.org/timemap/json/'
                # base_url = 'http://web.archive.org/web/'
                try:
                    u = base_url + row[1]
                    r = requests.get(u)
                    # test whether request with desired company-url got valid response
                    # status_code: 200 --> valid response
                    if r.status_code == 200:
                        data = json.loads(r.text)
                        edited_row = row
                        edited_row.append("x")
                        print(data)
                        r.close()

                        # normal case: url directly accessible
                        if 'timemap_index' not in data:
                            for t in data['mementos']['list']:
                                with open(os.path.join(self.output_file_path, filename), 'a') as outfile:
                                    writer = csv.writer(outfile, delimiter=';', quotechar='"', quoting=csv.QUOTE_ALL)
                                    edited_row[1] = t['uri'].replace(':80/', '/')
                                    edited_row[-1] = t['datetime']
                                    writer.writerow(edited_row)
                                outfile.close()

                        # badge case --> first open url of badge
                        else:
                            for b in data['timemap_index']:
                                badge = b['uri']
                                print(badge)

                                try:
                                    # time.sleep(5)
                                    rb = requests.get(badge)
                                    badgy = bs4.BeautifulSoup(rb.text, 'html.parser')
                                    badge_data = json.loads(badgy.text)
                                    rb.close()
                                    for t in badge_data['mementos']['list']:
                                        with open(os.path.join(self.output_file_path, filename), 'a') as outfile:
                                            # print(outfile)
                                            writer = csv.writer(outfile, delimiter=';', quotechar='"',
                                                                quoting=csv.QUOTE_ALL)
                                            edited_row[1] = t['uri'].replace(':80/', '/')
                                            edited_row[-1] = t['datetime']
                                            writer.writerow(edited_row)
                                        outfile.close()
                                # time.sleep(5)
                                # except ChunkedEncodingError as e:
                                except urllib3_incompleteRead as e:
                                    print('shoooot')
                                    cc = cc + 1
                                except http_incompleteRead as e:
                                    print('shoooot')
                                    cc = cc + 1
                                except requests.exceptions.ChunkedEncodingError as e:
                                    print('shoooooot')
                                    cc = cc + 1
                                    # badge_data = json.loads(r.text)
                                    # no valid response (status code != 200)
                                    # json to store all companies, where no valid archive url response was obtained
                    else:
                        print('Oh nooooooooo')
                        counter = counter + 1
                        with open(os.path.join(self.output_file_path, "companies-not-in-archive.csv"), 'a') as outfile:
                            writer = csv.writer(outfile, delimiter=';', quotechar='"', quoting=csv.QUOTE_ALL)
                            writer.writerow(
                                ["companyname", "url", "status-code"])
                            edited_row.append(r.status_code)
                            writer.writerow(edited_row)
                        outfile.close()

                except requests.exceptions.ConnectionError as e:
                    print('Connection Error for site', row[1])
                # print(row)


            print('Number of times we got error', counter)
            print('Number of times server declined', cc)