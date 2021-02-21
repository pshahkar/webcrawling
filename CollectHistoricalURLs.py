# This file gets list of Urls from input, and gives the list of historical Urls as an output file.

import sys
import csv
import os
import re
import json

sys.path.append("../archive")
from api_crawling import ArchiveFirmWebsiteCrawler

# Read configurations from file
with open('settings_new.json') as f:
    SETTINGS_FILE = json.load(f)
    f.close()

# Crawl Urls of input file first from archive and write all archive_urls into files
ARCHIVE_INPUT_FILE_PATH = SETTINGS_FILE.get("ArchiveInputFilePath")
ARCHIVE_OUTPUT_FILE_PATH = SETTINGS_FILE.get("ArchiveOutputFilePath")
ARCHIVE_NAME_URL_LIST = SETTINGS_FILE.get("nameArchiveList")


archive_crawler = ArchiveFirmWebsiteCrawler(ARCHIVE_INPUT_FILE_PATH, ARCHIVE_OUTPUT_FILE_PATH, ARCHIVE_NAME_URL_LIST)
archive_crawler.crawl_archive_urls()



#Charter Communications;http://www.charter.com
# New York Life Insurance;http://www.newyorklife.com
# American Express;http://www.americanexpress.com
# Nationwide;http://www.nationwide.com
# Best Buy;http://www.bestbuy.com
# Liberty Mutual Insurance Group;http://www.libertymutual.com
# Merck;http://www.merck.com