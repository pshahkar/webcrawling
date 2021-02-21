# webcrawling

In this project, first we collect all historical URLs of some companies, then we filter them based on the given time-span and frequency.

CollectHistoricalURLs.py : The file that gets the name and current url of companies as input, and makes a list full of all historical URLs(+ datetime corresponding to that URL) as an output. The class defined in "api_crawling.py" file is used in this code. 

FilterByTimeSPanFrequency.py : This file filters the output of the previous step based on the given time-span and frequency. 

