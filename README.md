# webcrawling

In this project, first we collect all historical URLs of some companies, then we filter them based on the given time-span and frequency.

CollectHistoricalURLs.py : This file gets the name and current url of some companies as input, and makes a list full of all historical URLs(+ datetime corresponding to that URL) as an output. The class defined in "api_crawling.py" file is used in this code. 

FilterArchivedURLs.py : This file filters the output of the previous step based on the given time-span and frequency. If "Monthly" is true in the settings, the URLs are going to be filtered on a monthly basis. Otherwise, the frequency determined by the user in settings is going to be considered as the desired number of days between two consecutive filtered URLs. 


