# This file gets the list of all historical urls and based on the timespan and frequency gives the closest urls
import sys
import csv
import os
import re
import json
import pandas as pd
import requests
import datetime


sys.path.append("../archive")
import urllib.parse
import pandas as pd



def get_one_row(df):
    '''
    :param df: the dateframe for a given company and a given proper date indicating the urls related to that date
    :return: the first row of the dataframe, which has a valid response
    '''
    i = 0
    index = 0

    # while i < len(df):
    #     url = df['url'].iloc[i]
    #     try:
    #         page = requests.get(url)
    #         if page.status_code == 200:
    #             print('success')
    #             index = i
    #             break
    #     except requests.exceptions.ConnectionError as e:
    #         print('Connection error')
    #     i = i + 1

    return df.iloc[index]


with open('settings_new.json') as f:
    SETTINGS_FILE = json.load(f)
f.close()

start_year = int(SETTINGS_FILE.get("StartYear"))
start_month = int(SETTINGS_FILE.get("StartMonth"))
start_day = int(SETTINGS_FILE.get("StartDay"))
end_year = int(SETTINGS_FILE.get("EndYear"))
end_month = int(SETTINGS_FILE.get("EndMonth"))
end_day = int(SETTINGS_FILE.get("EndDay"))
frequency = int(SETTINGS_FILE.get("Frequency")) # Frequency based on number of days
monthly = bool(SETTINGS_FILE.get("Monthly"))
ARCHIVE_OUTPUT_FILE_PATH = SETTINGS_FILE.get("ArchiveOutputFilePath")
ARCHIVE_NAME_URL_LIST = SETTINGS_FILE.get("nameArchiveList")

def updated_df(df):
    df['Date_time'] = df['datetime'].astype('datetime64[ns]')
    df['year'] = df.Date_time.dt.year
    df['month'] = df.Date_time.dt.month
    df['day'] = df.Date_time.dt.day
    df['hour'] = df.Date_time.dt.hour
    df['minute'] = df.Date_time.dt.minute
    df['second'] = df.Date_time.dt.second
    return df

########## Opening Input file as a dataframe
with open(os.path.join(ARCHIVE_OUTPUT_FILE_PATH, ARCHIVE_NAME_URL_LIST), 'r') as f:
    reader = csv.reader(f, delimiter=';', quotechar='"', quoting=csv.QUOTE_ALL)
    domains = list(reader)
f.close()

domains.pop(0)
df = pd.DataFrame({'company': [domains[i][0] for i in range(0, len(domains))],
                   'url' : [domains[i][1] for i in range(0, len(domains))],
                   'datetime': [domains[i][-1] for i in range(0, len(domains))]})
new_df = updated_df(df)

########### Filtering dates not in Time span

print('start year:',start_year,'start month:', start_month, 'start_day', start_day, 'end year:',end_year, 'end month:', end_month, 'end_day:', end_day, 'monthly:', monthly,'frequency:', frequency)
new_dff = new_df[new_df['year'].between(start_year, end_year)]
indx1 = new_dff.index[(new_dff['year'] == start_year) & (new_dff['month'] < start_month)].tolist()
indx2 = new_dff.index[(new_dff['year'] == end_year) & (new_dff['month'] > end_month)].tolist()
my_list = indx1 + indx2
Filter_df  = new_dff[~new_dff.index.isin(my_list)]
indx1 = Filter_df.index[(Filter_df['year'] == start_year) & (Filter_df['month'] == start_month) & (Filter_df['day'] < start_day)].tolist()
indx2 = Filter_df.index[(Filter_df['year'] == end_year) & (Filter_df['month'] == end_month) & (Filter_df['day'] > end_day)].tolist()
my_list = indx1 + indx2
Filter_df  = Filter_df[~Filter_df.index.isin(my_list)]


def frequency_based(Filter_df):
    ########### Add new column to filtered dataframe
    Filter_df.reset_index(drop=True, inplace=True)
    Filter_df['days_from_start'] = 0
    # Base for start time = start year and month and day
    for i in range(0, len(Filter_df)):
        Filter_df['days_from_start'].loc[i] = (
        Filter_df['Date_time'].loc[i].date() - datetime.date(start_year, start_month, start_day)).days

    ########## Adding two new columns, finding closest proper date for all dates
    Filter_df['closest_selected_date'] = Filter_df['days_from_start'].div(frequency).round() * frequency
    Filter_df['distance_closest_selected_date'] = (
    Filter_df['days_from_start'] - Filter_df['closest_selected_date']).abs()

    ########### Making a list out of proper dates based on frequency
    selected_days = []
    i = 0
    while frequency * i <= (
        datetime.date(end_year, end_month, end_day) - datetime.date(start_year, start_month, start_day)).days:
        selected_days.append(frequency * i)
        i = i + 1

    ######### For each company and proper date, chooses the url of the closest date with valid response
    companies = Filter_df.company.unique()
    final_df = pd.DataFrame(columns=Filter_df.columns.tolist())
    for company in companies:  # companies
        # select rows with a given frequency
        mini_df = Filter_df[Filter_df['company'] == company]
        for selected_day in selected_days:  # selected_days
            mini_mini_df = mini_df[mini_df['closest_selected_date'] == selected_day]
            if mini_mini_df.empty == True:
                one_row_df = final_df.iloc[-1:]
                one_row_df['closest_selected_date'] = selected_day
            else:
                one_row_df = get_one_row(mini_mini_df[mini_mini_df['distance_closest_selected_date'] == mini_mini_df[
                    'distance_closest_selected_date'].min()])

            final_df = final_df.append(one_row_df, ignore_index=True)

    return final_df


def monthly_based(Filter_df):
    companies = Filter_df.company.unique()
    Filter_df.reset_index(drop=True, inplace=True)
    Filter_df['distance'] = 0
    Filter_df['desired_date'] = 0
    final_df = pd.DataFrame(columns=Filter_df.columns.tolist())
    for company in companies:  # companies
        # select rows with a given frequency
        mini_df = Filter_df[Filter_df['company'] == company]
        mini_df.reset_index(drop=True, inplace=True)

        if start_year == end_year:
            for month in range(start_month, end_month + 1):
                for i in range(0, len(mini_df)):
                    mini_df['distance'].iloc[i] = (
                    mini_df['Date_time'].iloc[i].date() - datetime.date(start_year, month, start_day)).days
                mini_df['distance'] = mini_df['distance'].abs()
                one_row_df = mini_df[mini_df['distance'] == mini_df['distance'].min()].iloc[0]
                one_row_df['desired_date'] = datetime.date(start_year, month, start_day)
                final_df = final_df.append(one_row_df, ignore_index=True)
        else:
            ### in start year
            for month in range(start_month, 13):
                for i in range(0, len(mini_df)):
                    mini_df['distance'].iloc[i] = (
                    mini_df['Date_time'].iloc[i].date() - datetime.date(start_year, month, start_day)).days
                mini_df['distance'] = mini_df['distance'].abs()
                one_row_df = mini_df[mini_df['distance'] == mini_df['distance'].min()].iloc[0]
                one_row_df['desired_date'] = datetime.date(start_year, month, start_day)
                final_df = final_df.append(one_row_df, ignore_index=True)
            ### in years in between
            for year in range(start_year + 1, end_year):
                for month in range(1, 13):
                    for i in range(0, len(mini_df)):
                        mini_df['distance'].iloc[i] = (
                        mini_df['Date_time'].iloc[i].date() - datetime.date(year, month, start_day)).days
                    mini_df['distance'] = mini_df['distance'].abs()
                    one_row_df = mini_df[mini_df['distance'] == mini_df['distance'].min()].iloc[0]
                    one_row_df['desired_date'] = datetime.date(year, month, start_day)
                    final_df = final_df.append(one_row_df, ignore_index=True)
            ### in end year
            for month in range(1, end_month + 1):
                for i in range(0, len(mini_df)):
                    mini_df['distance'].iloc[i] = (
                    mini_df['Date_time'].iloc[i].date() - datetime.date(end_year, month, start_day)).days
                mini_df['distance'] = mini_df['distance'].abs()
                one_row_df = mini_df[mini_df['distance'] == mini_df['distance'].min()].iloc[0]
                one_row_df['desired_date'] = datetime.date(end_year, month, start_day)
                final_df = final_df.append(one_row_df, ignore_index=True)
    return final_df


monthly = True
if monthly == True:
    final_df = monthly_based(Filter_df)
else:
    final_df = frequency_based(Filter_df)


######## Removing trivial columns
final = final_df.drop(columns=['year', 'month', 'day', 'hour', 'minute', 'second', 'Date_time'])

######## Saving the output
final.to_csv(ARCHIVE_OUTPUT_FILE_PATH + '/' + str(start_year) + '-' + str(start_month) + '-' + str(start_day) +
             'to' + str(end_year) + '-' + str(end_month) + '-' + str(end_day) + 'every' + str(frequency) + 'days.csv')



