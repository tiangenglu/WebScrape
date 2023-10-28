#!/usr/bin/env python
# coding: utf-8

"""
Created on 2023-10-27
@author: Tiangeng Lu

- monthly/routine updates of sevis statistics w/o tabulation
- save monthly data to local sevis folder
- expanded previous sevis_catalog with file name and download time
"""

import requests
from scrapy import Selector
import pandas as pd
import os
from datetime import datetime
from pandas.tseries.offsets import MonthEnd
import datetime as dt
os.chdir('/Users/tiangeng/Documents/Python Files')
import time as tm
import sys

def dtime(file):
    return datetime.fromtimestamp(os.path.getmtime(file)).strftime("%Y-%m-%d, %A, %H:%M:%S")

old_catalog = pd.read_csv('sevis_catalog.csv')

# urls
main_url = 'https://studyinthestates.dhs.gov/sevis-by-the-numbers/sevis-by-the-numbers-data'
main_html = requests.get(main_url).content
main_selector = Selector(text = main_html)
print("Length of selectors \(webpages potentially contain web tables\):\n")
print(len(main_selector.xpath('//*')))
# extract from selector
all_links = main_selector.xpath('//*[contains(@href,"sevis-data-mapping-tool-data")]/@href').extract()
# urls to overall data
sevis = [link for link in all_links if '-stem-' not in link]
# https://studyinthestates.dhs.gov/sevis-data-mapping-tool/march-2023-stem-sevis-data-mapping-tool-data
prefix = "https://studyinthestates.dhs.gov"
sevis = [prefix + link for link in sevis if link.startswith('/sevis')]
sevis = list(set(sevis))


sevis_catalog = pd.DataFrame(data = {
    'url': sevis,
    'year': [link.split('/')[4].split('-')[1].upper() for link in sevis],
    'month': [link.split('/')[4].split('-')[0].upper() for link in sevis]})
sevis_catalog['stamp'] = (sevis_catalog['month'].str.cat(sevis_catalog['year'], sep = '-'))
sevis_catalog['stamp'] = [datetime.strptime(stamp, "%B-%Y").strftime("%Y-%m-%d") for stamp in sevis_catalog['stamp']]
sevis_catalog['stamp'] = (pd.to_datetime(sevis_catalog['stamp'], errors = 'ignore') + MonthEnd()).dt.date
sevis_catalog = sevis_catalog.sort_values('stamp', ascending = True)
sevis_catalog = sevis_catalog.drop_duplicates()
sevis_catalog = sevis_catalog.sort_values('stamp').reset_index(drop = True)
print("Time stamp default format:",type(sevis_catalog['stamp'][0]))
sevis_catalog['stamp'] = sevis_catalog['stamp'].astype(str)
print("Time stamp format now:",type(sevis_catalog['stamp'][0]))


from collections import Counter
sevis_counter = Counter(sevis_catalog['year'])
print(sorted(sevis_counter.items()))

new_urls = list(set(sevis).difference(old_catalog['url']))


if len(new_urls) < 1:
    print("No new urls found. Stop execution.")
    sys.exit()
else:
    print("New urls detected. Continue building the new data catalog.")

new_catalog = sevis_catalog[sevis_catalog['url'].isin(new_urls)].reset_index(drop = True)


sevis_elements = [None] * len(new_catalog)

for i in range(len(new_catalog)):
    html = requests.get(new_catalog['url'][i]).content
    sel = Selector(text = html)
    sevis_elements[i] = sel.xpath('//table').extract()
    print("Element", str(i), "extracted at", tm.strftime("%Y-%m-%d, %H:%M:%S"))

print("Are there any SEVIS webpages have more than 1 tables?\n",[len(element) for element in sevis_elements if len(element) > 1])


SEVIS_dfs = [None] * len(new_catalog)
for element in sevis_elements:
    SEVIS_dfs = [pd.read_html(element[0], header = 0)[0] for element in sevis_elements]
    print(tm.strftime("%Y-%m-%d, %H:%M:%S"))
for i in range(len(SEVIS_dfs)):
    SEVIS_dfs[i].columns = SEVIS_dfs[i].columns.str.upper()

print("Conversion from html elements to dataframes completed at", tm.strftime("%Y-%m-%d, %H:%M:%S"))


counts_col = {}
for df in SEVIS_dfs:
    for col in df.columns:
        if col in counts_col.keys():
            counts_col[col] += 1
        else:
            counts_col[col] = 1
# print the resulting dictionary of column name counts
print(counts_col)

rename_dict = {
    'COUNTRY OF CITIZENSHIP': 'CITIZENSHIP',
    '# OF ACTIVE STUDENTS': 'ACTIVE_STUDENTS',
    'BACHELORS':'BACHELOR',
    "BACHELOR'S":'BACHELOR',
    "MASTER'S": "MASTER",
    'MASTERS':"MASTER",
    'FLIGHT TRAINING': "FLIGHT",
    'FLIGHT_SCHOOL':'FLIGHT',
    'HIGH SCHOOL': 'HIGH_SCHOOL',
    'LANGUAGE TRAINING':'LANGUAGE_TRAINING',
    'OTHER VOCATIONAL SCHOOL':'OTHER_VOCATIONAL_SCHOOL'}
### AFTER STANDARDIZING COLUMN NAMES
for i in range(len(SEVIS_dfs)):
    SEVIS_dfs[i] = SEVIS_dfs[i].rename(columns = rename_dict)
counts_col = {}
for df in SEVIS_dfs:
    for col in df.columns:
        if col in counts_col.keys():
            counts_col[col] += 1
        else:
            counts_col[col] = 1
# print the resulting dictionary of column name counts
print(counts_col)


# Add timestamp by using the info_df_short dataframe
for i in range(len(SEVIS_dfs)):
    SEVIS_dfs[i]['time'] = sevis_catalog['stamp'][i]
    SEVIS_dfs[i]['year'] = sevis_catalog['year'][i]
    SEVIS_dfs[i]['month'] = sevis_catalog['month'][i]


sevis_csv_names = ["sevis_"+ stamp + '.csv' for stamp in new_catalog['stamp'].astype(str)]
print(sevis_csv_names)


if not os.path.exists('sevis'):
    os.makedirs('sevis')    
for i in range(len(SEVIS_dfs)):
    SEVIS_dfs[i].drop(['time','year','month'], axis = 1).to_csv('sevis/'+sevis_csv_names[i], index = False)

data_list = [f for f in os.listdir(r'sevis/') if f.startswith('sevis_')]
download_time = [dtime(r'sevis/'+f) for f in os.listdir(r'sevis/') if f.startswith('sevis_')]
download_status = pd.DataFrame({'name': data_list, 'download_time': download_time})
download_status = download_status.sort_values('name').reset_index(drop = True)
print(download_status.tail())
sevis_catalog_full = pd.concat([sevis_catalog, download_status], axis = 1)
sevis_catalog_full.to_csv('sevis_catalog.csv', index = False)